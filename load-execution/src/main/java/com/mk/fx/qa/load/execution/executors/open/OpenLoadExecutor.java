package com.mk.fx.qa.load.execution.executors.open;

import static com.mk.fx.qa.load.execution.utils.LoadUtils.toDuration;
import static java.util.concurrent.Executors.newScheduledThreadPool;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class OpenLoadExecutor {

  private OpenLoadExecutor() {
    throw new UnsupportedOperationException("OpenLoadExecutor cannot be instantiated");
  }

  public static OpenLoadResult execute(
      UUID taskId,
      OpenLoadParameters parameters,
      BooleanSupplier cancellationRequested,
      Runnable iterationTask)
      throws InterruptedException {

    validateTask(taskId, parameters, cancellationRequested, iterationTask);

    int maxConcurrent = Math.max(1, parameters.maxConcurrent());
    double arrivalRatePerSec = Math.max(0.00001, parameters.arrivalRatePerSec());
    Duration duration = toDuration(parameters.duration());

    long durationNanos = duration.isZero() ? Long.MAX_VALUE : duration.toNanos();
    long startNanos = System.nanoTime();
    long intervalNanos = (long) Math.max(1, 1_000_000_000L / arrivalRatePerSec);
    AtomicBoolean cancelled = new AtomicBoolean(false);
    AtomicLong launchedTasks = new AtomicLong();
    AtomicLong completedTasks = new AtomicLong();

    Semaphore permits = new Semaphore(maxConcurrent);

    ThreadFactory threadFactory =
        runnable -> {
          Thread thread = new Thread(runnable);
          thread.setName("open-load-task-" + taskId + "-" + thread.getId());
          thread.setDaemon(true);
          return thread;
        };

    ScheduledExecutorService scheduler = newScheduledThreadPool(maxConcurrent, threadFactory);
    Runnable scheduleLoop =
        () -> {
          try {
            if (cancelled.get()) {
              return;
            }
            if (shouldStop(cancellationRequested)) {
              cancelled.set(true);
              return;
            }
            long elapsed = System.nanoTime() - startNanos;
            if (elapsed >= durationNanos) {
              return;
            }
            if (!permits.tryAcquire()) {
              return;
            }
            launchedTasks.incrementAndGet();
            scheduler.execute(
                () -> executeIteration(iterationTask, permits, completedTasks, cancelled));
          } catch (Throwable throwable) {
            log.error(
                "Task {} open load scheduler encountered error: {}",
                taskId,
                throwable.getMessage(),
                throwable);
            cancelled.set(true);
          }
        };

    long initialDelay = 0L;
    scheduler.scheduleAtFixedRate(scheduleLoop, initialDelay, intervalNanos, TimeUnit.NANOSECONDS);

    while (!cancelled.get()) {
      if (shouldStop(cancellationRequested)) {
        cancelled.set(true);
        break;
      }
      long elapsed = System.nanoTime() - startNanos;
      if (elapsed >= durationNanos) {
        break;
      }
      TimeUnit.MILLISECONDS.sleep(50);
    }

    scheduler.shutdown();
    try {
      scheduler.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw interrupted;
    }

    return new OpenLoadResult(launchedTasks.get(), completedTasks.get(), cancelled.get());
  }

  private static void validateTask(
      UUID taskId,
      OpenLoadParameters parameters,
      BooleanSupplier cancellationRequested,
      Runnable iterationTask) {
    Objects.requireNonNull(taskId, "taskId");
    Objects.requireNonNull(parameters, "parameters");
    Objects.requireNonNull(cancellationRequested, "cancellationRequested");
    Objects.requireNonNull(iterationTask, "iterationTask");
  }

  private static void executeIteration(
      Runnable iterationTask,
      Semaphore permits,
      AtomicLong completedTasks,
      AtomicBoolean cancelled) {
    try {
      iterationTask.run();
    } catch (Exception ex) {
      log.error("Open load iteration failed: {}", ex.getMessage(), ex);
      cancelled.set(true);
    } finally {
      permits.release();
      completedTasks.incrementAndGet();
    }
  }

  private static boolean shouldStop(BooleanSupplier cancellationRequested) {
    return Thread.currentThread().isInterrupted() || cancellationRequested.getAsBoolean();
  }
}
