package com.mk.fx.qa.load.execution.executors.open;

import static com.mk.fx.qa.load.execution.utils.LoadUtils.toDuration;
import static java.util.concurrent.Executors.newFixedThreadPool;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.concurrent.locks.LockSupport;
import lombok.extern.slf4j.Slf4j;

/**
 * Executes an "open" load model where requests are launched at a target arrival rate up to a
 * maximum concurrency. Uses a scheduled executor to fire iterations at fixed intervals while
 * respecting cancellation and total duration limits.
 */
@Slf4j
public final class OpenLoadExecutor {

  private OpenLoadExecutor() {
    throw new UnsupportedOperationException("OpenLoadExecutor cannot be instantiated");
  }

  /**
   * Runs an open model execution.
   *
   * @param taskId unique task identifier used for thread names and logs
   * @param parameters open model parameters (arrival rate, max concurrency, duration)
   * @param cancellationRequested supplier checked for cooperative cancellation
   * @param iterationTask work to execute per launched request
   * @return result including launched/completed counts and cancellation flag
   * @throws InterruptedException if the calling thread is interrupted while coordinating
   */
  public static OpenLoadResult execute(
      UUID taskId,
      OpenLoadParameters parameters,
      BooleanSupplier cancellationRequested,
      Runnable iterationTask)
      throws InterruptedException {
    return execute(taskId, parameters, cancellationRequested, iterationTask, OpenLoadOptions.defaults(), null);
  }

  /**
   * Runs an open model execution with options and optional metrics listener.
   */
  public static OpenLoadResult execute(
      UUID taskId,
      OpenLoadParameters parameters,
      BooleanSupplier cancellationRequested,
      Runnable iterationTask,
      OpenLoadOptions options,
      Consumer<OpenLoadMetrics> metricsListener)
      throws InterruptedException {

    validateTask(taskId, parameters, cancellationRequested, iterationTask);

    int maxConcurrent = Math.max(1, parameters.maxConcurrent());
    double arrivalRatePerSec = Math.max(0.00001, parameters.arrivalRatePerSec());
    Duration duration = toDuration(parameters.duration());

    long durationNanos = duration.isZero() ? Long.MAX_VALUE : duration.toNanos();
    long startNanos = System.nanoTime();
    long intervalNanos = (long) Math.max(1, 1_000_000_000L / arrivalRatePerSec);
    AtomicBoolean cancelled = new AtomicBoolean(false);
    AtomicLong scheduledTicks = new AtomicLong();
    AtomicLong launchedTasks = new AtomicLong();
    AtomicLong startedTasks = new AtomicLong();
    AtomicLong completedTasks = new AtomicLong();
    AtomicLong failedTasks = new AtomicLong();
    AtomicLong droppedTasks = new AtomicLong();
    AtomicLong backlog = new AtomicLong();

    Semaphore permits = new Semaphore(maxConcurrent);

    ThreadFactory threadFactory =
        runnable -> {
          Thread thread = new Thread(runnable);
          thread.setName("open-load-task-" + taskId + "-" + thread.getId());
          thread.setDaemon(true);
          return thread;
        };

    ExecutorService worker = newFixedThreadPool(maxConcurrent, threadFactory);

    // Deterministic nano-time based ticker on a dedicated thread
    Thread schedulerThread =
        new Thread(
            () -> {
              long nextTick = startNanos + intervalNanos;
              try {
                while (!cancelled.get()) {
                  if (shouldStop(cancellationRequested)) {
                    cancelled.set(true);
                    break;
                  }

                  long now = System.nanoTime();
                  long elapsed = now - startNanos;
                  if (elapsed >= durationNanos) {
                    break; // stop scheduling new arrivals
                  }

                  long sleepNanos = nextTick - now;
                  if (sleepNanos > 0) {
                    // Park precisely until next tick
                    LockSupport.parkNanos(sleepNanos);
                    continue;
                  }

                  // Tick boundary reached
                  scheduledTicks.incrementAndGet();

                  boolean acquired = permits.tryAcquire();
                  if (acquired) {
                    launchedTasks.incrementAndGet();
                    worker.execute(
                        () ->
                            executeIteration(
                                iterationTask,
                                permits,
                                startedTasks,
                                completedTasks,
                                failedTasks,
                                cancelled,
                                options,
                                backlog,
                                launchedTasks,
                                worker,
                                taskId));
                  } else {
                    switch (options.saturationPolicy()) {
                      case DROP -> droppedTasks.incrementAndGet();
                      case DELAY -> backlog.incrementAndGet();
                    }
                  }

                  // Align nextTick to the next boundary strictly after 'now' to avoid bursts
                  long ticksSinceStart = Math.max(1L, ((now - startNanos) / intervalNanos) + 1L);
                  nextTick = startNanos + ticksSinceStart * intervalNanos;

                  // Push metrics if requested
                  if (metricsListener != null) {
                    long inflight = Math.max(0L, startedTasks.get() - completedTasks.get());
                    metricsListener.accept(
                        new OpenLoadMetrics(
                            scheduledTicks.get(),
                            launchedTasks.get(),
                            startedTasks.get(),
                            completedTasks.get(),
                            failedTasks.get(),
                            droppedTasks.get(),
                            inflight,
                            backlog.get()));
                  }
                }
              } catch (Throwable throwable) {
                log.error(
                    "Task {} open load scheduler encountered error: {}",
                    taskId,
                    throwable.getMessage(),
                    throwable);
                cancelled.set(true);
              }
            },
            "open-load-scheduler-" + taskId);
    schedulerThread.setDaemon(true);
    schedulerThread.start();

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

    // Stop scheduling new arrivals; allow delayed backlog to drain if configured
    try {
      schedulerThread.join(TimeUnit.SECONDS.toMillis(5));
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw interrupted;
    }

    if (!cancelled.get() && options.saturationPolicy() == OpenLoadOptions.SaturationPolicy.DELAY) {
      // Wait for backlog to drain cooperatively
      long spinWaitMillis = 0L;
      while (backlog.get() > 0 && !cancelled.get()) {
        if (shouldStop(cancellationRequested)) {
          cancelled.set(true);
          break;
        }
        TimeUnit.MILLISECONDS.sleep(50);
        spinWaitMillis += 50;
        if (spinWaitMillis > TimeUnit.MINUTES.toMillis(5)) {
          log.warn(
              "Task {} backlog did not drain within timeout; remaining={}", taskId, backlog.get());
          break;
        }
      }
    }

    worker.shutdown();
    try {
      if (!worker.awaitTermination(30, TimeUnit.SECONDS)) {
        worker.shutdownNow();
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw interrupted;
    }

    return new OpenLoadResult(launchedTasks.get(), completedTasks.get(), cancelled.get());
  }

  /** Validates mandatory inputs for an open execution. */
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

  /** Executes a single iteration, signalling cancellation on errors and releasing permits. */
  private static void executeIteration(
      Runnable iterationTask,
      Semaphore permits,
      AtomicLong startedTasks,
      AtomicLong completedTasks,
      AtomicLong failedTasks,
      AtomicBoolean cancelled,
      OpenLoadOptions options,
      AtomicLong backlog,
      AtomicLong launchedTasks,
      ExecutorService worker,
      UUID taskId) {
    startedTasks.incrementAndGet();
    try {
      iterationTask.run();
    } catch (Exception ex) {
      failedTasks.incrementAndGet();
      log.error("Open load iteration failed: {}", ex.getMessage(), ex);
      if (options.iterationFailurePolicy() == OpenLoadOptions.IterationFailurePolicy.CANCEL_TEST) {
        cancelled.set(true);
      }
    } finally {
      permits.release();
      completedTasks.incrementAndGet();

      // Drain delayed backlog opportunistically on completion
      if (options.saturationPolicy() == OpenLoadOptions.SaturationPolicy.DELAY) {
        while (backlog.get() > 0 && permits.tryAcquire()) {
          long remaining = backlog.decrementAndGet();
          if (remaining < 0) {
            // Another thread drained it first; undo decrement and release permit
            backlog.incrementAndGet();
            permits.release();
            break;
          }
          launchedTasks.incrementAndGet();
          worker.execute(
              () ->
                  executeIteration(
                      iterationTask,
                      permits,
                      startedTasks,
                      completedTasks,
                      failedTasks,
                      cancelled,
                      options,
                      backlog,
                      launchedTasks,
                      worker,
                      taskId));
        }
      }
    }
  }

  /** Returns true if current thread is interrupted or external cancellation is signalled. */
  private static boolean shouldStop(BooleanSupplier cancellationRequested) {
    return Thread.currentThread().isInterrupted() || cancellationRequested.getAsBoolean();
  }
}
