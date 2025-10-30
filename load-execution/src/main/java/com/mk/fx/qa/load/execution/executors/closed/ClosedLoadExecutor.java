package com.mk.fx.qa.load.execution.executors.closed;

import static com.mk.fx.qa.load.execution.utils.LoadUtils.toDuration;
import static java.util.concurrent.Executors.newFixedThreadPool;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ClosedLoadExecutor {

  private static final long SLEEP_CHUNK_MILLIS = 100L;

  private ClosedLoadExecutor() {
    throw new UnsupportedOperationException("ClosedLoadExecutor cannot be instantiated");
  }

  public static ClosedLoadResult execute(
      UUID taskId,
      ClosedLoadParameters parameters,
      BooleanSupplier cancellationRequested,
      VirtualUserIterationRunner iterationRunner)
      throws Exception {
    validateTask(taskId, parameters, cancellationRequested, iterationRunner);

    var users = Math.max(1, parameters.users());
    var iterations = Math.max(1, parameters.iterations());
    var warmup = toDuration(parameters.warmup());
    var rampUp = toDuration(parameters.rampUp());
    var holdFor = toDuration(parameters.holdFor());

    var cancellationObserved = new AtomicBoolean(false);
    var holdExpired = new AtomicBoolean(false);
    var completedUsers = new AtomicInteger();

    if (!warmup.isZero()) {
      log.info("Task {} entering warmup for {}", taskId, warmup);
      sleepWithCancellation(warmup, cancellationRequested, cancellationObserved);
      log.info("Task {} completed warmup", taskId);
    }

    ThreadFactory threadFactory =
        runnable -> {
          Thread thread = new Thread(runnable);
          thread.setName("closed-load-task-" + taskId + "-" + thread.getId());
          thread.setDaemon(true);
          return thread;
        };

    var executor = newFixedThreadPool(users, threadFactory);
    List<Future<?>> futures = new ArrayList<>();

    var holdDeadline = holdFor.isZero() ? Long.MAX_VALUE : System.nanoTime() + holdFor.toNanos();
    var rampIntervalMillis = computeRampIntervalMillis(users, rampUp);

    try {
      log.info("Task {} starting ramp-up for {} users over {}", taskId, users, rampUp);
      for (int userIndex = 0; userIndex < users; userIndex++) {
        if (shouldStop(cancellationRequested, cancellationObserved)
            || isHoldExpired(holdDeadline)) {
          holdExpired.compareAndSet(false, isHoldExpired(holdDeadline));
          log.info(
              "Task {} stopping ramp-up at user {} due to {}",
              taskId,
              userIndex,
              holdExpired.get() ? "hold expiration" : "cancellation");
          break;
        }

        final var currentUser = userIndex;
        futures.add(
            executor.submit(
                () ->
                    runVirtualUser(
                        taskId,
                        users,
                        currentUser,
                        iterations,
                        holdDeadline,
                        cancellationRequested,
                        cancellationObserved,
                        holdExpired,
                        completedUsers,
                        iterationRunner)));

        if (userIndex < users - 1 && rampIntervalMillis > 0) {
          sleepWithCancellation(
              Duration.ofMillis((long) rampIntervalMillis),
              cancellationRequested,
              cancellationObserved);
        }
      }

      log.info("Task {} ramp-up complete, awaiting user completion", taskId);
      waitForUsers(
          futures, holdDeadline, cancellationRequested, cancellationObserved, holdExpired, taskId);
    } finally {
      executor.shutdownNow();
      try {
        executor.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw interrupted;
      }
    }

    return new ClosedLoadResult(
        users, completedUsers.get(), cancellationObserved.get(), holdExpired.get());
  }

  private static void validateTask(
      UUID taskId,
      ClosedLoadParameters parameters,
      BooleanSupplier cancellationRequested,
      VirtualUserIterationRunner iterationRunner) {
    Objects.requireNonNull(taskId, "taskId");
    Objects.requireNonNull(parameters, "parameters");
    Objects.requireNonNull(cancellationRequested, "cancellationRequested");
    Objects.requireNonNull(iterationRunner, "iterationRunner");
  }

  private static void runVirtualUser(
      UUID taskId,
      int totalUsers,
      int userIndex,
      int iterations,
      long holdDeadline,
      BooleanSupplier cancellationRequested,
      AtomicBoolean cancellationObserved,
      AtomicBoolean holdExpired,
      AtomicInteger completedUsers,
      VirtualUserIterationRunner iterationRunner) {
    log.info("Task {} virtual user {} started", taskId, userIndex + 1);
    for (int iteration = 0; iteration < iterations; iteration++) {
      if (shouldStop(cancellationRequested, cancellationObserved)) {
        log.info(
            "Task {} virtual user {} stopping due to cancellation at iteration {}",
            taskId,
            userIndex + 1,
            iteration);
        return;
      }

      if (isHoldExpired(holdDeadline)) {
        holdExpired.set(true);
        log.info(
            "Task {} virtual user {} stopping due to hold expiration at iteration {}",
            taskId,
            userIndex + 1,
            iteration);
        return;
      }
      try {
        iterationRunner.run(userIndex, iteration);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        log.info("Task {} virtual user {} interrupted", taskId, userIndex + 1);
        return;
      } catch (Exception ex) {
        throw new RuntimeException("Virtual user iteration failed", ex);
      }
    }
    var done = completedUsers.incrementAndGet();
    log.info(
        "Task {} virtual user {} completed all {} iterations (users completed {}/{})",
        taskId,
        userIndex + 1,
        iterations,
        done,
        totalUsers);
  }

  private static void waitForUsers(
      List<Future<?>> futures,
      long holdDeadline,
      BooleanSupplier cancellationRequested,
      AtomicBoolean cancellationObserved,
      AtomicBoolean holdExpired,
      UUID taskId)
      throws Exception {
    for (Future<?> future : futures) {
      if (future == null) {
        continue;
      }
      try {
        if (shouldStop(cancellationRequested, cancellationObserved)) {
          future.cancel(true);
          continue;
        }
        if (isHoldExpired(holdDeadline)) {
          holdExpired.set(true);
          future.cancel(true);
          continue;
        }
        future.get();
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw interrupted;
      } catch (java.util.concurrent.CancellationException ignored) {
        log.debug("Task {} future cancelled", taskId);
      }
    }
  }

  private static boolean shouldStop(BooleanSupplier cancelled, AtomicBoolean cancellationObserved) {
    var requested = Thread.currentThread().isInterrupted() || cancelled.getAsBoolean();
    if (requested) {
      cancellationObserved.set(true);
    }
    return requested;
  }

  private static boolean isHoldExpired(long holdDeadline) {
    return System.nanoTime() >= holdDeadline;
  }

  private static double computeRampIntervalMillis(int users, Duration rampUp) {
    if (users <= 1 || rampUp.isZero()) {
      return 0;
    }
    return rampUp.toMillis() / (double) (users - 1);
  }

  private static void sleepWithCancellation(
      Duration duration, BooleanSupplier cancellationRequested, AtomicBoolean cancellationObserved)
      throws InterruptedException {
    long remaining = duration.toMillis();
    while (remaining > 0) {
      if (shouldStop(cancellationRequested, cancellationObserved)) {
        throw new InterruptedException("Cancelled during sleep");
      }
      var chunk = Math.min(SLEEP_CHUNK_MILLIS, remaining);
      TimeUnit.MILLISECONDS.sleep(chunk);
      remaining -= chunk;
    }
  }
}
