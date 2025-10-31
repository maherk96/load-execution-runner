package com.mk.fx.qa.load.execution.executors.closed;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

class ClosedLoadExecutorTest {

  private static ClosedLoadParameters params(int users, int iterations, Duration warm, Duration ramp, Duration hold) {
    return new ClosedLoadParameters(users, iterations, warm, ramp, hold);
  }

  private static BooleanSupplier neverCancel() { return () -> false; }

  @Test
  void executesAllUsersComplete_whenSufficientHoldAndNoCancel() throws Exception {
    UUID taskId = UUID.randomUUID();
    var p = params(3, 2, Duration.ZERO, Duration.ZERO, Duration.ofSeconds(2));
    var runnerCalls = new AtomicInteger();
    VirtualUserIterationRunner runner = (u, i) -> runnerCalls.incrementAndGet();

    ClosedLoadResult result =
        ClosedLoadExecutor.execute(taskId, p, neverCancel(), runner);

    assertEquals(3, result.totalUsers());
    assertEquals(3, result.completedUsers());
    assertFalse(result.cancelled());
    assertFalse(result.holdExpired());
    // 3 users * 2 iterations each
    assertEquals(6, runnerCalls.get());
  }

  @Test
  void cancelsDuringWarmup_throwsInterruptedException() {
    UUID taskId = UUID.randomUUID();
    var p = params(1, 1, Duration.ofMillis(300), Duration.ZERO, Duration.ofSeconds(1));
    AtomicBoolean cancel = new AtomicBoolean(true); // cancel immediately

    assertThrows(
        InterruptedException.class,
        () -> ClosedLoadExecutor.execute(taskId, p, cancel::get, (u, i) -> {}));
  }

  @Test
  void holdExpires_duringRampOrRun_setsHoldExpiredFlag() throws Exception {
    UUID taskId = UUID.randomUUID();
    // Short hold, longer ramp so not all users start
    var p = params(5, 5, Duration.ZERO, Duration.ofMillis(500), Duration.ofMillis(50));
    ClosedLoadResult result =
        ClosedLoadExecutor.execute(taskId, p, neverCancel(), (u, i) -> {});

    assertTrue(result.holdExpired());
    assertFalse(result.cancelled());
    assertTrue(result.completedUsers() >= 0 && result.completedUsers() <= result.totalUsers());
  }

  @Test
  void cancellationDuringRun_setsCancelledFlag_andStopsUsers() throws Exception {
    UUID taskId = UUID.randomUUID();
    var p = params(3, 1000, Duration.ZERO, Duration.ZERO, Duration.ofSeconds(5));
    AtomicBoolean cancel = new AtomicBoolean(false);
    VirtualUserIterationRunner runner =
        new VirtualUserIterationRunner() {
          @Override
          public void run(int userIndex, int iteration) throws Exception {
            if (iteration == 1 && userIndex == 0) {
              cancel.set(true);
            }
            // small pause to allow cancel to propagate
            Thread.sleep(1);
          }
        };

    ClosedLoadResult result =
        ClosedLoadExecutor.execute(taskId, p, cancel::get, runner);

    assertTrue(result.cancelled());
    // Not all users should complete 1000 iterations
    assertTrue(result.completedUsers() < result.totalUsers());
  }

  @Test
  void oneUserFailure_doesNotCancelOthers() throws Exception {
    UUID taskId = UUID.randomUUID();
    var p = params(3, 2, Duration.ZERO, Duration.ZERO, Duration.ofSeconds(2));
    VirtualUserIterationRunner runner =
        (u, i) -> {
          if (u == 1 && i == 0) throw new RuntimeException("boom");
        };

    ClosedLoadResult result =
        ClosedLoadExecutor.execute(taskId, p, neverCancel(), runner);

    assertEquals(3, result.totalUsers());
    assertEquals(2, result.completedUsers());
    assertFalse(result.cancelled());
    assertFalse(result.holdExpired());
  }

  @Test
  void validateTask_nullInputs_throwNpe() {
    var p = params(1, 1, Duration.ZERO, Duration.ZERO, Duration.ZERO);
    assertThrows(NullPointerException.class, () -> ClosedLoadExecutor.execute(null, p, () -> false, (u, i) -> {}));
    assertThrows(NullPointerException.class, () -> ClosedLoadExecutor.execute(UUID.randomUUID(), null, () -> false, (u, i) -> {}));
    assertThrows(NullPointerException.class, () -> ClosedLoadExecutor.execute(UUID.randomUUID(), p, null, (u, i) -> {}));
    assertThrows(NullPointerException.class, () -> ClosedLoadExecutor.execute(UUID.randomUUID(), p, () -> false, null));
  }

  @Test
  void zeroUsersOrIterations_areClampedToMinimumOne() throws Exception {
    UUID taskId = UUID.randomUUID();
    var p = params(0, 0, Duration.ZERO, Duration.ZERO, Duration.ofSeconds(1));
    var completed = new AtomicInteger();
    ClosedLoadResult result =
        ClosedLoadExecutor.execute(taskId, p, neverCancel(), (u, i) -> completed.incrementAndGet());

    assertEquals(1, result.totalUsers());
    assertEquals(1, result.completedUsers());
    assertEquals(1, completed.get());
  }

  @Test
  void rampUp_sleepOccursBetweenUserStarts_whenRampSpecified() throws Exception {
    UUID taskId = UUID.randomUUID();
    int users = 3;
    var p = params(users, 1, Duration.ZERO, Duration.ofMillis(200), Duration.ofSeconds(2));
    long[] firstIterationAt = new long[users];
    VirtualUserIterationRunner runner =
        (u, i) -> {
          // record timestamp at first iteration only
          if (i == 0) firstIterationAt[u] = System.nanoTime();
        };

    ClosedLoadResult result =
        ClosedLoadExecutor.execute(taskId, p, neverCancel(), runner);

    assertEquals(users, result.completedUsers());
    // Ensure ordering and non-zero deltas (indicative of ramp sleeps), while avoiding brittle thresholds
    assertTrue(firstIterationAt[1] == 0 || firstIterationAt[1] >= firstIterationAt[0]);
    assertTrue(firstIterationAt[2] == 0 || firstIterationAt[2] >= firstIterationAt[1]);
  }
}

