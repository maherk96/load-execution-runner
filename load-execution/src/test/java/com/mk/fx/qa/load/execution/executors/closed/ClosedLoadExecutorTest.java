package com.mk.fx.qa.load.execution.executors.closed;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

class ClosedLoadExecutorTest {

  /* ---------------- helpers ---------------- */

  private static ClosedLoadParameters params(
          int users, int iterations, Duration warm, Duration ramp, Duration hold) {
    return new ClosedLoadParameters(users, iterations, warm, ramp, hold);
  }

  private static BooleanSupplier neverCancel() {
    return () -> false;
  }

  /* ---------------- core execution ---------------- */

  @Test
  void executesAllUsersComplete_whenSufficientHoldAndNoCancel() throws Exception {
    UUID taskId = UUID.randomUUID();
    var p = params(3, 2, Duration.ZERO, Duration.ZERO, Duration.ofSeconds(2));
    var runnerCalls = new AtomicInteger();

    ClosedLoadResult result =
            ClosedLoadExecutor.execute(
                    taskId, p, neverCancel(), (u, i) -> runnerCalls.incrementAndGet());

    assertEquals(3, result.totalUsers());
    assertEquals(3, result.completedUsers());
    assertFalse(result.cancelled());
    assertFalse(result.holdExpired());
    assertEquals(6, runnerCalls.get());
  }

  /* ---------------- warmup ---------------- */

  @Test
  void cancellationDuringWarmup_throwsInterruptedException() {
    UUID taskId = UUID.randomUUID();
    var p = params(1, 1, Duration.ofMillis(300), Duration.ZERO, Duration.ofSeconds(1));
    AtomicBoolean cancel = new AtomicBoolean(true);

    InterruptedException ex =
            assertThrows(
                    InterruptedException.class,
                    () -> ClosedLoadExecutor.execute(taskId, p, cancel::get, (u, i) -> {}));

    assertEquals("Cancelled during sleep", ex.getMessage());
  }

  /* ---------------- hold expiry ---------------- */

  @Test
  void holdExpires_duringRampOrRun_setsHoldExpiredFlag() throws Exception {
    UUID taskId = UUID.randomUUID();
    var p = params(5, 5, Duration.ZERO, Duration.ofMillis(500), Duration.ofMillis(50));

    ClosedLoadResult result =
            ClosedLoadExecutor.execute(taskId, p, neverCancel(), (u, i) -> {});

    assertTrue(result.holdExpired());
    assertFalse(result.cancelled());
    assertTrue(result.completedUsers() >= 0);
    assertTrue(result.completedUsers() <= result.totalUsers());
  }

  @Test
  void durationStop_preventsAllUsersCompleting() throws Exception {
    UUID taskId = UUID.randomUUID();
    var p = params(3, 100, Duration.ZERO, Duration.ZERO, Duration.ofMillis(100));

    ClosedLoadResult result =
            ClosedLoadExecutor.execute(
                    taskId,
                    p,
                    neverCancel(),
                    (u, i) -> Thread.sleep(10));

    assertTrue(result.holdExpired());
    assertTrue(result.completedUsers() < result.totalUsers());
  }

  /* ---------------- cancellation ---------------- */

  @Test
  void cancellationDuringRun_setsCancelledFlag_andStopsUsers() throws Exception {
    UUID taskId = UUID.randomUUID();
    var p = params(3, 1000, Duration.ZERO, Duration.ZERO, Duration.ofSeconds(5));
    AtomicBoolean cancel = new AtomicBoolean(false);

    ClosedLoadResult result =
            ClosedLoadExecutor.execute(
                    taskId,
                    p,
                    cancel::get,
                    (u, i) -> {
                      if (u == 0 && i == 1) {
                        cancel.set(true);
                      }
                      Thread.sleep(1);
                    });

    assertTrue(result.cancelled());
    assertTrue(result.completedUsers() < result.totalUsers());
  }

  /* ---------------- failure behaviour ---------------- */

  @Test
  void oneUserFailure_doesNotCancelOthers_byDefault() throws Exception {
    UUID taskId = UUID.randomUUID();
    var p = params(3, 2, Duration.ZERO, Duration.ZERO, Duration.ofSeconds(2));

    ClosedLoadResult result =
            ClosedLoadExecutor.execute(
                    taskId,
                    p,
                    neverCancel(),
                    (u, i) -> {
                      if (u == 1 && i == 0) throw new RuntimeException("boom");
                    });

    assertEquals(3, result.totalUsers());
    assertEquals(2, result.completedUsers());
    assertFalse(result.cancelled());
    assertFalse(result.holdExpired());
  }

  /* ---------------- parameter clamping ---------------- */

  @Test
  void zeroUsersOrIterations_areClampedToMinimumOne() throws Exception {
    UUID taskId = UUID.randomUUID();
    var p = params(0, 0, Duration.ZERO, Duration.ZERO, Duration.ofSeconds(1));
    var completed = new AtomicInteger();

    ClosedLoadResult result =
            ClosedLoadExecutor.execute(
                    taskId, p, neverCancel(), (u, i) -> completed.incrementAndGet());

    assertEquals(1, result.totalUsers());
    assertEquals(1, result.completedUsers());
    assertEquals(1, completed.get());
  }

  /* ---------------- ramp-up behaviour ---------------- */

  @Test
  void rampUp_delaysUserSubmission_orderPreserved() throws Exception {
    UUID taskId = UUID.randomUUID();
    int users = 3;
    var p = params(users, 1, Duration.ZERO, Duration.ofMillis(200), Duration.ofSeconds(2));

    long[] firstRunAt = new long[users];

    ClosedLoadResult result =
            ClosedLoadExecutor.execute(
                    taskId,
                    p,
                    neverCancel(),
                    (u, i) -> {
                      if (i == 0) {
                        firstRunAt[u] = System.nanoTime();
                      }
                    });

    assertEquals(users, result.completedUsers());
    assertTrue(firstRunAt[1] == 0 || firstRunAt[1] >= firstRunAt[0]);
    assertTrue(firstRunAt[2] == 0 || firstRunAt[2] >= firstRunAt[1]);
  }

  /* ---------------- validation ---------------- */

  @Test
  void validateTask_nullInputs_throwNpe() {
    var p = params(1, 1, Duration.ZERO, Duration.ZERO, Duration.ZERO);

    assertThrows(
            NullPointerException.class,
            () -> ClosedLoadExecutor.execute(null, p, neverCancel(), (u, i) -> {}));

    assertThrows(
            NullPointerException.class,
            () -> ClosedLoadExecutor.execute(UUID.randomUUID(), null, neverCancel(), (u, i) -> {}));

    assertThrows(
            NullPointerException.class,
            () -> ClosedLoadExecutor.execute(UUID.randomUUID(), p, null, (u, i) -> {}));

    assertThrows(
            NullPointerException.class,
            () -> ClosedLoadExecutor.execute(UUID.randomUUID(), p, neverCancel(), null));
  }
}