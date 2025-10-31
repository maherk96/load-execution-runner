package com.mk.fx.qa.load.execution.executors.open;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

class OpenLoadExecutorTest {

  private static OpenLoadParameters params(double rate, int maxConc, Duration dur) {
    return new OpenLoadParameters(rate, maxConc, dur);
  }

  private static BooleanSupplier neverCancel() { return () -> false; }

  @Test
  void executesWithinDuration_respectsConcurrency_andCounts() throws Exception {
    UUID taskId = UUID.randomUUID();
    // Short duration with high arrival rate and limited concurrency
    var p = params(200.0, 3, Duration.ofMillis(200));
    AtomicInteger current = new AtomicInteger();
    AtomicInteger peak = new AtomicInteger();
    AtomicInteger ran = new AtomicInteger();

    Runnable iteration =
        () -> {
          ran.incrementAndGet();
          int now = current.incrementAndGet();
          peak.accumulateAndGet(now, Math::max);
          try {
            TimeUnit.MILLISECONDS.sleep(10);
          } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
          } finally {
            current.decrementAndGet();
          }
        };

    OpenLoadResult result = OpenLoadExecutor.execute(taskId, p, neverCancel(), iteration);

    assertFalse(result.cancelled());
    assertEquals(result.launched(), result.completed());
    assertTrue(result.launched() > 0);
    assertTrue(peak.get() <= 3, "Peak concurrency exceeded maxConcurrent");
    assertEquals(ran.get(), result.completed());
  }

  @Test
  void cancellationDuringRun_setsCancelled_andStopsScheduling() throws Exception {
    UUID taskId = UUID.randomUUID();
    var p = params(500.0, 5, Duration.ofSeconds(2));
    AtomicBoolean cancel = new AtomicBoolean(false);
    AtomicBoolean firstRun = new AtomicBoolean(true);
    Runnable iteration =
        () -> {
          if (firstRun.compareAndSet(true, false)) {
            cancel.set(true);
          }
        };

    OpenLoadResult result = OpenLoadExecutor.execute(taskId, p, cancel::get, iteration);
    assertTrue(result.cancelled());
    assertEquals(result.launched(), result.completed());
  }

  @Test
  void iterationThrows_exceptionCancelsAndReleasesPermits() throws Exception {
    UUID taskId = UUID.randomUUID();
    var p = params(100.0, 2, Duration.ofSeconds(1));
    Runnable iteration =
        () -> {
          throw new RuntimeException("boom");
        };

    OpenLoadResult result = OpenLoadExecutor.execute(taskId, p, neverCancel(), iteration);
    assertTrue(result.cancelled());
    assertTrue(result.launched() >= 1);
    assertEquals(result.launched(), result.completed());
  }

  @Test
  void minArrivalRateClamped_executesOrReturnsCleanly() throws Exception {
    UUID taskId = UUID.randomUUID();
    var p = params(1e-12, 2, Duration.ofMillis(20));
    Runnable iteration = () -> {};

    OpenLoadResult result = OpenLoadExecutor.execute(taskId, p, neverCancel(), iteration);
    assertFalse(result.cancelled());
    assertEquals(result.launched(), result.completed());
    assertTrue(result.launched() >= 0);
  }

  @Test
  void maxConcurrentClampedToAtLeastOne() throws Exception {
    UUID taskId = UUID.randomUUID();
    var p = params(500.0, 0, Duration.ofMillis(100));
    AtomicInteger current = new AtomicInteger();
    AtomicInteger peak = new AtomicInteger();

    Runnable iteration =
        () -> {
          int now = current.incrementAndGet();
          peak.accumulateAndGet(now, Math::max);
          try {
            TimeUnit.MILLISECONDS.sleep(10);
          } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
          } finally {
            current.decrementAndGet();
          }
        };

    OpenLoadResult result = OpenLoadExecutor.execute(taskId, p, neverCancel(), iteration);
    assertFalse(result.cancelled());
    assertTrue(peak.get() <= 1);
    assertEquals(result.launched(), result.completed());
  }

  @Test
  void validateTask_nullInputs_throwNpe() {
    var p = params(1.0, 1, Duration.ofMillis(10));
    assertThrows(NullPointerException.class, () -> OpenLoadExecutor.execute(null, p, () -> false, () -> {}));
    assertThrows(NullPointerException.class, () -> OpenLoadExecutor.execute(UUID.randomUUID(), null, () -> false, () -> {}));
    assertThrows(NullPointerException.class, () -> OpenLoadExecutor.execute(UUID.randomUUID(), p, null, () -> {}));
    assertThrows(NullPointerException.class, () -> OpenLoadExecutor.execute(UUID.randomUUID(), p, () -> false, null));
  }
}

