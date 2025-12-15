package com.mk.fx.qa.load.execution.executors.open;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.Test;

class OpenLoadExecutorTest {

    /* ---------------- helpers ---------------- */

    private static OpenLoadParameters params(double rate, int maxConc, Duration dur) {
        return new OpenLoadParameters(rate, maxConc, dur);
    }

    private static BooleanSupplier neverCancel() {
        return () -> false;
    }

    /* ---------------- core behaviour ---------------- */

    @Test
    void executesWithinDuration_respectsConcurrency_andCounts() throws Exception {
        UUID taskId = UUID.randomUUID();

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

        OpenLoadResult result =
                OpenLoadExecutor.execute(taskId, p, neverCancel(), iteration);

        assertFalse(result.cancelled(), "Execution should not be cancelled");
        assertTrue(result.launched() > 0, "No iterations launched");
        assertEquals(result.launched(), result.completed(), "Launched != completed");
        assertEquals(ran.get(), result.completed(), "Completed count mismatch");
        assertTrue(peak.get() <= 3, "Peak concurrency exceeded maxConcurrent");
    }

    /* ---------------- cancellation ---------------- */

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

        OpenLoadResult result =
                OpenLoadExecutor.execute(taskId, p, cancel::get, iteration);

        assertTrue(result.cancelled(), "Execution should be cancelled");
        assertEquals(result.launched(), result.completed(), "Permits leaked on cancel");
    }

    @Test
    void cancellationSupplierAlreadyTrue_exitsCleanly() throws Exception {
        UUID taskId = UUID.randomUUID();
        var p = params(100.0, 3, Duration.ofSeconds(1));

        OpenLoadResult result =
                OpenLoadExecutor.execute(taskId, p, () -> true, () -> fail("Should not run"));

        assertTrue(result.cancelled());
        assertEquals(0, result.launched());
        assertEquals(0, result.completed());
    }

    /* ---------------- failure handling ---------------- */

    @Test
    void iterationThrows_exceptionCancelsAndReleasesPermits() throws Exception {
        UUID taskId = UUID.randomUUID();
        var p = params(100.0, 2, Duration.ofSeconds(1));

        Runnable iteration =
                () -> {
                    throw new RuntimeException("boom");
                };

        OpenLoadResult result =
                OpenLoadExecutor.execute(taskId, p, neverCancel(), iteration);

        assertTrue(result.cancelled(), "Failure should cancel execution");
        assertTrue(result.launched() >= 1, "At least one iteration expected");
        assertEquals(result.launched(), result.completed(), "Permits not released");
    }

    @Test
    void iterationThrows_continuePolicy_doesNotCancel() throws Exception {
        UUID taskId = UUID.randomUUID();
        var p = params(50.0, 1, Duration.ofMillis(200));

        OpenLoadOptions options =
                new OpenLoadOptions(
                        OpenLoadOptions.SaturationPolicy.DROP,
                        OpenLoadOptions.IterationFailurePolicy.CONTINUE);

        Runnable iteration =
                () -> {
                    throw new RuntimeException("boom");
                };

        OpenLoadResult result =
                OpenLoadExecutor.execute(taskId, p, neverCancel(), iteration, options, null);

        assertFalse(result.cancelled(), "Execution should continue on failure");
        assertTrue(result.completed() > 1, "Expected multiple failures");
    }

    /* ---------------- arrival rate & clamping ---------------- */

    @Test
    void minArrivalRateClamped_executesOrReturnsCleanly() throws Exception {
        UUID taskId = UUID.randomUUID();
        var p = params(1e-12, 2, Duration.ofMillis(20));

        Runnable iteration = () -> {};

        OpenLoadResult result =
                OpenLoadExecutor.execute(taskId, p, neverCancel(), iteration);

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

        OpenLoadResult result =
                OpenLoadExecutor.execute(taskId, p, neverCancel(), iteration);

        assertFalse(result.cancelled());
        assertTrue(peak.get() <= 1, "Concurrency exceeded clamped value");
        assertEquals(result.launched(), result.completed());
    }

    /* ---------------- saturation policies ---------------- */

    @Test
    void saturationDrop_limitsExecutionUnderLoad() throws Exception {
        UUID taskId = UUID.randomUUID();

        OpenLoadOptions options =
                new OpenLoadOptions(
                        OpenLoadOptions.SaturationPolicy.DROP,
                        OpenLoadOptions.IterationFailurePolicy.CANCEL_TEST);

        AtomicInteger current = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        AtomicLong ran = new AtomicLong();

        Runnable slowIteration =
                () -> {
                    ran.incrementAndGet();
                    int now = current.incrementAndGet();
                    peak.accumulateAndGet(now, Math::max);
                    try {
                        TimeUnit.MILLISECONDS.sleep(100);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    } finally {
                        current.decrementAndGet();
                    }
                };

        var p = params(500.0, 1, Duration.ofMillis(300));

        OpenLoadResult result =
                OpenLoadExecutor.execute(
                        taskId, p, neverCancel(), slowIteration, options, null);

        // ✅ Correct invariants
        assertFalse(result.cancelled());
        assertEquals(result.launched(), result.completed());
        assertEquals(result.completed(), ran.get());

        // ✅ Concurrency never exceeded
        assertTrue(peak.get() <= 1);

        // ✅ Proof of dropping: far fewer executions than arrival rate implies
        // 500 rps for 300ms ≈ 150 attempts, but concurrency=1 + 100ms work
        assertTrue(
                result.completed() < 10,
                "Expected throttled execution due to DROP policy");
    }

    @Test
    void saturationDelay_drainsBacklogEventually() throws Exception {
        UUID taskId = UUID.randomUUID();

        OpenLoadOptions options =
                new OpenLoadOptions(
                        OpenLoadOptions.SaturationPolicy.DELAY,
                        OpenLoadOptions.IterationFailurePolicy.CANCEL_TEST);

        AtomicLong ran = new AtomicLong();

        Runnable iteration =
                () -> {
                    ran.incrementAndGet();
                    try {
                        TimeUnit.MILLISECONDS.sleep(20);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                };

        var p = params(200.0, 1, Duration.ofMillis(300));

        OpenLoadResult result =
                OpenLoadExecutor.execute(
                        taskId, p, neverCancel(), iteration, options, null);

        assertEquals(result.completed(), ran.get(), "Backlog did not drain fully");
    }

    /* ---------------- metrics ---------------- */

    @Test
    void metricsListener_receivesUpdates() throws Exception {
        UUID taskId = UUID.randomUUID();
        AtomicInteger calls = new AtomicInteger();

        var p = params(50.0, 2, Duration.ofMillis(200));

        OpenLoadExecutor.execute(
                taskId,
                p,
                neverCancel(),
                () -> {},
                OpenLoadOptions.defaults(),
                metrics -> calls.incrementAndGet());

        assertTrue(calls.get() > 0, "Metrics listener not invoked");
    }

    /* ---------------- validation ---------------- */

    @Test
    void validateTask_nullInputs_throwNpe() {
        var p = params(1.0, 1, Duration.ofMillis(10));

        assertThrows(
                NullPointerException.class,
                () -> OpenLoadExecutor.execute(null, p, () -> false, () -> {}));

        assertThrows(
                NullPointerException.class,
                () -> OpenLoadExecutor.execute(UUID.randomUUID(), null, () -> false, () -> {}));

        assertThrows(
                NullPointerException.class,
                () -> OpenLoadExecutor.execute(UUID.randomUUID(), p, null, () -> {}));

        assertThrows(
                NullPointerException.class,
                () -> OpenLoadExecutor.execute(UUID.randomUUID(), p, () -> false, null));
    }
}