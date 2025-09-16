// Main orchestrator class - with cancellation mechanism
package org.load.execution.runner.load;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Simplified Load Test Execution Runner that orchestrates load tests with cancellation support.
 *
 * This refactored version focuses purely on coordination and delegates
 * specific responsibilities to specialized components.
 */
public class LoadTestExecutionRunner {

    private static final Logger log = LoggerFactory.getLogger(LoadTestExecutionRunner.class);

    private final TestPlanSpec testPlanSpec;
    private final TestPhaseManager phaseManager;
    private final RequestExecutor requestExecutor;
    private final ResourceManager resourceManager;
    private final WorkloadStrategyFactory strategyFactory;

    private final AtomicReference<String> terminationReason = new AtomicReference<>();
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    /**
     * Constructs a new LoadTestExecutionRunner with the specified test plan.
     */
    public LoadTestExecutionRunner(TestPlanSpec testPlanSpec) {
        this.testPlanSpec = testPlanSpec;
        this.phaseManager = new TestPhaseManager();
        this.requestExecutor = new RequestExecutor(testPlanSpec);
        this.resourceManager = new ResourceManager();
        this.strategyFactory = new WorkloadStrategyFactory();
    }

    /**
     * Main execution method that orchestrates the entire load test.
     */
    public CompletableFuture<Void> execute() {
        return CompletableFuture.runAsync(() -> {
            try {
                log.info("Starting load test execution: {}", testPlanSpec.getTestSpec().getId());

                phaseManager.startTest();
                var testStartTime = Instant.now();

                // Create and execute workload strategy
                var strategy = strategyFactory.createStrategy(
                        testPlanSpec.getExecution().getLoadModel(),
                        phaseManager,
                        requestExecutor,
                        resourceManager,
                        cancelled
                );

                strategy.execute(testPlanSpec);

                // Wait for completion
                phaseManager.waitForCompletion();

                var testEndTime = Instant.now();
                var totalDuration = Duration.between(testStartTime, testEndTime);

                // FIX: Ensure termination reason is always set


                phaseManager.completeTest();
                log.info("Load test completed. Duration: {}s, Reason: {}",
                        totalDuration.getSeconds(), getTerminationReason());

            } catch (Exception e) {
                log.error("Load test execution failed", e);
                terminateTest("EXECUTION_ERROR: " + e.getMessage());
                throw new RuntimeException("Load test execution failed", e);
            }
        }, resourceManager.getMainExecutor());
    }

    /**
     * Cancels the load test execution gracefully.
     * This will stop further request submissions and let running requests complete.
     */
    public void cancel() {
        if (cancelled.compareAndSet(false, true)) {
            log.info("Load test cancellation requested - stopping execution gracefully");
            terminateTest("CANCELLED");
        } else {
            log.debug("Load test cancellation already in progress");
        }
    }

    /**
     * Returns true if the test has been cancelled.
     */
    public boolean isCancelled() {
        return cancelled.get();
    }

    /**
     * Terminates the test with the specified reason.
     */
    public void terminateTest(String reason) {
        terminationReason.compareAndSet(null, reason);
        phaseManager.terminateTest(reason);
    }

    public String getTerminationReason() {
        // Always check phase manager first since that's where strategies set the reason
        String reason = phaseManager.getTerminationReason();
        if (reason == null) {
            reason = terminationReason.get();
        }
        if (reason == null) {
            reason = "UNKNOWN";
        }
        return reason;
    }

    /**
     * Cleanup all resources.
     */
    public void cleanup() {
        log.info("Starting load test cleanup...");
        try {
            phaseManager.cleanup();
            requestExecutor.cleanup();
            resourceManager.cleanup();
            log.info("Load test cleanup completed successfully");
        } catch (Exception e) {
            log.error("Unexpected error during cleanup", e);
        }
    }
}

class OpenWorkloadStrategy implements WorkloadStrategy {

    private static final Logger log = LoggerFactory.getLogger(OpenWorkloadStrategy.class);

    private final TestPhaseManager phaseManager;
    private final RequestExecutor requestExecutor;
    private final ResourceManager resourceManager;
    private final TimingUtils timingUtils;
    private final AtomicBoolean cancelled;

    private RateLimiter rateLimiter;
    private ScheduledFuture<?> warmupTask;
    private ScheduledFuture<?> durationTask;

    public OpenWorkloadStrategy(TestPhaseManager phaseManager, RequestExecutor requestExecutor,
                                ResourceManager resourceManager, AtomicBoolean cancelled) {
        this.phaseManager = phaseManager;
        this.requestExecutor = requestExecutor;
        this.resourceManager = resourceManager;
        this.timingUtils = new TimingUtils();
        this.cancelled = cancelled;
    }

    @Override
    public void execute(TestPlanSpec testPlanSpec) {
        try {
            var loadModel = testPlanSpec.getExecution().getLoadModel();

            var warmupDuration = timingUtils.parseDuration(loadModel.getWarmup());
            var testDuration = timingUtils.parseDuration(loadModel.getDuration());

            int arrivalRate = loadModel.getArrivalRatePerSec();
            int maxConcurrent = loadModel.getMaxConcurrent();

            log.info("Executing OPEN workload: {} req/sec, max {} concurrent, duration: {}s",
                    arrivalRate, maxConcurrent, testDuration.getSeconds());

            rateLimiter = RateLimiter.create(arrivalRate);

            // Phase 1: Warmup
            if (warmupDuration.toMillis() > 0 && !cancelled.get()) {
                executeWarmup(testPlanSpec, warmupDuration);
            }

            if (!phaseManager.isTestRunning() || cancelled.get()) return;

            // Phase 2: Main execution
            executeMainLoad(testPlanSpec, testDuration, maxConcurrent);
        } finally {
            cancelScheduledTasks();
        }
    }

    private void executeWarmup(TestPlanSpec testPlanSpec, Duration warmupDuration) {
        phaseManager.setPhase(TestPhaseManager.TestPhase.WARMUP);
        log.info("Starting warmup phase for {} seconds", warmupDuration.getSeconds());

        var warmupEnd = Instant.now().plus(warmupDuration);
        warmupTask = resourceManager.getSchedulerService().scheduleWithFixedDelay(() -> {
            if (!phaseManager.isTestRunning() || cancelled.get() || Instant.now().isAfter(warmupEnd)) {
                return;
            }
            try {
                requestExecutor.executeAllRequests(testPlanSpec, phaseManager, -1, true, null, cancelled);
            } catch (Exception e) {
                log.debug("Warmup request failed: {}", e.getMessage());
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);

        timingUtils.sleep(warmupDuration, cancelled);

        if (warmupTask != null) {
            warmupTask.cancel(false);
        }
        log.info("Warmup phase completed");
    }

    private void executeMainLoad(TestPlanSpec testPlanSpec, Duration testDuration, int maxConcurrent) {
        if (cancelled.get()) return;

        phaseManager.setPhase(TestPhaseManager.TestPhase.HOLD);
        var testEndTime = Instant.now().plus(testDuration);

        scheduleDurationTermination(testDuration);

        var concurrencyLimiter = new Semaphore(maxConcurrent);

        // Main request generation loop
        while (phaseManager.isTestRunning() && !cancelled.get() && Instant.now().isBefore(testEndTime)) {
            if (cancelled.get()) {
                log.debug("Request generation loop cancelled");
                break;
            }

            try {
                rateLimiter.acquire();
            } catch (Exception e) {
                log.debug("Rate limiter acquisition interrupted: {}", e.getMessage());
                break;
            }

            if (!phaseManager.isTestRunning() || cancelled.get() || Instant.now().isAfter(testEndTime)) {
                break;
            }

            if (concurrencyLimiter.tryAcquire()) {
                resourceManager.getMainExecutor().submit(() -> {
                    requestExecutor.executeAllRequests(testPlanSpec, phaseManager, -1, false, concurrencyLimiter, cancelled);
                });
            } else {
                requestExecutor.logBackPressureEvent(phaseManager.getCurrentPhase());
            }
        }

        // Wait for remaining requests
        waitForActiveRequestsToComplete(Duration.ofSeconds(30));

        if (phaseManager.isTestRunning() && !cancelled.get()) {
            // FIX: Use terminateTest to ensure reason is propagated
            phaseManager.terminateTest("DURATION_COMPLETED");
        }
    }


    private void scheduleDurationTermination(Duration testDuration) {
        durationTask = resourceManager.getSchedulerService().schedule(() -> {
            if (phaseManager.isTestRunning() && !cancelled.get()) {
                phaseManager.terminateTest("DURATION_COMPLETED");
            }
        }, testDuration.toMillis(), TimeUnit.MILLISECONDS);
    }


    private void waitForActiveRequestsToComplete(Duration timeout) {
        var deadline = Instant.now().plus(timeout);

        while (requestExecutor.getActiveRequestCount() > 0 && !cancelled.get() && Instant.now().isBefore(deadline)) {
            timingUtils.sleep(Duration.ofMillis(100), cancelled);
        }

        if (requestExecutor.getActiveRequestCount() > 0) {
            log.warn("Timeout waiting for {} active requests to complete", requestExecutor.getActiveRequestCount());
        }
    }

    private void cancelScheduledTasks() {
        if (warmupTask != null) {
            warmupTask.cancel(false);
        }
        if (durationTask != null) {
            durationTask.cancel(false);
        }
    }
}


