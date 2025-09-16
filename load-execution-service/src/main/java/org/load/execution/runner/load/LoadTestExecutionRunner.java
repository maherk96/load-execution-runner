// ===== MAIN ORCHESTRATOR =====
// File: org/load/execution/runner/load/LoadTestExecutionRunner.java
package org.load.execution.runner.load;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class LoadTestExecutionRunner {

    private static final Logger log = LoggerFactory.getLogger(LoadTestExecutionRunner.class);

    private final TestPlanSpec testPlanSpec;
    private final TestPhaseManager phaseManager;
    private final RequestExecutor requestExecutor;
    private final ResourceManager resourceManager;
    private final WorkloadStrategyFactory strategyFactory;
    private final LoadMetrics loadMetrics;

    private final AtomicReference<String> terminationReason = new AtomicReference<>();
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    public LoadTestExecutionRunner(TestPlanSpec testPlanSpec) {
        this.testPlanSpec = testPlanSpec;
        this.phaseManager = new TestPhaseManager();
        this.requestExecutor = new RequestExecutor(testPlanSpec);
        this.resourceManager = new ResourceManager();
        this.strategyFactory = new WorkloadStrategyFactory();
        this.loadMetrics = new LoadMetrics(testPlanSpec);
    }

    public CompletableFuture<Void> execute() {
        return CompletableFuture.runAsync(() -> {
            try {
                log.info("Starting load test execution: {}", testPlanSpec.getTestSpec().getId());

                phaseManager.startTest();
                loadMetrics.startTest();
                var testStartTime = Instant.now();

                var strategy = strategyFactory.createStrategy(
                        testPlanSpec.getExecution().getLoadModel(),
                        phaseManager,
                        requestExecutor,
                        resourceManager,
                        loadMetrics,
                        cancelled
                );

                strategy.execute(testPlanSpec);
                phaseManager.waitForCompletion();

                var testEndTime = Instant.now();
                var totalDuration = Duration.between(testStartTime, testEndTime);

                loadMetrics.completeTest();
                
                var testResult = createTestResult(testStartTime, testEndTime, totalDuration);
                log.info("Publishing test result: {}", testResult);

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

    public void cancel() {
        if (cancelled.compareAndSet(false, true)) {
            log.info("Load test cancellation requested - stopping execution gracefully");
            terminateTest("CANCELLED");
        } else {
            log.debug("Load test cancellation already in progress");
        }
    }

    public boolean isCancelled() {
        return cancelled.get();
    }

    public void terminateTest(String reason) {
        terminationReason.compareAndSet(null, reason);
        phaseManager.terminateTest(reason);
    }

    public String getTerminationReason() {
        String reason = phaseManager.getTerminationReason();
        if (reason == null) {
            reason = terminationReason.get();
        }
        if (reason == null) {
            reason = "UNKNOWN";
        }
        return reason;
    }

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

    private TestResult createTestResult(Instant startTime, Instant endTime, Duration duration) {
        return new TestResult(
                testPlanSpec.getTestSpec().getId(),
                startTime,
                endTime,
                duration,
                getTerminationReason(),
                loadMetrics.getFinalSummary()
        );
    }

    public record TestResult(
            String testId,
            Instant startTime,
            Instant endTime,
            Duration duration,
            String terminationReason,
            String metricsSummary
    ) {}
}



