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
    private final SolacePublisher solacePublisher;

    private final AtomicReference<String> terminationReason = new AtomicReference<>();
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    public LoadTestExecutionRunner(TestPlanSpec testPlanSpec) {
        this.testPlanSpec = testPlanSpec;
        this.solacePublisher = new SolacePublisher();
        this.phaseManager = new TestPhaseManager(testPlanSpec.getTestSpec().getId(), solacePublisher);
        this.requestExecutor = new RequestExecutor(testPlanSpec, solacePublisher);
        this.resourceManager = new ResourceManager(testPlanSpec.getTestSpec().getId(), solacePublisher);
        this.strategyFactory = new WorkloadStrategyFactory();
        this.loadMetrics = new LoadMetrics(testPlanSpec, solacePublisher);
    }

    public CompletableFuture<Void> execute() {
        return CompletableFuture.runAsync(() -> {
            try {
                log.info("Starting load test execution: {}", testPlanSpec.getTestSpec().getId());

                // Publish test start event
                publishTestStartEvent();

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

                // Publish test completion event
                publishTestCompletionEvent(testStartTime, testEndTime, totalDuration);

                var testResult = createTestResult(testStartTime, testEndTime, totalDuration);
                solacePublisher.publishTestResult(testResult);

                phaseManager.completeTest();
                log.info("Load test completed. Duration: {}s, Reason: {}",
                        totalDuration.getSeconds(), getTerminationReason());

            } catch (Exception e) {
                log.error("Load test execution failed", e);
                publishCriticalEvent("EXECUTION_ERROR", "HIGH", "Load test execution failed: " + e.getMessage());
                terminateTest("EXECUTION_ERROR: " + e.getMessage());
                throw new RuntimeException("Load test execution failed", e);
            }
        }, resourceManager.getMainExecutor());
    }

    public void cancel() {
        if (cancelled.compareAndSet(false, true)) {
            log.info("Load test cancellation requested - stopping execution gracefully");
            publishCriticalEvent("CANCELLATION", "MEDIUM", "Load test cancelled by user request");
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
            publishCriticalEvent("CLEANUP_ERROR", "LOW", "Error during cleanup: " + e.getMessage());
        }
    }

    private void publishTestStartEvent() {
        var loadModel = testPlanSpec.getExecution().getLoadModel();
        var endpoints = testPlanSpec.getTestSpec().getScenarios().stream()
                .flatMap(s -> s.getRequests().stream())
                .map(TestPlanSpec.Request::getPath)
                .distinct()
                .toArray(String[]::new);

        int expectedTPS = switch (loadModel.getType()) {
            case OPEN -> loadModel.getArrivalRatePerSec();
            case CLOSED -> {
                // Rough estimate for closed model TPS
                int requestsPerIteration = testPlanSpec.getTestSpec().getScenarios().stream()
                        .mapToInt(s -> s.getRequests().size()).sum();
                yield (loadModel.getUsers() * requestsPerIteration) / 60; // rough estimate
            }
        };

        var event = new TestStartEvent(
                testPlanSpec.getTestSpec().getId(),
                loadModel.getType().toString(),
                loadModel.getUsers(),
                loadModel.getDuration() != null ? loadModel.getDuration() : loadModel.getHoldFor(),
                expectedTPS,
                endpoints,
                "load-test", // environment - could be configurable
                Instant.now()
        );

        solacePublisher.publishTestStart(event);
    }

    private void publishTestCompletionEvent(Instant startTime, Instant endTime, Duration duration) {
        var event = new TestCompletionEvent(
                testPlanSpec.getTestSpec().getId(),
                endTime,
                loadMetrics.getFinalSummary(),
                getTerminationReason(),
                duration.getSeconds()
        );

        solacePublisher.publishTestCompletion(event);
    }

    private void publishCriticalEvent(String eventType, String severity, String description) {
        var event = new CriticalEvent(
                testPlanSpec.getTestSpec().getId(),
                Instant.now(),
                eventType,
                severity,
                description,
                severity.equals("HIGH") ? "TEST_FAILURE" : "OPERATIONAL"
        );

        solacePublisher.publishCriticalEvent(event);
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



