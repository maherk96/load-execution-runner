package org.load.execution.runner.load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Enhanced Load Test Execution Runner that orchestrates load tests with comprehensive
 * error handling, progress tracking, and resource management.
 *
 * <p>This orchestrator coordinates the entire load test lifecycle while delegating
 * specific responsibilities to specialized components. It provides:
 * <ul>
 *   <li>Comprehensive error handling and recovery</li>
 *   <li>Progress tracking and execution metrics</li>
 *   <li>Graceful cancellation and cleanup</li>
 *   <li>Configurable component dependencies</li>
 *   <li>Detailed execution state management</li>
 * </ul>
 *
 * <p>The runner maintains a clear separation of concerns, focusing on orchestration
 * while delegating domain-specific logic to appropriate components.
 *
 * @author Load Test Team
 * @version 2.0.0
 */
public class LoadTestExecutionRunner {
    private static final Logger log = LoggerFactory.getLogger(LoadTestExecutionRunner.class);

    // Default timeouts
    private static final Duration DEFAULT_EXECUTION_TIMEOUT = Duration.ofHours(2);
    private static final Duration DEFAULT_CLEANUP_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Configuration for load test execution behavior.
     */
    public static class ExecutionConfig {
        private final Duration executionTimeout;
        private final Duration cleanupTimeout;
        private final boolean enableProgressTracking;
        private final boolean enableDetailedLogging;
        private final boolean failFastOnValidation;

        public ExecutionConfig(Duration executionTimeout, Duration cleanupTimeout,
                               boolean enableProgressTracking, boolean enableDetailedLogging,
                               boolean failFastOnValidation) {
            this.executionTimeout = executionTimeout != null ? executionTimeout : DEFAULT_EXECUTION_TIMEOUT;
            this.cleanupTimeout = cleanupTimeout != null ? cleanupTimeout : DEFAULT_CLEANUP_TIMEOUT;
            this.enableProgressTracking = enableProgressTracking;
            this.enableDetailedLogging = enableDetailedLogging;
            this.failFastOnValidation = failFastOnValidation;
        }

        public static ExecutionConfig defaultConfig() {
            return new ExecutionConfig(DEFAULT_EXECUTION_TIMEOUT, DEFAULT_CLEANUP_TIMEOUT, true, true, true);
        }

        // Getters
        public Duration getExecutionTimeout() { return executionTimeout; }
        public Duration getCleanupTimeout() { return cleanupTimeout; }
        public boolean isProgressTrackingEnabled() { return enableProgressTracking; }
        public boolean isDetailedLoggingEnabled() { return enableDetailedLogging; }
        public boolean isFailFastOnValidation() { return failFastOnValidation; }
    }

    /**
     * Current execution state and progress information.
     */
    public static class ExecutionState {
        private final TestPhaseManager.TestPhase currentPhase;
        private final boolean isRunning;
        private final boolean isCancelled;
        private final String terminationReason;
        private final Duration elapsed;
        private final Instant startTime;
        private final Map<String, Object> metrics;

        public ExecutionState(TestPhaseManager.TestPhase currentPhase, boolean isRunning, boolean isCancelled,
                              String terminationReason, Duration elapsed, Instant startTime, Map<String, Object> metrics) {
            this.currentPhase = currentPhase;
            this.isRunning = isRunning;
            this.isCancelled = isCancelled;
            this.terminationReason = terminationReason;
            this.elapsed = elapsed;
            this.startTime = startTime;
            this.metrics = Map.copyOf(metrics != null ? metrics : Map.of());
        }

        // Getters
        public TestPhaseManager.TestPhase getCurrentPhase() { return currentPhase; }
        public boolean isRunning() { return isRunning; }
        public boolean isCancelled() { return isCancelled; }
        public String getTerminationReason() { return terminationReason; }
        public Duration getElapsed() { return elapsed; }
        public Instant getStartTime() { return startTime; }
        public Map<String, Object> getMetrics() { return metrics; }

        @Override
        public String toString() {
            return String.format("ExecutionState{phase=%s, running=%s, cancelled=%s, elapsed=%ds, reason='%s'}",
                    currentPhase, isRunning, isCancelled, elapsed.getSeconds(), terminationReason);
        }
    }

    // Configuration and dependencies
    private final TestPlanSpec testPlanSpec;
    private final ExecutionConfig config;
    private final TestPhaseManager phaseManager;
    private final RequestExecutor requestExecutor;
    private final ResourceManager resourceManager;
    private final WorkloadStrategyFactory strategyFactory;

    // Execution state
    private final AtomicReference<String> terminationReason = new AtomicReference<>();
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private volatile Instant executionStartTime;
    private volatile WorkloadStrategyFactory.WorkloadStrategy currentStrategy;

    // ================================
    // CONSTRUCTORS
    // ================================

    /**
     * Constructs a new LoadTestExecutionRunner with default configuration.
     */
    public LoadTestExecutionRunner(TestPlanSpec testPlanSpec) {
        this(testPlanSpec, ExecutionConfig.defaultConfig());
    }

    /**
     * Constructs a new LoadTestExecutionRunner with custom configuration.
     */
    public LoadTestExecutionRunner(TestPlanSpec testPlanSpec, ExecutionConfig config) {
        this(testPlanSpec, config, null, null, null, null);
    }

    /**
     * Constructs a new LoadTestExecutionRunner with custom components.
     * Null components will be created with default configuration.
     */
    public LoadTestExecutionRunner(TestPlanSpec testPlanSpec, ExecutionConfig config,
                                   TestPhaseManager phaseManager, RequestExecutor requestExecutor,
                                   ResourceManager resourceManager, WorkloadStrategyFactory strategyFactory) {
        this.testPlanSpec = validateTestPlanSpec(testPlanSpec);
        this.config = config != null ? config : ExecutionConfig.defaultConfig();

        // Initialize components with dependency injection support
        this.phaseManager = phaseManager != null ? phaseManager : new TestPhaseManager();
        this.requestExecutor = requestExecutor != null ? requestExecutor : new RequestExecutor(testPlanSpec);
        this.resourceManager = resourceManager != null ? resourceManager : new ResourceManager();
        this.strategyFactory = strategyFactory != null ? strategyFactory : new WorkloadStrategyFactory();

        log.info("LoadTestExecutionRunner initialized for test: {}", getTestId());
    }

    // ================================
    // PUBLIC API
    // ================================

    /**
     * Executes the load test with comprehensive error handling and timeout management.
     */
    public CompletableFuture<ExecutionResult> execute() {
        return CompletableFuture.supplyAsync(() -> {
            executionStartTime = Instant.now();

            try {
                log.info("Starting load test execution: {}", getTestId());

                // Pre-execution validation
                if (config.isFailFastOnValidation()) {
                    validateExecution();
                }

                // Start test execution
                phaseManager.startTest();

                // Create and validate strategy
                currentStrategy = createAndValidateStrategy();

                // Execute the workload strategy
                executeStrategy();

                // Wait for completion with timeout
                waitForCompletion();

                // Calculate final metrics
                Duration totalDuration = Duration.between(executionStartTime, Instant.now());

                String finalReason = getTerminationReason();
                boolean success = isSuccessfulCompletion(finalReason);

                log.info("Load test completed. Duration: {}s, Reason: {}, Success: {}",
                        totalDuration.getSeconds(), finalReason, success);

                return new ExecutionResult(success, totalDuration, finalReason, gatherFinalMetrics());

            } catch (ExecutionTimeoutException e) {
                log.error("Load test execution timed out after {}s", config.getExecutionTimeout().getSeconds());
                terminateTest("EXECUTION_TIMEOUT");
                return new ExecutionResult(false, getDuration(), "EXECUTION_TIMEOUT", gatherFinalMetrics());

            } catch (ExecutionValidationException e) {
                log.error("Load test validation failed: {}", e.getMessage());
                terminateTest("VALIDATION_FAILED: " + e.getMessage());
                return new ExecutionResult(false, getDuration(), "VALIDATION_FAILED", gatherFinalMetrics());

            } catch (Exception e) {
                log.error("Load test execution failed", e);
                String reason = "EXECUTION_ERROR: " + e.getMessage();
                terminateTest(reason);
                return new ExecutionResult(false, getDuration(), reason, gatherFinalMetrics());

            } finally {
                // Always cleanup, regardless of outcome
                performCleanup();
            }
        }, resourceManager.getMainExecutor());
    }

    /**
     * Cancels the load test execution gracefully.
     */
    public boolean cancel() {
        if (cancelled.compareAndSet(false, true)) {
            log.info("Load test cancellation requested - stopping execution gracefully");
            terminateTest("CANCELLED");
            return true;
        } else {
            log.debug("Load test cancellation already in progress");
            return false;
        }
    }

    /**
     * Returns the current execution state.
     */
    public ExecutionState getExecutionState() {
        Duration elapsed = executionStartTime != null ?
                Duration.between(executionStartTime, Instant.now()) : Duration.ZERO;

        Map<String, Object> currentMetrics = gatherCurrentMetrics();

        return new ExecutionState(
                phaseManager.getCurrentPhase(),
                phaseManager.isTestRunning(),
                cancelled.get(),
                getTerminationReason(),
                elapsed,
                executionStartTime,
                currentMetrics
        );
    }

    /**
     * Returns true if the test has been cancelled.
     */
    public boolean isCancelled() {
        return cancelled.get();
    }

    /**
     * Returns true if the test is currently running.
     */
    public boolean isRunning() {
        return phaseManager.isTestRunning();
    }

    /**
     * Terminates the test with the specified reason.
     */
    public void terminateTest(String reason) {
        terminationReason.compareAndSet(null, reason);
        phaseManager.terminateTest(reason);
    }

    /**
     * Returns the termination reason.
     */
    public String getTerminationReason() {
        // Check phase manager first since strategies set the reason there
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
     * Returns the test ID for logging and tracking.
     */
    public String getTestId() {
        return testPlanSpec != null && testPlanSpec.getTestSpec() != null ?
                testPlanSpec.getTestSpec().getId() : "unknown";
    }

    // ================================
    // PRIVATE IMPLEMENTATION
    // ================================

    private TestPlanSpec validateTestPlanSpec(TestPlanSpec testPlanSpec) {
        if (testPlanSpec == null) {
            throw new IllegalArgumentException("TestPlanSpec cannot be null");
        }
        if (testPlanSpec.getTestSpec() == null) {
            throw new IllegalArgumentException("TestSpec cannot be null");
        }
        if (testPlanSpec.getExecution() == null) {
            throw new IllegalArgumentException("ExecutionSpec cannot be null");
        }
        if (testPlanSpec.getExecution().getLoadModel() == null) {
            throw new IllegalArgumentException("LoadModel cannot be null");
        }
        return testPlanSpec;
    }

    private void validateExecution() throws ExecutionValidationException {
        try {
            // Validate test plan spec structure (already done in constructor)

            // Validate load model
            var loadModel = testPlanSpec.getExecution().getLoadModel();
            if (loadModel.getType() == null) {
                throw new ExecutionValidationException("Load model type is required");
            }

            // Validate strategy can be created
            var testStrategy = strategyFactory.createStrategy(loadModel, phaseManager,
                    requestExecutor, resourceManager, cancelled);
            testStrategy.validate(testPlanSpec);
            testStrategy.cleanup(); // Clean up test instance

            log.debug("Pre-execution validation completed successfully");

        } catch (Exception e) {
            throw new ExecutionValidationException("Execution validation failed: " + e.getMessage(), e);
        }
    }

    private WorkloadStrategyFactory.WorkloadStrategy createAndValidateStrategy() throws ExecutionValidationException {
        try {
            var loadModel = testPlanSpec.getExecution().getLoadModel();
            var strategy = strategyFactory.createStrategy(loadModel, phaseManager,
                    requestExecutor, resourceManager, cancelled);

            // Validate strategy configuration
            strategy.validate(testPlanSpec);

            log.info("Created and validated {} strategy", loadModel.getType());
            return strategy;

        } catch (Exception e) {
            throw new ExecutionValidationException("Failed to create workload strategy: " + e.getMessage(), e);
        }
    }

    private void executeStrategy() {
        try {
            log.debug("Starting strategy execution");
            currentStrategy.execute(testPlanSpec);
            log.debug("Strategy execution completed");
        } catch (Exception e) {
            log.error("Strategy execution failed", e);
            throw new RuntimeException("Strategy execution failed", e);
        }
    }

    private void waitForCompletion() throws ExecutionTimeoutException {
        try {
            boolean completed = phaseManager.waitForCompletion(
                    config.getExecutionTimeout().toMillis(), TimeUnit.MILLISECONDS);

            if (!completed) {
                throw new ExecutionTimeoutException("Test execution exceeded timeout of " + config.getExecutionTimeout());
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Test execution wait interrupted");
            throw new RuntimeException("Test execution interrupted", e);
        }
    }

    private boolean isSuccessfulCompletion(String reason) {
        if (reason == null) return false;

        return reason.equals("COMPLETED") ||
                reason.equals("ALL_ITERATIONS_COMPLETED") ||
                reason.equals("DURATION_COMPLETED") ||
                reason.equals("HOLD_TIME_EXPIRED");
    }

    private Duration getDuration() {
        return executionStartTime != null ?
                Duration.between(executionStartTime, Instant.now()) : Duration.ZERO;
    }

    private Map<String, Object> gatherCurrentMetrics() {
        Map<String, Object> metrics = new java.util.HashMap<>();

        try {
            // Basic execution metrics
            metrics.put("phase", phaseManager.getCurrentPhase().name());
            metrics.put("running", phaseManager.isTestRunning());
            metrics.put("cancelled", cancelled.get());
            metrics.put("elapsedSeconds", getDuration().getSeconds());

            // Request executor metrics
            if (requestExecutor != null) {
                var requestMetrics = requestExecutor.getMetrics();
                metrics.put("totalRequests", requestMetrics.getTotalRequests());
                metrics.put("successfulRequests", requestMetrics.getSuccessfulRequests());
                metrics.put("failedRequests", requestMetrics.getFailedRequests());
                metrics.put("activeRequests", requestMetrics.getActiveRequests());
                metrics.put("successRate", requestMetrics.getSuccessRate());
                metrics.put("averageResponseTime", requestMetrics.getAverageResponseTime());
            }

            // Resource manager metrics
            if (resourceManager != null) {
                var resourceMetrics = resourceManager.getMetrics();
                metrics.put("resourceManagerHealthy", resourceMetrics.isHealthy());
                metrics.put("activeResourceTasks", resourceMetrics.getActiveTaskCount());
            }

            // Strategy-specific metrics
            if (currentStrategy != null) {
                if (currentStrategy instanceof ClosedWorkloadStrategy closedStrategy) {
                    var closedMetrics = closedStrategy.getMetrics();
                    metrics.put("activeUsers", closedMetrics.getActiveUsers());
                    metrics.put("completedUsers", closedMetrics.getCompletedUsers());
                    metrics.put("completedIterations", closedMetrics.getCompletedIterations());
                } else if (currentStrategy instanceof OpenWorkloadStrategy openStrategy) {
                    var openMetrics = openStrategy.getMetrics();
                    metrics.put("requestsGenerated", openMetrics.getRequestsGenerated());
                    metrics.put("backPressureEvents", openMetrics.getBackPressureEvents());
                    metrics.put("currentRate", openMetrics.getCurrentRate());
                }
            }

        } catch (Exception e) {
            log.debug("Error gathering metrics: {}", e.getMessage());
            metrics.put("metricsError", e.getMessage());
        }

        return metrics;
    }

    private Map<String, Object> gatherFinalMetrics() {
        Map<String, Object> metrics = gatherCurrentMetrics();

        // Add final execution summary
        metrics.put("finalPhase", phaseManager.getCurrentPhase().name());
        metrics.put("totalDurationSeconds", getDuration().getSeconds());
        metrics.put("terminationReason", getTerminationReason());
        metrics.put("executionSuccessful", isSuccessfulCompletion(getTerminationReason()));

        return metrics;
    }

    private void performCleanup() {
        log.info("Starting load test cleanup...");

        try {
            // Cleanup in reverse dependency order
            if (currentStrategy != null) {
                try {
                    currentStrategy.cleanup();
                    log.debug("Strategy cleanup completed");
                } catch (Exception e) {
                    log.warn("Error during strategy cleanup: {}", e.getMessage());
                }
            }

            if (phaseManager != null) {
                try {
                    phaseManager.cleanup();
                    log.debug("Phase manager cleanup completed");
                } catch (Exception e) {
                    log.warn("Error during phase manager cleanup: {}", e.getMessage());
                }
            }

            if (requestExecutor != null) {
                try {
                    requestExecutor.cleanup();
                    log.debug("Request executor cleanup completed");
                } catch (Exception e) {
                    log.warn("Error during request executor cleanup: {}", e.getMessage());
                }
            }

            if (resourceManager != null) {
                try {
                    resourceManager.cleanup();
                    log.debug("Resource manager cleanup completed");
                } catch (Exception e) {
                    log.warn("Error during resource manager cleanup: {}", e.getMessage());
                }
            }

            if (strategyFactory != null) {
                try {
                    strategyFactory.cleanup();
                    log.debug("Strategy factory cleanup completed");
                } catch (Exception e) {
                    log.warn("Error during strategy factory cleanup: {}", e.getMessage());
                }
            }

            log.info("Load test cleanup completed successfully");

        } catch (Exception e) {
            log.error("Unexpected error during cleanup", e);
        }
    }

    // ================================
    // RESULT CLASSES
    // ================================

    /**
     * Result of load test execution.
     */
    public static class ExecutionResult {
        private final boolean successful;
        private final Duration totalDuration;
        private final String terminationReason;
        private final Map<String, Object> metrics;
        private final Instant completedAt;

        public ExecutionResult(boolean successful, Duration totalDuration, String terminationReason, Map<String, Object> metrics) {
            this.successful = successful;
            this.totalDuration = totalDuration;
            this.terminationReason = terminationReason;
            this.metrics = Map.copyOf(metrics != null ? metrics : Map.of());
            this.completedAt = Instant.now();
        }

        public boolean isSuccessful() { return successful; }
        public Duration getTotalDuration() { return totalDuration; }
        public String getTerminationReason() { return terminationReason; }
        public Map<String, Object> getMetrics() { return metrics; }
        public Instant getCompletedAt() { return completedAt; }

        @Override
        public String toString() {
            return String.format("ExecutionResult{successful=%s, duration=%ds, reason='%s', metrics=%d}",
                    successful, totalDuration.getSeconds(), terminationReason, metrics.size());
        }
    }

    // ================================
    // CUSTOM EXCEPTIONS
    // ================================

    /**
     * Exception thrown when execution validation fails.
     */
    public static class ExecutionValidationException extends Exception {
        public ExecutionValidationException(String message) {
            super(message);
        }

        public ExecutionValidationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Exception thrown when execution times out.
     */
    public static class ExecutionTimeoutException extends Exception {
        public ExecutionTimeoutException(String message) {
            super(message);
        }
    }
}