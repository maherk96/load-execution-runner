package org.load.execution.runner.core.processor;

import org.load.execution.runner.JsonUtil;
import org.load.execution.runner.api.dto.TaskDto;
import org.load.execution.runner.core.model.TaskType;
import org.load.execution.runner.load.LoadTestExecutionRunner;
import org.load.execution.runner.load.TestPlanSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Enhanced processor for REST load test tasks using the improved LoadTestExecutionRunner.
 *
 * <p>This processor handles the complete lifecycle of load test tasks including:
 * <ul>
 *   <li>Task validation with detailed error reporting</li>
 *   <li>Load test execution with progress monitoring</li>
 *   <li>Graceful cancellation and cleanup</li>
 *   <li>Comprehensive error handling and recovery</li>
 * </ul>
 *
 * <p>The processor leverages the enhanced LoadTestExecutionRunner for better
 * reliability, observability, and error handling.
 *
 * @author Load Test Team
 * @version 2.0.0
 */
@Component
public class LoadHttpClientTaskProcessor implements InterruptibleTaskProcessor, ValidatableTaskProcessor {

    private static final Logger logger = LoggerFactory.getLogger(LoadHttpClientTaskProcessor.class);

    // Configuration constants
    private static final Duration DEFAULT_TASK_TIMEOUT = Duration.ofMinutes(30);
    private static final Duration TIMEOUT_BUFFER = Duration.ofSeconds(30);
    private static final long PROGRESS_LOG_INTERVAL_MS = 30000; // 30 seconds

    private volatile LoadTestExecutionRunner runner;
    private volatile CompletableFuture<LoadTestExecutionRunner.ExecutionResult> executionFuture;

    /**
     * Returns the supported TaskType for this processor.
     *
     * @return TaskType.REST_LOAD
     */
    @Override
    public TaskType getTaskType() {
        return TaskType.REST_LOAD;
    }

    /**
     * Processes a load test task with enhanced error handling and progress monitoring.
     *
     * @param task the task to process
     * @throws Exception if execution fails or is interrupted
     */
    @Override
    public void processTask(TaskDto task) throws Exception {
        logger.info("Starting LoadHttpClientTaskProcessor for task: {}", task.getTaskId());

        try {
            // Step 1: Validate task structure and content
            validateTask(task);

            // Step 2: Build test plan from task data
            TestPlanSpec testPlanSpec = buildTestPlanSpecFromTask(task);

            // Step 3: Create and configure runner
            LoadTestExecutionRunner.ExecutionConfig config = createExecutionConfig(testPlanSpec);
            runner = new LoadTestExecutionRunner(testPlanSpec, config);

            // Step 4: Execute load test with monitoring
            executionFuture = runner.execute();

            // Step 5: Monitor execution with progress logging
            LoadTestExecutionRunner.ExecutionResult result = monitorExecution(task, testPlanSpec);

            // Step 6: Handle results
            handleExecutionResult(task, result);

        } catch (InterruptedException e) {
            logger.info("Load test execution cancelled for task: {}", task.getTaskId());
            handleCancellation(task);
            Thread.currentThread().interrupt();
            throw e;

        } catch (TimeoutException e) {
            logger.error("Load test execution timed out for task: {}", task.getTaskId());
            handleTimeout(task);
            throw new Exception("Load test execution timed out", e);

        } catch (LoadTestExecutionRunner.ExecutionValidationException e) {
            logger.error("Load test validation failed for task: {} - {}", task.getTaskId(), e.getMessage());
            throw new IllegalArgumentException("Load test validation failed: " + e.getMessage(), e);

        } catch (Exception e) {
            logger.error("Load test execution failed for task: {} - {}", task.getTaskId(), e.getMessage(), e);
            handleExecutionError(task, e);
            throw e;

        } finally {
            cleanup();
        }
    }

    /**
     * Cancels the currently running load test task.
     *
     * @param task the task to cancel
     */
    @Override
    public void cancelTask(TaskDto task) {
        logger.info("Cancelling load test task: {}", task.getTaskId());

        if (runner != null) {
            boolean cancelled = runner.cancel();
            if (cancelled) {
                logger.info("Load test cancellation initiated for task: {}", task.getTaskId());
            } else {
                logger.warn("Load test cancellation was already in progress for task: {}", task.getTaskId());
            }
        }

        if (executionFuture != null && !executionFuture.isDone()) {
            executionFuture.cancel(true);
        }
    }

    /**
     * Validates the load test task with comprehensive checks.
     *
     * @param task the task to validate
     * @throws IllegalArgumentException if validation fails
     */
    @Override
    public void validateTask(TaskDto task) throws IllegalArgumentException {
        logger.debug("Validating load test task: {}", task.getTaskId());

        // Basic task structure validation
        if (task.getData() == null || task.getData().isEmpty()) {
            throw new IllegalArgumentException("Task data cannot be null or empty");
        }

        // Parse and validate test plan structure
        TestPlanSpec spec;
        try {
            spec = JsonUtil.convertValue(task.getData(), TestPlanSpec.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse task data as TestPlanSpec: " + e.getMessage(), e);
        }

        // Validate required sections
        validateTestPlanStructure(spec);

        // Validate load model configuration
        validateLoadModel(spec.getExecution().getLoadModel());

        logger.debug("Task validation completed successfully for task: {}", task.getTaskId());
    }

    // ================================
    // PRIVATE IMPLEMENTATION
    // ================================

    private void validateTestPlanStructure(TestPlanSpec spec) {
        if (spec == null) {
            throw new IllegalArgumentException("TestPlanSpec cannot be null");
        }
        if (spec.getTestSpec() == null) {
            throw new IllegalArgumentException("Missing testSpec section");
        }
        if (spec.getExecution() == null) {
            throw new IllegalArgumentException("Missing execution section");
        }
        if (spec.getExecution().getLoadModel() == null) {
            throw new IllegalArgumentException("Missing loadModel section");
        }
    }

    private void validateLoadModel(TestPlanSpec.LoadModel loadModel) {
        if (loadModel.getType() == null) {
            throw new IllegalArgumentException("loadModel.type must be specified (CLOSED or OPEN)");
        }

        switch (loadModel.getType()) {
            case CLOSED -> validateClosedModel(loadModel);
            case OPEN -> validateOpenModel(loadModel);
            default -> throw new IllegalArgumentException("Unsupported load model type: " + loadModel.getType());
        }
    }

    private void validateClosedModel(TestPlanSpec.LoadModel loadModel) {
        if (loadModel.getUsers() <= 0) {
            throw new IllegalArgumentException("CLOSED model must have users > 0, got: " + loadModel.getUsers());
        }
        if (loadModel.getIterations() <= 0) {
            throw new IllegalArgumentException("CLOSED model must have iterations > 0, got: " + loadModel.getIterations());
        }
        if (isEmpty(loadModel.getRampUp())) {
            throw new IllegalArgumentException("CLOSED model must specify rampUp duration (e.g. '5s', '2m')");
        }
        if (isEmpty(loadModel.getHoldFor())) {
            throw new IllegalArgumentException("CLOSED model must specify holdFor duration (e.g. '10s', '5m')");
        }

        // Validate duration formats
        validateDurationFormat("rampUp", loadModel.getRampUp());
        validateDurationFormat("holdFor", loadModel.getHoldFor());
    }

    private void validateOpenModel(TestPlanSpec.LoadModel loadModel) {
        if (loadModel.getArrivalRatePerSec() <= 0) {
            throw new IllegalArgumentException("OPEN model must have arrivalRatePerSec > 0, got: " + loadModel.getArrivalRatePerSec());
        }
        if (loadModel.getMaxConcurrent() <= 0) {
            throw new IllegalArgumentException("OPEN model must have maxConcurrent > 0, got: " + loadModel.getMaxConcurrent());
        }
        if (isEmpty(loadModel.getDuration())) {
            throw new IllegalArgumentException("OPEN model must specify duration (e.g. '30s', '5m')");
        }

        // Validate duration format
        validateDurationFormat("duration", loadModel.getDuration());

        // Validate reasonable configuration
        if (loadModel.getArrivalRatePerSec() > 1000) {
            logger.warn("High arrival rate ({} req/s) - ensure system can handle the load", loadModel.getArrivalRatePerSec());
        }
        if (loadModel.getMaxConcurrent() > 1000) {
            logger.warn("High concurrency limit ({}) - monitor resource usage", loadModel.getMaxConcurrent());
        }
    }

    private void validateDurationFormat(String field, String duration) {
        try {
            parseDurationToSeconds(duration);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Invalid %s duration format '%s'. Expected format: '30s', '5m', etc.", field, duration));
        }
    }

    private boolean isEmpty(String value) {
        return value == null || value.isBlank();
    }

    private LoadTestExecutionRunner.ExecutionConfig createExecutionConfig(TestPlanSpec testPlanSpec) {
        Duration calculatedTimeout = calculateExecutionTimeout(testPlanSpec);

        return new LoadTestExecutionRunner.ExecutionConfig(
                calculatedTimeout,           // execution timeout
                Duration.ofSeconds(30),      // cleanup timeout
                true,                        // enable progress tracking
                true,                        // enable detailed logging
                false                        // don't fail fast (we already validated)
        );
    }

    private Duration calculateExecutionTimeout(TestPlanSpec spec) {
        var loadModel = spec.getExecution().getLoadModel();

        try {
            Duration testDuration;

            if (loadModel.getType() == TestPlanSpec.WorkLoadModel.OPEN) {
                // For OPEN model, use duration + warmup
                testDuration = Duration.ofSeconds(parseDurationToSeconds(loadModel.getDuration()));
                if (!isEmpty(loadModel.getWarmup())) {
                    testDuration = testDuration.plus(Duration.ofSeconds(parseDurationToSeconds(loadModel.getWarmup())));
                }
            } else {
                // For CLOSED model, use rampUp + holdFor + warmup
                long rampUp = parseDurationToSeconds(loadModel.getRampUp());
                long holdFor = parseDurationToSeconds(loadModel.getHoldFor());
                testDuration = Duration.ofSeconds(rampUp + holdFor);

                if (!isEmpty(loadModel.getWarmup())) {
                    testDuration = testDuration.plus(Duration.ofSeconds(parseDurationToSeconds(loadModel.getWarmup())));
                }
            }

            // Add buffer time for setup, teardown, and safety margin
            Duration totalTimeout = testDuration.plus(TIMEOUT_BUFFER);

            // Ensure minimum timeout
            if (totalTimeout.compareTo(DEFAULT_TASK_TIMEOUT) < 0) {
                totalTimeout = DEFAULT_TASK_TIMEOUT;
            }

            logger.debug("Calculated execution timeout: {}s for test duration: {}s",
                    totalTimeout.getSeconds(), testDuration.getSeconds());

            return totalTimeout;

        } catch (Exception e) {
            logger.warn("Failed to calculate timeout from test plan, using default: {}s", DEFAULT_TASK_TIMEOUT.getSeconds());
            return DEFAULT_TASK_TIMEOUT;
        }
    }

    private LoadTestExecutionRunner.ExecutionResult monitorExecution(TaskDto task, TestPlanSpec testPlanSpec)
            throws Exception {

        Duration timeout = calculateExecutionTimeout(testPlanSpec);
        logger.info("Monitoring load test execution for task: {} (timeout: {}s)", task.getTaskId(), timeout.getSeconds());

        // Start progress monitoring in a separate thread
        CompletableFuture<Void> progressMonitor = startProgressMonitoring(task);

        try {
            // Wait for execution to complete with timeout
            LoadTestExecutionRunner.ExecutionResult result = executionFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

            // Stop progress monitoring
            progressMonitor.cancel(false);

            return result;

        } catch (TimeoutException e) {
            progressMonitor.cancel(false);
            if (runner != null) {
                runner.terminateTest("TASK_TIMEOUT");
            }
            throw e;
        } catch (Exception e) {
            progressMonitor.cancel(false);
            throw e;
        }
    }

    private CompletableFuture<Void> startProgressMonitoring(TaskDto task) {
        return CompletableFuture.runAsync(() -> {
            try {
                while (!Thread.currentThread().isInterrupted() && runner != null && runner.isRunning()) {
                    LoadTestExecutionRunner.ExecutionState state = runner.getExecutionState();

                    logger.info("Load test progress for task {}: {}", task.getTaskId(), state);

                    Thread.sleep(PROGRESS_LOG_INTERVAL_MS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.debug("Progress monitoring interrupted for task: {}", task.getTaskId());
            } catch (Exception e) {
                logger.debug("Error in progress monitoring for task {}: {}", task.getTaskId(), e.getMessage());
            }
        });
    }

    private void handleExecutionResult(TaskDto task, LoadTestExecutionRunner.ExecutionResult result) {
        if (result.isSuccessful()) {
            logger.info("Load test completed successfully for task: {} in {}s. Reason: {}",
                    task.getTaskId(), result.getTotalDuration().getSeconds(), result.getTerminationReason());

            // Log key metrics
            logExecutionMetrics(task, result);

        } else {
            logger.error("Load test failed for task: {} after {}s. Reason: {}",
                    task.getTaskId(), result.getTotalDuration().getSeconds(), result.getTerminationReason());

            throw new RuntimeException("Load test execution failed: " + result.getTerminationReason());
        }
    }

    private void logExecutionMetrics(TaskDto task, LoadTestExecutionRunner.ExecutionResult result) {
        try {
            var metrics = result.getMetrics();

            logger.info("Load test metrics for task {}: Total requests: {}, Success rate: {}%, " +
                            "Avg response time: {}ms, Active requests: {}",
                    task.getTaskId(),
                    metrics.getOrDefault("totalRequests", "N/A"),
                    String.format("%.1f", ((Number) metrics.getOrDefault("successRate", 0.0)).doubleValue() * 100),
                    metrics.getOrDefault("averageResponseTime", "N/A"),
                    metrics.getOrDefault("activeRequests", "N/A"));

        } catch (Exception e) {
            logger.debug("Error logging execution metrics for task {}: {}", task.getTaskId(), e.getMessage());
        }
    }

    private void handleCancellation(TaskDto task) {
        if (runner != null) {
            logger.info("Handling cancellation for task: {}, termination reason: {}",
                    task.getTaskId(), runner.getTerminationReason());
        }
    }

    private void handleTimeout(TaskDto task) {
        if (runner != null) {
            runner.terminateTest("TASK_PROCESSOR_TIMEOUT");
            logger.error("Load test timed out for task: {}, final reason: {}",
                    task.getTaskId(), runner.getTerminationReason());
        }
    }

    private void handleExecutionError(TaskDto task, Exception error) {
        if (runner != null) {
            String reason = "TASK_PROCESSOR_ERROR: " + error.getMessage();
            runner.terminateTest(reason);
            logger.error("Load test error for task: {}, final reason: {}",
                    task.getTaskId(), runner.getTerminationReason());
        }
    }

    private void cleanup() {
        try {
            if (runner != null) {
                logger.debug("Cleaning up LoadTestExecutionRunner");
                // The runner's cleanup is called automatically in its finally block
                runner = null;
            }

            if (executionFuture != null && !executionFuture.isDone()) {
                logger.debug("Cancelling execution future");
                executionFuture.cancel(false);
                executionFuture = null;
            }

        } catch (Exception e) {
            logger.warn("Error during processor cleanup: {}", e.getMessage());
        }
    }

    private TestPlanSpec buildTestPlanSpecFromTask(TaskDto task) {
        logger.debug("Mapping task data to TestPlanSpec for task: {}", task.getTaskId());

        try {
            return JsonUtil.convertValue(task.getData(), TestPlanSpec.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to build TestPlanSpec from task data: " + e.getMessage(), e);
        }
    }

    private long parseDurationToSeconds(String duration) {
        if (duration == null || duration.isBlank()) {
            throw new IllegalArgumentException("Duration cannot be null or empty");
        }

        String d = duration.trim().toLowerCase();

        try {
            if (d.endsWith("s")) {
                return Long.parseLong(d.substring(0, d.length() - 1));
            } else if (d.endsWith("m")) {
                return Long.parseLong(d.substring(0, d.length() - 1)) * 60;
            } else if (d.endsWith("h")) {
                return Long.parseLong(d.substring(0, d.length() - 1)) * 3600;
            } else {
                // Assume seconds if no unit specified
                return Long.parseLong(d);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid duration format: " + duration, e);
        }
    }
}