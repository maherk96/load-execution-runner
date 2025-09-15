package org.load.execution.runner.core.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.load.execution.runner.api.dto.TaskDto;
import org.load.execution.runner.core.model.TaskType;
import org.load.execution.runner.load.LoadTestExecutionRunner;
import org.load.execution.runner.load.TestPlanSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Component
public class LoadHttpClientTaskProcessor implements InterruptibleTaskProcessor, ValidatableTaskProcessor {

    private static final Logger logger = LoggerFactory.getLogger(LoadHttpClientTaskProcessor.class);

    private final ObjectMapper objectMapper = new ObjectMapper(); // Thread-safe, can be reused
    private volatile LoadTestExecutionRunner runner;

    @Override
    public TaskType getTaskType() {
        return TaskType.REST_LOAD;
    }

    @Override
    public void processTask(TaskDto task) throws Exception {
        logger.info("Starting LoadHttpClientTaskProcessor for task: {}", task.getTaskId());

        validateTask(task);
        TestPlanSpec testPlanSpec = buildTestPlanSpecFromTask(task);
        runner = new LoadTestExecutionRunner(testPlanSpec);
        CompletableFuture<Void> future = runner.execute();

        try {
            long timeout = calculateTimeoutSeconds(testPlanSpec);
            logger.info("Awaiting load test completion (timeout={}s)", timeout);
            future.get(timeout, TimeUnit.SECONDS);

            logger.info("Load test task {} finished. Termination reason: {}",
                    task.getTaskId(), runner.getTerminationReason());

        } catch (InterruptedException e) {
            // Handle cancellation gracefully - this is expected, not an error
            logger.info("Load test execution cancelled for task: {}", task.getTaskId());
            if (runner != null) {
                runner.terminateTest("CANCELLED");
            }
            Thread.currentThread().interrupt(); // Restore interrupt status
            throw e; // Re-throw so the queue service can handle it properly

        } catch (Exception e) {
            // Check if the exception is due to cancellation/interruption
            if (Thread.currentThread().isInterrupted()) {
                logger.info("Load test execution interrupted for task: {}", task.getTaskId());
                if (runner != null) {
                    runner.terminateTest("CANCELLED");
                }
                Thread.currentThread().interrupt();
                throw new InterruptedException("Load test was interrupted");
            }

            // This is an actual error - handle null message safely
            String errorMessage = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            logger.error("Load test execution failed for task: {} - Error: {}", task.getTaskId(), errorMessage, e);

            if (runner != null) {
                runner.terminateTest("TIMEOUT_OR_FAILURE");
            }
            throw e;

        } finally {
            // Always cleanup, regardless of success or failure
            if (runner != null) {
                runner.cleanup();
            }
        }
    }

    @Override
    public void cancelTask(TaskDto task) {
        logger.info("Cancelling load test task: {}", task.getTaskId());
        if (runner != null) {
            runner.cancel(); // Gracefully signals cancellation to runner & phases
        }
    }

    @Override
    public void validateTask(TaskDto task) throws IllegalArgumentException {
        logger.info("Validating load test task: {}", task.getTaskId());

        if (task.getData() == null || task.getData().isEmpty()) {
            throw new IllegalArgumentException("Task data cannot be null or empty");
        }

        // Convert first so we can validate strongly typed fields
        TestPlanSpec spec;
        try {
            spec = objectMapper.convertValue(task.getData(), TestPlanSpec.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to map task data to TestPlanSpec: " + e.getMessage(), e);
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

        TestPlanSpec.LoadModel loadModel = spec.getExecution().getLoadModel();

        // Validate load model type
        if (loadModel.getType() == null) {
            throw new IllegalArgumentException("loadModel.type must be specified (CLOSED or OPEN)");
        }

        switch (loadModel.getType()) {
            case CLOSED -> validateClosedModel(loadModel);
            case OPEN -> validateOpenModel(loadModel);
        }
    }

    private void validateClosedModel(TestPlanSpec.LoadModel loadModel) {
        if (loadModel.getUsers() <= 0) {
            throw new IllegalArgumentException("CLOSED model must have users > 0");
        }
        if (loadModel.getIterations() <= 0) {
            throw new IllegalArgumentException("CLOSED model must have iterations > 0");
        }
        if (isEmpty(loadModel.getRampUp())) {
            throw new IllegalArgumentException("CLOSED model must specify rampUp (e.g. '5s')");
        }
        if (isEmpty(loadModel.getHoldFor())) {
            throw new IllegalArgumentException("CLOSED model must specify holdFor (e.g. '10s')");
        }
    }

    private void validateOpenModel(TestPlanSpec.LoadModel loadModel) {
        if (loadModel.getArrivalRatePerSec() <= 0) {
            throw new IllegalArgumentException("OPEN model must have arrivalRatePerSec > 0");
        }
        if (loadModel.getMaxConcurrent() <= 0) {
            throw new IllegalArgumentException("OPEN model must have maxConcurrent > 0");
        }
        if (isEmpty(loadModel.getDuration())) {
            throw new IllegalArgumentException("OPEN model must specify duration (e.g. '30s')");
        }
    }

    private boolean isEmpty(String value) {
        return value == null || value.isBlank();
    }

    /**
     * Builds a TestPlanSpec object from TaskDto.data using Jackson.
     */
    private TestPlanSpec buildTestPlanSpecFromTask(TaskDto task) {
        logger.debug("Mapping task data to TestPlanSpec: {}", task.getData());
        return objectMapper.convertValue(task.getData(), TestPlanSpec.class);
    }

    /**
     * Optionally derive a timeout based on execution.duration or holdFor + rampUp.
     */
    private long calculateTimeoutSeconds(TestPlanSpec spec) {
        var loadModel = spec.getExecution().getLoadModel();

        try {
            if (loadModel.getDuration() != null) {
                return parseDurationToSeconds(loadModel.getDuration()) + 10; // 10s buffer
            }

            long rampUp = parseDurationToSeconds(loadModel.getRampUp());
            long holdFor = parseDurationToSeconds(loadModel.getHoldFor());
            return rampUp + holdFor + 10; // Buffer included
        } catch (Exception e) {
            logger.warn("Failed to parse durations, falling back to default timeout (120s)");
            return 120;
        }
    }

    private long parseDurationToSeconds(String duration) {
        if (duration == null) return 0;
        String d = duration.trim().toLowerCase();
        if (d.endsWith("s")) return Long.parseLong(d.replace("s", ""));
        if (d.endsWith("m")) return Long.parseLong(d.replace("m", "")) * 60;
        return Long.parseLong(d); // fallback to seconds
    }
}