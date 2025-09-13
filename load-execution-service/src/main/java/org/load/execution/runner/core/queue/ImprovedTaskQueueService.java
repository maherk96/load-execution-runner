package org.load.execution.runner.core.queue;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.load.execution.runner.api.dto.TaskDto;
import org.load.execution.runner.api.dto.TaskExecution;
import org.load.execution.runner.api.dto.TaskResponseDto;
import org.load.execution.runner.api.exception.RejectedTaskException;
import org.load.execution.runner.config.TaskQueueConfig;
import org.load.execution.runner.core.history.TaskHistoryService;
import org.load.execution.runner.core.model.ServiceState;
import org.load.execution.runner.core.model.TaskStatus;
import org.load.execution.runner.core.model.TaskType;
import org.load.execution.runner.core.processor.InterruptibleTaskProcessor;
import org.load.execution.runner.core.processor.TaskProcessor;
import org.load.execution.runner.core.processor.TaskProcessorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Service
public class ImprovedTaskQueueService {
    private static final Logger logger = LoggerFactory.getLogger(ImprovedTaskQueueService.class);

    private final TaskValidator validator;
    private final TaskHistoryService historyService;
    private final TaskProcessorManager processorManager;
    private final TaskQueueConfig config;

    // Core executors - shared thread pools for better performance
    private final BlockingQueue<TaskWrapper> taskQueue = new LinkedBlockingQueue<>();
    private final ExecutorService queueProcessor;
    private final ExecutorService taskRunnerPool; // Shared pool instead of per-task executors
    private final ScheduledExecutorService maintenanceExecutor;

    // State tracking
    private final AtomicReference<TaskWrapper> currentTask = new AtomicReference<>();
    private final AtomicInteger completedTasks = new AtomicInteger(0);
    private final AtomicInteger failedTasks = new AtomicInteger(0);
    private final AtomicLong totalProcessingTimeMs = new AtomicLong(0);
    private volatile ServiceState serviceState = ServiceState.STARTING;
    private volatile LocalDateTime serviceStartTime;

    public ImprovedTaskQueueService(TaskValidator validator, TaskHistoryService historyService,
                                    TaskProcessorManager processorManager, TaskQueueConfig config) {
        this.validator = validator;
        this.historyService = historyService;
        this.processorManager = processorManager;
        this.config = config;

        // Single-threaded queue processor for sequential task handling
        this.queueProcessor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "TaskQueueProcessor");
            thread.setDaemon(false);
            thread.setUncaughtExceptionHandler(this::handleUncaughtException);
            return thread;
        });

        this.taskRunnerPool = Executors.newFixedThreadPool(config.getMaxConcurrentTasks(), runnable -> {
            Thread thread = new Thread(runnable, "TaskRunner");
            thread.setDaemon(false);
            thread.setUncaughtExceptionHandler(this::handleUncaughtException);
            return thread;
        });

        this.maintenanceExecutor = Executors.newScheduledThreadPool(1, runnable -> {
            Thread thread = new Thread(runnable, "TaskQueueMaintenance");
            thread.setDaemon(true);
            return thread;
        });
    }

    @PostConstruct
    public void initialize() {
        serviceStartTime = LocalDateTime.now();
        serviceState = ServiceState.RUNNING;
        logger.info("TaskQueueService initializing...");

        startProcessing();
        startMaintenanceTasks();

        logger.info("Config loaded {}",config);
        logger.info("TaskQueueService initialized - State: {}, Max Concurrent Tasks: {}",
                serviceState, config.getMaxConcurrentTasks());
    }

    // ===============================
    // PUBLIC API
    // ===============================

    public TaskResponseDto submitTask(TaskDto task) {
        String taskId = task != null ? task.getTaskId() : "unknown";

        try {
            // MDC for better log traceability
            MDC.put("taskId", taskId);
            logger.debug("Received task submission request");

            // Fast-fail if shutting down
            if (serviceState != ServiceState.RUNNING) {
                logger.warn("Task submission rejected - service state: {}", serviceState);
                return createErrorResponse(taskId, "Service is not accepting new tasks");
            }

            validator.validateServiceState();
            validator.validateTask(task);
            validator.checkDuplicate(task);
            validator.validateBusinessRules(task, taskQueue.size());

            return queueTask(task);

        } catch (RejectedTaskException e) {
            logger.warn("Task submission rejected: {}", e.getMessage());
            return createErrorResponse(taskId, e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected error during task submission", e);
            return createErrorResponse(taskId, "Internal error during task submission: " + e.getMessage());
        } finally {
            MDC.remove("taskId");
        }
    }

    public TaskResponseDto cancelTask(String taskId) {
        if (taskId == null || taskId.trim().isEmpty()) {
            return createErrorResponse("invalid-id", "Task ID is required");
        }

        try {
            MDC.put("taskId", taskId);
            logger.info("Processing cancel request");

            if (!historyService.isTaskActive(taskId) && historyService.getTaskExecution(taskId) == null) {
                logger.warn("Cancel request for unknown task");
                return createErrorResponse(taskId, "Task not found");
            }

            TaskExecution execution = historyService.getTaskExecution(taskId);
            if (execution != null && isTaskFinished(execution.getStatus())) {
                logger.info("Cancel request for already finished task (status: {})", execution.getStatus());
                return new TaskResponseDto(taskId, execution.getStatus().name(),
                        "Task is already " + execution.getStatus().name().toLowerCase());
            }

            // Try to cancel currently processing task
            TaskWrapper currentWrapper = currentTask.get();
            if (currentWrapper != null && currentWrapper.getTask().getTaskId().equals(taskId)) {
                logger.info("Cancelling currently processing task");
                boolean cancelled = currentWrapper.cancel();

                // Notify processor if it supports cancellation
                notifyProcessorOfCancellation(currentWrapper.getTask());

                if (cancelled) {
                    historyService.markCancelled(taskId, 0, "Task cancelled by user request");
                    return new TaskResponseDto(taskId, "CANCELLED", "Task was cancelled successfully");
                } else {
                    return createErrorResponse(taskId, "Failed to cancel running task");
                }
            }

            // Try to cancel queued task
            boolean removedFromQueue = removeFromQueue(taskId);
            if (removedFromQueue) {
                historyService.markCancelled(taskId, 0, "Task cancelled by user request before processing");
                logger.info("Successfully cancelled queued task");
                return new TaskResponseDto(taskId, "CANCELLED", "Queued task was cancelled successfully");
            }

            logger.warn("Task could not be cancelled");
            return createErrorResponse(taskId, "Task could not be cancelled");

        } finally {
            MDC.remove("taskId");
        }
    }

    public TaskResponseDto getTaskStatus(String taskId) {
        if (taskId == null || taskId.trim().isEmpty()) {
            return createErrorResponse("invalid-id", "Task ID is required");
        }

        TaskExecution execution = historyService.getTaskExecution(taskId);
        if (execution != null) {
            return buildStatusResponse(taskId, execution);
        }

        return new TaskResponseDto(taskId, "NOT_FOUND", "Task not found");
    }

    public TaskExecution getDetailedTaskStatus(String taskId) {
        return historyService.getTaskExecution(taskId);
    }

    public List<TaskExecution> getTaskHistory(int limit) {
        return historyService.getTaskHistory(limit);
    }

    public List<TaskExecution> getTasksByStatus(TaskStatus status, int limit) {
        return historyService.getTasksByStatus(status, limit);
    }

    public Map<String, Object> getServiceStatus() {
        TaskWrapper current = currentTask.get();
        long uptimeHours = ChronoUnit.HOURS.between(serviceStartTime, LocalDateTime.now());

        Map<String, Object> status = new HashMap<>();

        status.put("serviceName", "TaskQueueService");
        status.put("version", "3.0.0");
        status.put("serviceState", serviceState.name());
        status.put("uptimeHours", uptimeHours);
        status.put("startTime", serviceStartTime);

        status.put("queueSize", taskQueue.size());
        status.put("isProcessing", current != null);
        status.put("currentTask", current != null ? Map.of(
                "taskId", current.getTask().getTaskId(),
                "taskType", current.getTask().getTaskType().name(),
                "startedAt", current.getQueuedAt(),
                "processingTimeMs", ChronoUnit.MILLIS.between(current.getQueuedAt(), LocalDateTime.now())
        ) : null);

        int completed = completedTasks.get();
        int failed = failedTasks.get();
        long totalProcessingTime = totalProcessingTimeMs.get();

        status.put("statistics", Map.of(
                "totalCompleted", completed,
                "totalFailed", failed,
                "totalProcessed", completed + failed,
                "successRate", completed + failed > 0 ?
                        String.format("%.2f%%", (100.0 * completed) / (completed + failed)) : "N/A",
                "averageProcessingTimeMs", completed > 0 ? totalProcessingTime / completed : 0,
                "statusBreakdown", historyService.getStatusBreakdown()
        ));

        status.put("availableTaskTypes", processorManager.getAvailableTaskTypes().stream()
                .map(Enum::name)
                .sorted()
                .collect(Collectors.toList()));

        status.put("configuration", Map.of(
                "maxQueueSize", config.getMaxQueueSize(),
                "maxConcurrentTasks", config.getMaxConcurrentTasks(),
                "taskTimeoutMinutes", config.getTaskTimeoutMinutes()
        ));

        status.put("activeTasks", historyService.getActiveTaskCount());
        status.put("taskHistorySize", historyService.getHistorySize());

        return status;
    }

    public Set<TaskType> getAvailableTaskTypes() {
        return processorManager.getAvailableTaskTypes();
    }

    public int getQueueSize() {
        return taskQueue.size();
    }

    public boolean isCurrentlyProcessing() {
        return currentTask.get() != null;
    }

    public String getCurrentTaskId() {
        TaskWrapper current = currentTask.get();
        return current != null ? current.getTask().getTaskId() : null;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    // ===============================
    // PRIVATE IMPLEMENTATION
    // ===============================

    /**
     * Simplified queue task method - just enqueue and delegate to history service
     */
    private TaskResponseDto queueTask(TaskDto task) {
        String taskId = task.getTaskId();

        // Calculate position BEFORE adding to queue
        int currentPosition = taskQueue.size() + 1;

        TaskWrapper wrapper = new TaskWrapper(task);
        taskQueue.offer(wrapper);

        // Delegate history tracking to service
        historyService.recordQueued(task, currentPosition);

        logger.info("Task queued - Queue size: {}, Position: {}", taskQueue.size(), currentPosition);

        if (currentPosition == 1 && currentTask.get() == null) {
            return new TaskResponseDto(taskId, "PROCESSING", "Task is being processed");
        } else {
            return new TaskResponseDto(taskId, "QUEUED", currentPosition,
                    String.format("Task queued at position %d", currentPosition));
        }
    }
    private void startProcessing() {
        queueProcessor.submit(this::processLoop);
    }

    private void processLoop() {
        logger.info("Queue processor started");

        while (!Thread.currentThread().isInterrupted() && serviceState == ServiceState.RUNNING) {
            try {
                TaskWrapper wrapper = taskQueue.take();
                currentTask.set(wrapper);
                processTaskSafely(wrapper);

            } catch (InterruptedException e) {
                logger.info("Queue processor interrupted - shutting down");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in queue processor", e);
            }
        }

        logger.info("Queue processor stopped");
    }

    private void processTaskSafely(TaskWrapper wrapper) {
        TaskDto task = wrapper.getTask();
        String taskId = task.getTaskId();
        long startTime = System.currentTimeMillis();
        boolean taskStatusSet = false; // Track if status was already set

        try {
            MDC.put("taskId", taskId);

            // Check for pre-processing cancellation
            if (wrapper.isCancelled()) {
                logger.info("Task was cancelled before processing started");
                //historyService.markCancelled(taskId, 0, "Task cancelled before processing");
                return; // Exit early, don't continue processing
            }

            logger.info("STARTED processing task - Type: {}, Queue size: {}",
                    task.getTaskType(), taskQueue.size());

            historyService.markProcessing(taskId);

            // Submit to shared thread pool instead of creating per-task executor
            Future<Void> executionFuture = taskRunnerPool.submit(() -> {
                try {
                    MDC.put("taskId", taskId);
                    executeTaskWithInterruptChecks(task, wrapper);
                    return null;
                } finally {
                    MDC.remove("taskId");
                }
            });

            wrapper.setExecutionFuture(executionFuture);

            // Wait for completion with timeout
            executionFuture.get(config.getTaskTimeoutMinutes(), TimeUnit.MINUTES);

            // Final cancellation check
            if (wrapper.isCancelled()) {
                throw new InterruptedException("Task was cancelled during execution");
            }

            // Success
            long duration = System.currentTimeMillis() - startTime;
            completedTasks.incrementAndGet();
            totalProcessingTimeMs.addAndGet(duration);
            historyService.markCompleted(taskId, duration);
            taskStatusSet = true;

            logger.info("COMPLETED task in {}ms - Total completed: {}", duration, completedTasks.get());

        } catch (InterruptedException e) {
            if (!taskStatusSet) {
                long duration = System.currentTimeMillis() - startTime;
                historyService.markCancelled(taskId, duration, "Task was cancelled");
                failedTasks.incrementAndGet();
                taskStatusSet = true;
                logger.warn("Task CANCELLED after {}ms", duration);
            }
            // Don't re-interrupt here as we're handling it appropriately

        } catch (CancellationException e) {
            // Handle CancellationException specifically as a cancellation, not a failure
            if (!taskStatusSet) {
                long duration = System.currentTimeMillis() - startTime;
                historyService.markCancelled(taskId, duration, "Task was cancelled");
                failedTasks.incrementAndGet();
                taskStatusSet = true;
                logger.warn("Task CANCELLED after {}ms (CancellationException)", duration);
            }

        } catch (TimeoutException e) {
            if (!taskStatusSet) {
                long duration = System.currentTimeMillis() - startTime;
                wrapper.cancel(); // Cancel the execution
                historyService.markFailed(taskId, duration, "Task timeout after " + config.getTaskTimeoutMinutes() + " minutes");
                failedTasks.incrementAndGet();
                taskStatusSet = true;
                logger.error("Task TIMEOUT after {}ms", duration);
            }

        } catch (Exception e) {
            // Only mark as failed if status hasn't been set yet
            if (!taskStatusSet) {
                long duration = System.currentTimeMillis() - startTime;
                failedTasks.incrementAndGet();

                // Ensure error message is never null
                String errorMessage = e.getMessage();
                if (errorMessage == null) {
                    errorMessage = "Unknown error: " + e.getClass().getSimpleName();
                }

                historyService.markFailed(taskId, duration, errorMessage);
                taskStatusSet = true;
                logger.error("Task FAILED after {}ms - Error: {}", duration, errorMessage, e);
            } else {
                // Task status already set, just log the cleanup exception
                logger.debug("Ignoring cleanup exception - task status already set: {}",
                        e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            }

        } finally {
            currentTask.set(null);
            MDC.remove("taskId");
        }
    }

    /**
     * Execute task with periodic interrupt checks for better cancellation support
     */
    private void executeTaskWithInterruptChecks(TaskDto task, TaskWrapper wrapper) throws Exception {
        // Check interruption before starting
        if (Thread.currentThread().isInterrupted() || wrapper.isCancelled()) {
            throw new InterruptedException("Task was cancelled");
        }

        try {
            processorManager.processTask(task);
        } catch (InterruptedException e) {
            logger.info("Task processor properly handled interruption");
            throw e;
        } catch (Exception e) {
            // Check if this was due to interruption
            if (Thread.currentThread().isInterrupted()) {
                logger.info("Task failed due to interruption");
                throw new InterruptedException("Task was interrupted");
            }
            throw e;
        }

        // Final check after processing
        if (Thread.currentThread().isInterrupted() || wrapper.isCancelled()) {
            throw new InterruptedException("Task was cancelled after processing");
        }
    }

    private void notifyProcessorOfCancellation(TaskDto task) {
        try {
            TaskProcessor processor = processorManager.getProcessor(task.getTaskType());
            if (processor instanceof InterruptibleTaskProcessor) {
                ((InterruptibleTaskProcessor) processor).cancelTask(task);
            }
        } catch (Exception e) {
            logger.warn("Error notifying processor of cancellation: {}", e.getMessage());
        }
    }

    private boolean removeFromQueue(String taskId) {
        Iterator<TaskWrapper> iterator = taskQueue.iterator();
        while (iterator.hasNext()) {
            TaskWrapper wrapper = iterator.next();
            if (wrapper.getTask().getTaskId().equals(taskId)) {
                wrapper.cancel();
                iterator.remove();
                logger.info("Removed queued task from queue");
                return true;
            }
        }
        return false;
    }

    /**
     * Dynamic queue position calculation - computed when needed instead of at submission
     */
    private int calculateCurrentQueuePosition(String taskId) {
        int position = 1;

        // Check if currently processing
        TaskWrapper current = currentTask.get();
        if (current != null && current.getTask().getTaskId().equals(taskId)) {
            return 0; // Currently processing
        }

        // Search in queue
        for (TaskWrapper wrapper : taskQueue) {
            if (wrapper.getTask().getTaskId().equals(taskId)) {
                return position;
            }
            position++;
        }

        return -1; // Not found
    }

    private TaskResponseDto buildStatusResponse(String taskId, TaskExecution execution) {
        TaskResponseDto response = new TaskResponseDto(taskId, execution.getStatus().name(),
                execution.getQueuePosition(),
                String.format("Task %s", execution.getStatus().name().toLowerCase()));

        switch (execution.getStatus()) {
            case QUEUED:
                int currentPosition = calculateCurrentQueuePosition(taskId);
                if (currentPosition > 0) {
                    response = new TaskResponseDto(taskId, "QUEUED", currentPosition,
                            String.format("Task queued at position %d", currentPosition));
                } else if (currentPosition == 0) {
                    response = new TaskResponseDto(taskId, "PROCESSING", "Task is currently processing");
                }
                break;
            case PROCESSING:
                long processingTimeMs = execution.getStartedAt() != null ?
                        ChronoUnit.MILLIS.between(execution.getStartedAt(), LocalDateTime.now()) : 0;
                response.setMessage(String.format("Task processing for %d seconds", processingTimeMs / 1000));
                break;
            case COMPLETED:
                response.setMessage(String.format("Task completed in %d ms", execution.getProcessingTimeMs()));
                break;
            case FAILED:
            case TIMEOUT:
                response.setMessage(String.format("Task %s: %s",
                        execution.getStatus().name().toLowerCase(),
                        execution.getErrorMessage() != null ? execution.getErrorMessage() : "Unknown error"));
                break;
            case CANCELLED:
                response.setMessage(String.format("Task cancelled: %s",
                        execution.getErrorMessage() != null ? execution.getErrorMessage() : "Cancelled by user"));
                break;
        }

        return response;
    }

    private boolean isTaskFinished(TaskStatus status) {
        return status == TaskStatus.COMPLETED ||
                status == TaskStatus.FAILED ||
                status == TaskStatus.CANCELLED ||
                status == TaskStatus.TIMEOUT;
    }

    private TaskResponseDto createErrorResponse(String taskId, String message) {
        return new TaskResponseDto(taskId, "ERROR", message);
    }

    private void handleUncaughtException(Thread thread, Throwable exception) {
        logger.error("Uncaught exception in thread {}: {}", thread.getName(), exception.getMessage(), exception);
    }

    private void startMaintenanceTasks() {
        maintenanceExecutor.scheduleAtFixedRate(this::logQueueStatus, 5, 5, TimeUnit.MINUTES);
    }

    private void logQueueStatus() {
        int queueSize = taskQueue.size();
        TaskWrapper current = currentTask.get();

        if (queueSize > 0 || current != null) {
            logger.info("Queue status - Size: {}, Processing: {}, Service state: {}",
                    queueSize, current != null ? current.getTask().getTaskId() : "none", serviceState);
        }
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Initiating TaskQueueService shutdown...");
        serviceState = ServiceState.SHUTTING_DOWN;
        validator.setShuttingDown(true);

        // Optionally drain queue and cancel remaining tasks
        if (config.isDrainQueueOnShutdown()) {
            drainQueueAndCancelTasks();
        }

        // Shutdown executors in order
        maintenanceExecutor.shutdown();
        queueProcessor.shutdown();
        taskRunnerPool.shutdown();

        try {
            // Wait for queue processor
            if (!queueProcessor.awaitTermination(config.getShutdownTimeoutSeconds(), TimeUnit.SECONDS)) {
                logger.warn("Queue processor did not terminate within {} seconds, forcing shutdown",
                        config.getShutdownTimeoutSeconds());
                queueProcessor.shutdownNow();
            }

            // Wait for task runner pool
            if (!taskRunnerPool.awaitTermination(config.getShutdownTimeoutSeconds(), TimeUnit.SECONDS)) {
                logger.warn("Task runner pool did not terminate within {} seconds, forcing shutdown",
                        config.getShutdownTimeoutSeconds());
                taskRunnerPool.shutdownNow();
            }

            // Wait for maintenance
            if (!maintenanceExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                maintenanceExecutor.shutdownNow();
            }

            serviceState = ServiceState.SHUTDOWN;
            logger.info("TaskQueueService shutdown completed. Final statistics: completed={}, failed={}",
                    completedTasks.get(), failedTasks.get());

        } catch (InterruptedException e) {
            logger.warn("Shutdown interrupted, forcing immediate shutdown");
            queueProcessor.shutdownNow();
            taskRunnerPool.shutdownNow();
            maintenanceExecutor.shutdownNow();
            serviceState = ServiceState.SHUTDOWN;
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Drain queue and mark remaining tasks as cancelled during shutdown
     */
    private void drainQueueAndCancelTasks() {
        logger.info("Draining queue and cancelling remaining tasks...");

        List<TaskWrapper> remainingTasks = new ArrayList<>();
        taskQueue.drainTo(remainingTasks);

        for (TaskWrapper wrapper : remainingTasks) {
            String taskId = wrapper.getTask().getTaskId();
            wrapper.cancel();
            historyService.markCancelled(taskId, 0, "Task cancelled due to service shutdown");
            logger.info("Cancelled queued task: {}", taskId);
        }

        // Cancel currently running task
        TaskWrapper current = currentTask.get();
        if (current != null) {
            current.cancel();
            notifyProcessorOfCancellation(current.getTask());
            logger.info("Cancelled currently running task: {}", current.getTask().getTaskId());
        }

        logger.info("Queue drain completed. Cancelled {} queued tasks", remainingTasks.size());
    }
}
