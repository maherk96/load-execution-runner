package org.load.execution.runner.core.queue;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
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

/**
 * Enhanced task queue service that provides robust task processing capabilities with comprehensive
 * lifecycle management, monitoring, and graceful shutdown support.
 *
 */
@Service
public class ImprovedTaskQueueService {
    private static final Logger logger = LoggerFactory.getLogger(ImprovedTaskQueueService.class);

    /** Validates tasks before processing */
    private final TaskValidator validator;

    /** Manages task history and status tracking */
    private final TaskHistoryService historyService;

    /** Manages different types of task processors */
    private final TaskProcessorManager processorManager;

    /** Configuration settings for the task queue */
    private final TaskQueueConfig config;

    /** Thread-safe blocking queue for pending tasks */
    private final BlockingQueue<TaskWrapper> taskQueue = new LinkedBlockingQueue<>();

    /** Single-threaded executor for sequential queue processing */
    private final ExecutorService queueProcessor;

    /** Shared thread pool for executing individual tasks */
    private final ExecutorService taskRunnerPool;

    /** Scheduled executor for maintenance tasks like status logging */
    private final ScheduledExecutorService maintenanceExecutor;

    /** Reference to the currently processing task (null if none) */
    private final AtomicReference<TaskWrapper> currentTask = new AtomicReference<>();

    /** Counter for successfully completed tasks */
    private final AtomicInteger completedTasks = new AtomicInteger(0);

    /** Counter for failed tasks */
    private final AtomicInteger failedTasks = new AtomicInteger(0);

    /** Total processing time for completed tasks in milliseconds */
    private final AtomicLong totalProcessingTimeMs = new AtomicLong(0);

    /** Current state of the service
     * -- GETTER --
     *  Returns the current service state.
     *
     * @return current service state
     */
    @Getter
    private volatile ServiceState serviceState = ServiceState.STARTING;

    /** Timestamp when the service was started */
    private volatile LocalDateTime serviceStartTime;

    /**
     * Constructs a new ImprovedTaskQueueService with the specified dependencies.
     *
     * <p>This constructor initializes the thread pools but does not start processing.
     * The {@link #initialize()} method must be called to begin task processing.
     *
     * @param validator        task validator for input validation
     * @param historyService   service for managing task history and status
     * @param processorManager manager for task processors
     * @param config          configuration settings for the queue
     */
    public ImprovedTaskQueueService(TaskValidator validator, TaskHistoryService historyService,
                                    TaskProcessorManager processorManager, TaskQueueConfig config) {
        this.validator = validator;
        this.historyService = historyService;
        this.processorManager = processorManager;
        this.config = config;

        // Initialize single-threaded queue processor for sequential task handling
        this.queueProcessor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "TaskQueueProcessor");
            thread.setDaemon(false);  // Keep JVM alive while processing tasks
            thread.setUncaughtExceptionHandler(this::handleUncaughtException);
            return thread;
        });

        // Initialize shared thread pool for task execution
        this.taskRunnerPool = Executors.newSingleThreadExecutor( runnable -> {
            Thread thread = new Thread(runnable, "TaskRunner");
            thread.setDaemon(false);  // Keep JVM alive while executing tasks
            thread.setUncaughtExceptionHandler(this::handleUncaughtException);
            return thread;
        });

        // Initialize maintenance executor for periodic tasks
        this.maintenanceExecutor = Executors.newSingleThreadScheduledExecutor( runnable -> {
            Thread thread = new Thread(runnable, "TaskQueueMaintenance");
            thread.setDaemon(true);  // Allow JVM to exit even if maintenance is running
            return thread;
        });
    }

    /**
     * Initializes the task queue service and starts processing.
     *
     * <p>This method is automatically called by Spring after dependency injection.
     * It starts the queue processor thread and maintenance tasks.
     */
    @PostConstruct
    public void initialize() {
        serviceStartTime = LocalDateTime.now();
        serviceState = ServiceState.RUNNING;
        logger.info("TaskQueueService initializing...");

        // Start the main processing loop
        startProcessing();

        // Start periodic maintenance tasks
        startMaintenanceTasks();

        logger.info("Config loaded {}",config);
        logger.info("TaskQueueService initialized - State: {}, Max Concurrent Tasks: {}",
                serviceState, config.getMaxConcurrentTasks());
    }

    /**
     * Submits a new task for processing.
     *
     * <p>The task goes through validation before being queued. If validation fails,
     * an error response is returned immediately. Successfully validated tasks are
     * placed in the queue and will be processed in FIFO order.
     *
     * @param task the task to submit for processing
     * @return response containing task status and queue position
     */
    public TaskResponseDto submitTask(TaskDto task) {
        String taskId = task != null ? task.getTaskId() : "unknown";

        try {
            // Set up MDC for better log traceability across threads
            MDC.put("taskId", taskId);
            logger.debug("Received task submission request");

            // Fast-fail if service is shutting down
            if (serviceState != ServiceState.RUNNING) {
                logger.warn("Task submission rejected - service state: {}", serviceState);
                return createErrorResponse(taskId, "Service is not accepting new tasks");
            }

            // Perform comprehensive validation
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

    /**
     * Cancels a task by its ID.
     *
     * <p>Tasks can be cancelled in different states:
     * <ul>
     *   <li><b>Queued:</b> Removed from queue immediately</li>
     *   <li><b>Processing:</b> Interruption signal sent to processor</li>
     *   <li><b>Completed/Failed:</b> Cannot be cancelled</li>
     * </ul>
     *
     * @param taskId the ID of the task to cancel
     * @return response indicating cancellation result
     */
    public TaskResponseDto cancelTask(String taskId) {
        if (taskId == null || taskId.trim().isEmpty()) {
            return createErrorResponse("invalid-id", "Task ID is required");
        }

        try {
            MDC.put("taskId", taskId);
            logger.info("Processing cancel request");

            // Check if task exists
            if (!historyService.isTaskActive(taskId) && historyService.getTaskExecution(taskId) == null) {
                logger.warn("Cancel request for unknown task");
                return createErrorResponse(taskId, "Task not found");
            }

            // Check if task is already finished
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
                    // Let the task processor handle status update with proper timing
                    return new TaskResponseDto(taskId, "CANCELLED", "Task cancellation requested");
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

    /**
     * Retrieves the current status of a task.
     *
     * @param taskId the ID of the task to query
     * @return response containing current task status and details
     */
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

    /**
     * Retrieves detailed task execution information.
     *
     * @param taskId the ID of the task to query
     * @return detailed task execution object or null if not found
     */
    public TaskExecution getDetailedTaskStatus(String taskId) {
        return historyService.getTaskExecution(taskId);
    }

    /**
     * Retrieves task history with a limit on the number of results.
     *
     * @param limit maximum number of task executions to return
     * @return list of recent task executions
     */
    public List<TaskExecution> getTaskHistory(int limit) {
        return historyService.getTaskHistory(limit);
    }

    /**
     * Retrieves tasks by their status.
     *
     * @param status the task status to filter by
     * @param limit  maximum number of results to return
     * @return list of task executions with the specified status
     */
    public List<TaskExecution> getTasksByStatus(TaskStatus status, int limit) {
        return historyService.getTasksByStatus(status, limit);
    }

    /**
     * Provides comprehensive service status information including:
     * <ul>
     *   <li>Service state and uptime</li>
     *   <li>Queue statistics</li>
     *   <li>Processing metrics</li>
     *   <li>Configuration details</li>
     *   <li>Available task types</li>
     * </ul>
     *
     * @return map containing detailed service status information
     */
    public Map<String, Object> getServiceStatus() {
        TaskWrapper current = currentTask.get();
        long uptimeHours = ChronoUnit.HOURS.between(serviceStartTime, LocalDateTime.now());

        Map<String, Object> status = new HashMap<>();

        // Basic service information
        status.put("serviceName", "TaskQueueService");
        status.put("version", "3.0.0");
        status.put("serviceState", serviceState.name());
        status.put("uptimeHours", uptimeHours);
        status.put("startTime", serviceStartTime);

        // Current queue state
        status.put("queueSize", taskQueue.size());
        status.put("isProcessing", current != null);
        status.put("currentTask", current != null ? Map.of(
                "taskId", current.getTask().getTaskId(),
                "taskType", current.getTask().getTaskType().name(),
                "startedAt", current.getQueuedAt(),
                "processingTimeMs", ChronoUnit.MILLIS.between(current.getQueuedAt(), LocalDateTime.now())
        ) : null);

        // Processing statistics
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

        // Available capabilities
        status.put("availableTaskTypes", processorManager.getAvailableTaskTypes().stream()
                .map(Enum::name)
                .sorted()
                .collect(Collectors.toList()));

        // Configuration details
        status.put("configuration", Map.of(
                "maxQueueSize", config.getMaxQueueSize(),
                "maxConcurrentTasks", config.getMaxConcurrentTasks(),
                "taskTimeoutMinutes", config.getTaskTimeoutMinutes()
        ));

        // Additional metrics
        status.put("activeTasks", historyService.getActiveTaskCount());
        status.put("taskHistorySize", historyService.getHistorySize());

        return status;
    }

    /**
     * Returns the set of task types that can be processed by this service.
     *
     * @return set of available task types
     */
    public Set<TaskType> getAvailableTaskTypes() {
        return processorManager.getAvailableTaskTypes();
    }

    /**
     * Returns the current number of tasks in the queue.
     *
     * @return current queue size
     */
    public int getQueueSize() {
        return taskQueue.size();
    }

    /**
     * Checks if a task is currently being processed.
     *
     * @return true if a task is currently processing, false otherwise
     */
    public boolean isCurrentlyProcessing() {
        return currentTask.get() != null;
    }

    /**
     * Returns the ID of the currently processing task.
     *
     * @return task ID if processing, null otherwise
     */
    public String getCurrentTaskId() {
        TaskWrapper current = currentTask.get();
        return current != null ? current.getTask().getTaskId() : null;
    }

    /**
     * Queues a validated task for processing.
     *
     * <p>This method adds the task to the internal queue and records it in the history service.
     * It calculates the queue position before adding to ensure accurate positioning.
     *
     * @param task the validated task to queue
     * @return response indicating successful queuing with position information
     */
    private TaskResponseDto queueTask(TaskDto task) {
        String taskId = task.getTaskId();

        // Calculate position BEFORE adding to queue for accuracy
        int currentPosition = taskQueue.size() + 1;

        TaskWrapper wrapper = new TaskWrapper(task);
        taskQueue.offer(wrapper);

        // Delegate history tracking to service
        historyService.recordQueued(task, currentPosition);

        logger.info("Task queued - Queue size: {}, Position: {}", taskQueue.size(), currentPosition);

        // Determine response based on queue state
        if (currentPosition == 1 && currentTask.get() == null) {
            return new TaskResponseDto(taskId, "PROCESSING", "Task is being processed");
        } else {
            return new TaskResponseDto(taskId, "QUEUED", currentPosition,
                    String.format("Task queued at position %d", currentPosition));
        }
    }

    /**
     * Starts the main task processing loop in a dedicated thread.
     */
    private void startProcessing() {
        queueProcessor.submit(this::processLoop);
    }

    /**
     * Main processing loop that continuously takes tasks from the queue and processes them.
     *
     * <p>This method runs in a dedicated thread and processes tasks sequentially.
     * It handles interruptions gracefully during shutdown.
     */
    private void processLoop() {
        logger.info("Queue processor started");

        while (!Thread.currentThread().isInterrupted() && serviceState == ServiceState.RUNNING) {
            try {
                // Block until a task is available
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

    /**
     * Safely processes a single task with comprehensive error handling and timing.
     *
     * <p>This method handles the complete task lifecycle:
     * <ul>
     *   <li>Pre-processing cancellation checks</li>
     *   <li>Task execution with timeout</li>
     *   <li>Status updates and metrics tracking</li>
     *   <li>Error handling and logging</li>
     *   <li>Cleanup</li>
     * </ul>
     *
     * @param wrapper the task wrapper containing the task and metadata
     */
    private void processTaskSafely(TaskWrapper wrapper) {
        TaskDto task = wrapper.getTask();
        String taskId = task.getTaskId();
        long startTime = System.currentTimeMillis();
        boolean taskStatusSet = false;
        boolean wasCancelled = false;

        try {
            MDC.put("taskId", taskId);

            // Check for pre-processing cancellation
            if (wrapper.isCancelled()) {
                wasCancelled = true;
                logger.info("Task was cancelled before processing started");
                long duration = System.currentTimeMillis() - startTime;
                historyService.markCancelled(taskId, duration, "Task cancelled before processing");
                return;
            }

            logger.info("STARTED processing task - Type: {}, Queue size: {}",
                    task.getTaskType(), taskQueue.size());

            // Update status to processing
            historyService.markProcessing(taskId);

            // Submit to shared thread pool for execution
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

            // Wait for completion with configurable timeout
            executionFuture.get(config.getTaskTimeoutMinutes(), TimeUnit.MINUTES);

            // Final cancellation check after completion
            if (wrapper.isCancelled()) {
                wasCancelled = true;
                throw new InterruptedException("Task was cancelled during execution");
            }

            // Task completed successfully
            long duration = System.currentTimeMillis() - startTime;
            completedTasks.incrementAndGet();
            totalProcessingTimeMs.addAndGet(duration);
            historyService.markCompleted(taskId, duration);
            taskStatusSet = true;

            logger.info("COMPLETED task in {}ms - Total completed: {}", duration, completedTasks.get());

        } catch (InterruptedException | CancellationException e) {
            // Handle cancellation gracefully
            wasCancelled = true;
            if (!taskStatusSet) {
                long duration = System.currentTimeMillis() - startTime;
                historyService.markCancelled(taskId, duration, "Task was cancelled");
                failedTasks.incrementAndGet();
                taskStatusSet = true;
                logger.info("Task CANCELLED after {}ms", duration);
            }

        } catch (TimeoutException e) {
            // Handle task timeout
            if (!taskStatusSet) {
                long duration = System.currentTimeMillis() - startTime;
                wrapper.cancel();
                historyService.markFailed(taskId, duration, "Task timeout after " + config.getTaskTimeoutMinutes() + " minutes");
                failedTasks.incrementAndGet();
                taskStatusSet = true;
                logger.error("Task TIMEOUT after {}ms", duration);
            }

        } catch (Exception e) {
            // Handle unexpected errors
            if (!taskStatusSet) {
                long duration = System.currentTimeMillis() - startTime;
                failedTasks.incrementAndGet();

                String errorMessage = e.getMessage();
                if (errorMessage == null) {
                    errorMessage = "Unknown error: " + e.getClass().getSimpleName();
                }

                historyService.markFailed(taskId, duration, errorMessage);
                taskStatusSet = true;
                logger.error("Task FAILED after {}ms - Error: {}", duration, errorMessage, e);
            }

        } finally {
            // Clean up current task reference
            currentTask.set(null);
            MDC.remove("taskId");

            // Final outcome logging
            if (wasCancelled) {
                logger.info("Task processing completed: CANCELLED - taskId: {}", taskId);
            }
        }
    }

    /**
     * Executes a task with periodic interrupt checks for better cancellation support.
     *
     * <p>This method delegates to the processor manager while providing robust
     * cancellation handling throughout the execution process.
     *
     * @param task    the task to execute
     * @param wrapper the task wrapper for cancellation checks
     * @throws Exception if task execution fails or is interrupted
     */
    private void executeTaskWithInterruptChecks(TaskDto task, TaskWrapper wrapper) throws Exception {
        // Pre-execution cancellation check
        if (Thread.currentThread().isInterrupted() || wrapper.isCancelled()) {
            logger.info("Task cancellation detected before execution");
            throw new InterruptedException("Task was cancelled");
        }

        try {
            // Delegate to processor manager for actual execution
            processorManager.processTask(task);
        } catch (InterruptedException e) {
            logger.info("Task processor was interrupted - handling cancellation gracefully");
            throw e;
        } catch (Exception e) {
            // Check if failure was due to interruption
            if (Thread.currentThread().isInterrupted() || wrapper.isCancelled()) {
                logger.info("Task failed due to cancellation");
                throw new InterruptedException("Task was interrupted");
            }
            throw e;
        }

        // Post-execution cancellation check
        if (Thread.currentThread().isInterrupted() || wrapper.isCancelled()) {
            logger.info("Task cancellation detected after execution");
            throw new InterruptedException("Task was cancelled after processing");
        }
    }

    /**
     * Notifies the task processor about cancellation if it supports interruption.
     *
     * @param task the task being cancelled
     */
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

    /**
     * Removes a task from the queue by task ID.
     *
     * @param taskId the ID of the task to remove
     * @return true if task was found and removed, false otherwise
     */
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
     * Calculates the current queue position for a task.
     *
     * <p>This method dynamically computes position rather than storing it,
     * ensuring accuracy as the queue changes.
     *
     * @param taskId the task ID to find
     * @return current position (0 if processing, -1 if not found, >0 if queued)
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

    /**
     * Builds a detailed status response for a task based on its current execution state.
     *
     * @param taskId    the task ID
     * @param execution the current task execution details
     * @return formatted status response with appropriate messaging
     */
    private TaskResponseDto buildStatusResponse(String taskId, TaskExecution execution) {
        TaskResponseDto response = new TaskResponseDto(taskId, execution.getStatus().name(),
                execution.getQueuePosition(),
                String.format("Task %s", execution.getStatus().name().toLowerCase()));

        // Customize response based on current status
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

    /**
     * Checks if a task status represents a finished state.
     *
     * @param status the task status to check
     * @return true if the task is in a terminal state
     */
    private boolean isTaskFinished(TaskStatus status) {
        return status == TaskStatus.COMPLETED ||
                status == TaskStatus.FAILED ||
                status == TaskStatus.CANCELLED ||
                status == TaskStatus.TIMEOUT;
    }

    /**
     * Creates a standardized error response.
     *
     * @param taskId  the task ID
     * @param message the error message
     * @return error response DTO
     */
    private TaskResponseDto createErrorResponse(String taskId, String message) {
        return new TaskResponseDto(taskId, "ERROR", message);
    }

    /**
     * Handles uncaught exceptions in thread pools.
     *
     * @param thread    the thread where the exception occurred
     * @param exception the uncaught exception
     */
    private void handleUncaughtException(Thread thread, Throwable exception) {
        logger.error("Uncaught exception in thread {}: {}", thread.getName(), exception.getMessage(), exception);
    }

    /**
     * Starts periodic maintenance tasks.
     *
     * <p>Currently schedules queue status logging every 5 minutes.
     */
    private void startMaintenanceTasks() {
        maintenanceExecutor.scheduleAtFixedRate(this::logQueueStatus, 5, 5, TimeUnit.MINUTES);
    }

    /**
     * Logs current queue status for monitoring purposes.
     */
    private void logQueueStatus() {
        int queueSize = taskQueue.size();
        TaskWrapper current = currentTask.get();

        // Only log when there's activity to avoid spam
        if (queueSize > 0 || current != null) {
            logger.info("Queue status - Size: {}, Processing: {}, Service state: {}",
                    queueSize, current != null ? current.getTask().getTaskId() : "none", serviceState);
        }
    }

    /**
     * Initiates graceful shutdown of the task queue service.
     *
     * <p>This method is automatically called by Spring during application shutdown.
     * It performs the following steps:
     * <ul>
     *   <li>Sets service state to shutting down</li>
     *   <li>Optionally drains and cancels remaining tasks</li>
     *   <li>Shuts down all thread pools with timeout</li>
     *   <li>Forces shutdown if graceful shutdown times out</li>
     * </ul>
     */
    @PreDestroy
    public void shutdown() {
        logger.info("Initiating TaskQueueService shutdown...");
        serviceState = ServiceState.SHUTTING_DOWN;
        validator.setShuttingDown(true);

        // Optionally drain queue and cancel remaining tasks
        if (config.isDrainQueueOnShutdown()) {
            drainQueueAndCancelTasks();
        }

        // Shutdown executors in order of dependency
        maintenanceExecutor.shutdown();
        queueProcessor.shutdown();
        taskRunnerPool.shutdown();

        try {
            // Wait for graceful shutdown of queue processor
            if (!queueProcessor.awaitTermination(config.getShutdownTimeoutSeconds(), TimeUnit.SECONDS)) {
                logger.warn("Queue processor did not terminate within {} seconds, forcing shutdown",
                        config.getShutdownTimeoutSeconds());
                queueProcessor.shutdownNow();
            }

            // Wait for graceful shutdown of task runner pool
            if (!taskRunnerPool.awaitTermination(config.getShutdownTimeoutSeconds(), TimeUnit.SECONDS)) {
                logger.warn("Task runner pool did not terminate within {} seconds, forcing shutdown",
                        config.getShutdownTimeoutSeconds());
                taskRunnerPool.shutdownNow();
            }

            // Wait for maintenance executor
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
     * Drains the queue and cancels all remaining tasks during shutdown.
     *
     * <p>This method is called during shutdown if configured to drain the queue.
     * It removes all queued tasks and attempts to cancel the currently running task.
     */
    private void drainQueueAndCancelTasks() {
        logger.info("Draining queue and cancelling remaining tasks...");

        // Remove all queued tasks
        List<TaskWrapper> remainingTasks = new ArrayList<>();
        taskQueue.drainTo(remainingTasks);

        // Cancel each queued task
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