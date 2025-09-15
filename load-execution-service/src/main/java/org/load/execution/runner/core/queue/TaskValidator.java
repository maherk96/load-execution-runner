package org.load.execution.runner.core.queue;

import lombok.Data;
import org.load.execution.runner.api.dto.TaskDto;
import org.load.execution.runner.api.dto.TaskExecution;
import org.load.execution.runner.api.exception.DuplicateTaskException;
import org.load.execution.runner.api.exception.InvalidTaskException;
import org.load.execution.runner.api.exception.NoProcessorException;
import org.load.execution.runner.api.exception.NoProcessorsException;
import org.load.execution.runner.api.exception.QueueCapacityException;
import org.load.execution.runner.api.exception.ServiceShutdownException;
import org.load.execution.runner.api.exception.TaskValidationException;
import org.load.execution.runner.config.TaskQueueConfig;
import org.load.execution.runner.core.history.TaskHistoryService;
import org.load.execution.runner.core.model.TaskType;
import org.load.execution.runner.core.processor.TaskProcessorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Comprehensive task validation component that enforces business rules and system constraints
 * before tasks are accepted into the processing queue.
 *
 * <p>This validator performs multi-layered validation to ensure system integrity and prevent
 * invalid or problematic tasks from entering the processing pipeline. The validation process
 * includes:
 *
 * <ul>
 *   <li><b>Service State Validation:</b> Ensures the service is operational and ready to accept tasks</li>
 *   <li><b>Task Structure Validation:</b> Validates task object integrity, required fields, and constraints</li>
 *   <li><b>Duplicate Detection:</b> Prevents duplicate task submissions based on ID and timing rules</li>
 *   <li><b>Business Rules Validation:</b> Enforces domain-specific rules like processor availability and capacity limits</li>
 * </ul>
 *
 * <p>The validator is designed to fail fast and provide clear error messages to help clients
 * understand why their task submission was rejected. All validation failures are logged
 * appropriately for monitoring and debugging purposes.
 *
 * <p><b>Thread Safety:</b> This component is thread-safe and can be used concurrently by multiple
 * threads. The shutdown flag is volatile to ensure visibility across threads.
 *
 * <p><b>Validation Order:</b> Validations are performed in a specific order to optimize performance:
 * <ol>
 *   <li>Service state (fastest check)</li>
 *   <li>Task structure and constraints</li>
 *   <li>Duplicate detection (requires history lookup)</li>
 *   <li>Business rules (may involve processor queries)</li>
 * </ol>
 *
 * @author Task Queue Team
 * @version 1.0.0
 * @since 1.0.0
 * @see TaskDto
 * @see ImprovedTaskQueueService
 */
@Data
@Component
public class TaskValidator {

    private static final Logger logger = LoggerFactory.getLogger(TaskValidator.class);

    /** Configuration settings for task queue constraints and limits */
    private final TaskQueueConfig config;

    /** Manager for available task processors and their capabilities */
    private final TaskProcessorManager processorManager;

    /** Service for tracking task history and detecting duplicates */
    private final TaskHistoryService historyService;

    /** Flag indicating if the service is shutting down (volatile for thread visibility)
     * -- GETTER --
     *  Returns whether the service is currently shutting down.
     *
     * @return true if the service is shutting down, false otherwise
     */
    private volatile boolean isShuttingDown = false;

    /**
     * Constructs a new TaskValidator with the specified dependencies.
     *
     * @param config           configuration settings for validation rules
     * @param processorManager manager for task processors
     * @param historyService   service for task history operations
     */
    public TaskValidator(TaskQueueConfig config, TaskProcessorManager processorManager, TaskHistoryService historyService) {
        this.config = config;
        this.processorManager = processorManager;
        this.historyService = historyService;
    }

    /**
     * Validates the current service state to ensure it can accept new tasks.
     *
     * <p>This is the first validation step and performs the fastest checks:
     * <ul>
     *   <li>Ensures the service is not shutting down</li>
     *   <li>Verifies that task processors are available</li>
     * </ul>
     *
     * <p>This method should be called before any other validation to fail fast
     * when the service is not ready to accept tasks.
     *
     * @throws ServiceShutdownException if the service is shutting down
     * @throws NoProcessorsException    if no task processors are available
     */
    public void validateServiceState() {
        // Check if service is shutting down (fastest possible check)
        if (isShuttingDown) {
            logger.warn("Task submission rejected - service is shutting down");
            throw new ServiceShutdownException("Service is shutting down");
        }

        // Verify processors are available for task execution
        if (!processorManager.hasProcessors()) {
            logger.error("Task submission rejected - no processors available");
            throw new NoProcessorsException("No task processors are available");
        }
    }

    /**
     * Validates the structure and content of a task object.
     *
     * <p>This method performs comprehensive validation of the task object including:
     * <ul>
     *   <li><b>Null checks:</b> Task object and required fields</li>
     *   <li><b>ID validation:</b> Length and format constraints</li>
     *   <li><b>Type validation:</b> Ensures task type is specified</li>
     *   <li><b>Timestamp validation:</b> Checks creation time and age limits</li>
     *   <li><b>Data validation:</b> Ensures task data is present</li>
     * </ul>
     *
     * <p>Age validation prevents old tasks from being processed, which
     * could indicate stale or problematic submissions.
     *
     * @param task the task to validate
     * @throws InvalidTaskException if any validation rule is violated
     */
    public void validateTask(TaskDto task) {
        // Basic null check for the entire task object
        if (task == null) {
            logger.warn("Task submission rejected - task is null");
            throw new InvalidTaskException("Task cannot be null");
        }

        String taskId = task.getTaskId();

        // Validate task ID presence and format
        if (taskId == null || taskId.trim().isEmpty()) {
            logger.warn("Task submission rejected - missing task ID");
            throw new InvalidTaskException("Task ID is required");
        }

        // Enforce maximum task ID length to prevent abuse
        if (taskId.length() > config.getMaxTaskIdLength()) {
            logger.warn("Task submission rejected - task ID too long: {}", taskId.length());
            throw new InvalidTaskException(String.format("Task ID too long (max %d characters)", config.getMaxTaskIdLength()));
        }

        // Validate required task type
        if (task.getTaskType() == null) {
            logger.warn("Task {} rejected - missing task type", taskId);
            throw new InvalidTaskException("Task type is required");
        }

        // Double-check task ID (defensive programming)
        if (task.getTaskId() == null) {
            logger.warn("Task {} rejected - missing task ID", taskId);
            throw new InvalidTaskException("Task ID is required");
        }

        // Validate creation timestamp
        if (task.getCreatedAt() == null) {
            logger.warn("Task {} rejected - missing creation timestamp", taskId);
            throw new InvalidTaskException("Task creation timestamp is required");
        }

        // Check if task is too old (prevents stale task processing)
        LocalDateTime createdAt = task.getCreatedAt();
        long hoursOld = ChronoUnit.HOURS.between(createdAt, LocalDateTime.now());
        if (hoursOld > config.getMaxTaskAgeHours()) {
            logger.warn("Task {} rejected - too old: {} hours", taskId, hoursOld);
            throw new InvalidTaskException(String.format("Task too old (%d hours). Maximum age is %d hours.",
                    hoursOld, config.getMaxTaskAgeHours()));
        }

        // Validate task data presence (even if empty, the map should not be null)
        if (task.getData() == null) {
            logger.warn("Task {} rejected - data map is null", taskId);
            throw new InvalidTaskException("Task data cannot be null");
        }
    }

    /**
     * Checks for duplicate task submissions based on task ID and timing rules.
     *
     * <p>This method prevents duplicate task processing by enforcing the following rules:
     * <ul>
     *   <li><b>Active Task Check:</b> Rejects tasks with IDs that are currently active (queued or processing)</li>
     *   <li><b>Recent Completion Check:</b> Prevents rapid resubmission of recently completed tasks</li>
     * </ul>
     *
     * <p>The recent completion check uses a configurable time window (typically 1 hour)
     * to prevent accidental duplicate submissions while allowing legitimate resubmission
     * of tasks after a reasonable delay.
     *
     * @param task the task to check for duplicates
     * @throws DuplicateTaskException if a duplicate is detected
     */
    public void checkDuplicate(TaskDto task) {
        String taskId = task.getTaskId();

        // Check if task is currently active (queued or processing)
        if (historyService.isTaskActive(taskId)) {
            logger.warn("Task {} rejected - duplicate task ID (still active)", taskId);
            throw new DuplicateTaskException("Task with this ID is already active");
        }

        // Check for recently completed tasks with the same ID
        TaskExecution recentExecution = historyService.getTaskExecution(taskId);
        if (recentExecution != null && recentExecution.getCompletedAt() != null) {
            long hoursAgo = ChronoUnit.HOURS.between(recentExecution.getCompletedAt(), LocalDateTime.now());

            // Prevent rapid resubmission (typically within 1 hour)
            if (hoursAgo < 1) {
                logger.warn("Task {} rejected - duplicate task completed {} hours ago", taskId, hoursAgo);
                throw new DuplicateTaskException(String.format("Task with this ID was completed %.1f hours ago", hoursAgo / 60.0));
            }
        }
    }

    /**
     * Validates business rules and system constraints for task processing.
     *
     * <p>This method enforces domain-specific validation rules including:
     * <ul>
     *   <li><b>Processor Availability:</b> Ensures a processor exists for the task type</li>
     *   <li><b>Queue Capacity:</b> Prevents queue overflow by checking current size</li>
     *   <li><b>Task-Specific Validation:</b> Delegates to processor-specific validation logic</li>
     * </ul>
     *
     * <p>The processor-specific validation allows different task types to have
     * custom validation rules implemented by their respective processors.
     *
     * @param task             the task to validate
     * @param currentQueueSize the current number of tasks in the queue
     * @throws NoProcessorException      if no processor is available for the task type
     * @throws QueueCapacityException    if the queue is at maximum capacity
     * @throws TaskValidationException   if processor-specific validation fails
     */
    public void validateBusinessRules(TaskDto task, int currentQueueSize) {
        TaskType taskType = task.getTaskType();
        String taskId = task.getTaskId();

        // Verify processor availability for the specific task type
        if (!processorManager.hasProcessor(taskType)) {
            logger.warn("Task {} rejected - no processor for type: {}", taskId, taskType);
            throw new NoProcessorException(String.format("No processor available for task type: %s. Available types: %s",
                    taskType, processorManager.getAvailableTaskTypes()));
        }

        // Enforce queue capacity limits to prevent system overload
        if (currentQueueSize >= config.getMaxQueueSize()) {
            logger.warn("Task {} rejected - queue capacity exceeded: {}", taskId, currentQueueSize);
            throw new QueueCapacityException(String.format("Queue capacity exceeded (%d tasks). Please try again later.",
                    currentQueueSize));
        }

        // Delegate to processor-specific validation logic
        try {
            processorManager.validateTask(task);
        } catch (IllegalArgumentException e) {
            // Convert processor validation errors to our exception hierarchy
            logger.warn("Task {} rejected - processor validation failed: {}", taskId, e.getMessage());
            throw new TaskValidationException("Task validation failed: " + e.getMessage());
        }
    }

    /**
     * Sets the shutdown flag to indicate the service is shutting down.
     *
     * <p>When this flag is set to true, subsequent validation calls will
     * reject new tasks with a {@link ServiceShutdownException}.
     *
     * <p>This method is typically called by the task queue service during
     * its shutdown process to prevent new tasks from being accepted.
     *
     * @param shuttingDown true if the service is shutting down, false otherwise
     */
    public void setShuttingDown(boolean shuttingDown) {
        this.isShuttingDown = shuttingDown;
        if (shuttingDown) {
            logger.info("TaskValidator - shutdown flag set, will reject new tasks");
        }
    }

}