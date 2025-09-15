package org.load.execution.runner.core.history;

import org.load.execution.runner.api.dto.TaskDto;
import org.load.execution.runner.api.dto.TaskExecution;
import org.load.execution.runner.config.TaskQueueConfig;
import org.load.execution.runner.core.model.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Service for tracking and managing the execution history of tasks.
 * <p>
 * This service maintains an in-memory history of tasks, including their
 * lifecycle states (queued, processing, completed, failed, cancelled, timeout)
 * and provides methods to query task status, execution details, and aggregated metrics.
 * </p>
 *
 * <p><b>Responsibilities:</b></p>
 * <ul>
 *   <li>Records when tasks are queued, started, completed, failed, or cancelled.</li>
 *   <li>Maintains a list of active task IDs for quick lookup.</li>
 *   <li>Provides history queries and status breakdowns for monitoring purposes.</li>
 *   <li>Periodically cleans up old task history based on retention policy from {@link TaskQueueConfig}.</li>
 * </ul>
 */
@Service
public class TaskHistoryService {
    private static final Logger logger = LoggerFactory.getLogger(TaskHistoryService.class);

    private final TaskQueueConfig config;
    private final ConcurrentHashMap<String, TaskExecution> taskHistory = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LocalDateTime> activeTaskIds = new ConcurrentHashMap<>();

    /**
     * Creates a new TaskHistoryService.
     *
     * @param config the configuration for task queue behavior, including retention policy
     */
    public TaskHistoryService(TaskQueueConfig config) {
        this.config = config;
    }

    /**
     * Records that a task has been queued.
     *
     * @param task          the {@link TaskDto} representing the task
     * @param queuePosition the position of the task in the queue
     */
    public void recordQueued(TaskDto task, int queuePosition) {
        String taskId = task.getTaskId();
        activeTaskIds.put(taskId, LocalDateTime.now());

        TaskExecution execution = new TaskExecution(
                taskId, task.getTaskType(), TaskStatus.QUEUED,
                LocalDateTime.now(), null, null, 0, null, queuePosition
        );
        taskHistory.put(taskId, execution);
        logger.info("Task {} queued at position {}", taskId, queuePosition);
    }

    /**
     * Marks a task as currently being processed.
     *
     * @param taskId the ID of the task
     */
    public void markProcessing(String taskId) {
        LocalDateTime startTime = LocalDateTime.now();
        updateTaskHistory(taskId, TaskStatus.PROCESSING, startTime, null, 0, null);
        logger.info("STARTED processing task: {}", taskId);
    }

    /**
     * Marks a task as successfully completed.
     *
     * @param taskId           the ID of the task
     * @param processingTimeMs the time taken to process the task in milliseconds
     */
    public void markCompleted(String taskId, long processingTimeMs) {
        LocalDateTime completedTime = LocalDateTime.now();
        updateTaskHistory(taskId, TaskStatus.COMPLETED, null, completedTime, processingTimeMs, null);
        activeTaskIds.remove(taskId);
        logger.info("COMPLETED task: {} in {}ms", taskId, processingTimeMs);
    }

    /**
     * Marks a task as failed and records the error message.
     * Automatically sets status to {@link TaskStatus#TIMEOUT} if error message contains "timeout".
     *
     * @param taskId           the ID of the task
     * @param processingTimeMs the time taken before failure in milliseconds
     * @param errorMessage     the error message or {@code null} if unknown
     */
    public void markFailed(String taskId, long processingTimeMs, String errorMessage) {
        LocalDateTime completedTime = LocalDateTime.now();

        String safeErrorMessage = errorMessage != null ? errorMessage : "Unknown error";
        TaskStatus status = safeErrorMessage.contains("timeout") ? TaskStatus.TIMEOUT : TaskStatus.FAILED;

        updateTaskHistory(taskId, status, null, completedTime, processingTimeMs, safeErrorMessage);
        activeTaskIds.remove(taskId);
        logger.error("FAILED task: {} after {}ms - Error: {}", taskId, processingTimeMs, safeErrorMessage);
    }

    /**
     * Marks a task as cancelled.
     *
     * @param taskId           the ID of the task
     * @param processingTimeMs the time spent before cancellation
     * @param reason           the reason for cancellation
     */
    public void markCancelled(String taskId, long processingTimeMs, String reason) {
        LocalDateTime completedTime = LocalDateTime.now();
        updateTaskHistory(taskId, TaskStatus.CANCELLED, null, completedTime, processingTimeMs, reason);
        activeTaskIds.remove(taskId);
        logger.info("CANCELLED task: {} after {}ms - Reason: {}", taskId, processingTimeMs, reason);
    }

    /**
     * Checks if a task is currently active (queued or processing).
     *
     * @param taskId the ID of the task
     * @return {@code true} if active, {@code false} otherwise
     */
    public boolean isTaskActive(String taskId) {
        return activeTaskIds.containsKey(taskId);
    }

    /**
     * Retrieves the {@link TaskExecution} details for a specific task.
     *
     * @param taskId the task ID
     * @return the execution details, or {@code null} if not found
     */
    public TaskExecution getTaskExecution(String taskId) {
        return taskHistory.get(taskId);
    }

    /**
     * Returns a list of recent task executions, sorted by queue time (most recent first).
     *
     * @param limit the maximum number of tasks to return
     * @return a list of recent {@link TaskExecution} objects
     */
    public List<TaskExecution> getTaskHistory(int limit) {
        return taskHistory.values().stream()
                .sorted((a, b) -> b.getQueuedAt().compareTo(a.getQueuedAt()))
                .limit(limit)
                .collect(Collectors.toList());
    }

    /**
     * Returns a list of tasks with the specified status.
     *
     * @param status the desired status
     * @param limit  the maximum number of tasks to return
     * @return a list of tasks matching the given status
     */
    public List<TaskExecution> getTasksByStatus(TaskStatus status, int limit) {
        return taskHistory.values().stream()
                .filter(task -> task.getStatus() == status)
                .sorted((a, b) -> b.getQueuedAt().compareTo(a.getQueuedAt()))
                .limit(limit)
                .collect(Collectors.toList());
    }

    /**
     * Returns a breakdown of task counts per status.
     *
     * @return a map where keys are {@link TaskStatus} values and values are counts
     */
    public Map<TaskStatus, Long> getStatusBreakdown() {
        return taskHistory.values().stream()
                .collect(Collectors.groupingBy(TaskExecution::getStatus, Collectors.counting()));
    }

    /**
     * @return the number of currently active tasks.
     */
    public int getActiveTaskCount() {
        return activeTaskIds.size();
    }

    /**
     * @return the total number of tasks in the history (active + completed).
     */
    public int getHistorySize() {
        return taskHistory.size();
    }

    /**
     * Updates the task history with a new status and relevant timing information.
     *
     * @param taskId           the ID of the task
     * @param status           the new status
     * @param startedAt        the start time (if applicable)
     * @param completedAt      the completion time (if applicable)
     * @param processingTimeMs the total processing time in milliseconds
     * @param errorMessage     an optional error message
     */
    private void updateTaskHistory(String taskId, TaskStatus status,
                                   LocalDateTime startedAt, LocalDateTime completedAt,
                                   long processingTimeMs, String errorMessage) {
        taskHistory.computeIfPresent(taskId, (key, existing) ->
                new TaskExecution(
                        taskId, existing.getTaskType(), status, existing.getQueuedAt(),
                        startedAt != null ? startedAt : existing.getStartedAt(),
                        completedAt, processingTimeMs, errorMessage, existing.getQueuePosition()
                )
        );
    }

    /**
     * Periodically removes old task history entries that exceed the retention window.
     * Runs once per hour.
     */
    @Scheduled(fixedRate = 3600000) // Every hour
    public void cleanupTaskHistory() {
        LocalDateTime cutoff = LocalDateTime.now().minusHours(config.getTaskHistoryRetentionHours());

        taskHistory.entrySet().removeIf(entry -> {
            TaskExecution execution = entry.getValue();
            LocalDateTime completedAt = execution.getCompletedAt();
            return completedAt != null && completedAt.isBefore(cutoff);
        });

        logger.debug("Task history cleanup completed. Remaining entries: {}", taskHistory.size());
    }

    /**
     * Helper method to determine if a status represents a finished task.
     *
     * @param status the task status
     * @return {@code true} if completed, failed, cancelled, or timed out
     */
    private boolean isTaskFinished(TaskStatus status) {
        return status == TaskStatus.COMPLETED ||
                status == TaskStatus.FAILED ||
                status == TaskStatus.CANCELLED ||
                status == TaskStatus.TIMEOUT;
    }
}
