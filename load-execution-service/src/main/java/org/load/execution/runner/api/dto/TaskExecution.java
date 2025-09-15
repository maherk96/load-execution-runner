package org.load.execution.runner.api.dto;

import lombok.Data;
import org.load.execution.runner.core.model.TaskStatus;
import org.load.execution.runner.core.model.TaskType;

import java.time.LocalDateTime;

/**
 * Represents the execution details of a task within the execution runner.
 * <p>
 * This DTO provides information about a task's lifecycle, including when it was queued,
 * started, completed, and how long it took to process. It also holds status information,
 * error messages (if any), and the position of the task in the execution queue.
 * </p>
 *
 * <p>Features:</p>
 * <ul>
 *   <li>Immutable fields for thread-safety and reliability.</li>
 *   <li>Provides full visibility into task timing (queued, started, completed).</li>
 *   <li>Captures both successful and failed execution states via {@link TaskStatus}.</li>
 *   <li>Includes queue position for monitoring pending tasks.</li>
 * </ul>
 */
@Data
public class TaskExecution {

    /**
     * Unique identifier for the task.
     */
    private final String taskId;

    /**
     * The type/category of the task.
     * See {@link TaskType} for possible types.
     */
    private final TaskType taskType;

    /**
     * The current execution status of the task.
     * See {@link TaskStatus} for possible values.
     */
    private final TaskStatus status;

    /**
     * Timestamp when the task was added to the execution queue.
     */
    private final LocalDateTime queuedAt;

    /**
     * Timestamp when the task started execution.
     * May be {@code null} if the task has not started yet.
     */
    private final LocalDateTime startedAt;

    /**
     * Timestamp when the task completed execution.
     * May be {@code null} if the task is still running or queued.
     */
    private final LocalDateTime completedAt;

    /**
     * Total processing time for the task in milliseconds.
     * Typically calculated as {@code completedAt - startedAt}.
     */
    private final long processingTimeMs;

    /**
     * Error message if the task failed, or {@code null} if successful.
     */
    private final String errorMessage;

    /**
     * The task's position in the queue at the time of retrieval.
     * {@code 0} indicates that the task is next to be executed.
     */
    private final int queuePosition;

    /**
     * Creates a new {@code TaskExecution} instance with the provided details.
     *
     * @param taskId           the unique ID of the task
     * @param taskType         the type of the task
     * @param status           the current execution status
     * @param queuedAt         timestamp when the task was queued
     * @param startedAt        timestamp when the task started execution
     * @param completedAt      timestamp when the task completed execution
     * @param processingTimeMs the time taken to execute the task, in milliseconds
     * @param errorMessage     an error message if execution failed, otherwise {@code null}
     * @param queuePosition    the position of the task in the queue
     */
    public TaskExecution(String taskId, TaskType taskType, TaskStatus status,
                         LocalDateTime queuedAt, LocalDateTime startedAt,
                         LocalDateTime completedAt, long processingTimeMs,
                         String errorMessage, int queuePosition) {
        this.taskId = taskId;
        this.taskType = taskType;
        this.status = status;
        this.queuedAt = queuedAt;
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.processingTimeMs = processingTimeMs;
        this.errorMessage = errorMessage;
        this.queuePosition = queuePosition;
    }
}
