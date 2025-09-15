package org.load.execution.runner.api.dto;

import lombok.Data;

/**
 * Data Transfer Object (DTO) representing the response for a task request.
 * <p>
 * This class is typically returned by APIs to provide information about a task's
 * current state, its queue position (if applicable), and an optional message.
 * </p>
 *
 * <p>Usage examples:</p>
 * <pre>{@code
 * // Basic response
 * TaskResponseDto response = new TaskResponseDto("12345", "QUEUED", "Task has been added to the queue.");
 *
 * // Response with queue position
 * TaskResponseDto responseWithQueue = new TaskResponseDto("12345", "QUEUED", 3, "Task is in queue position 3.");
 * }</pre>
 */
@Data
public class TaskResponseDto {

    /**
     * Unique identifier of the task.
     */
    private String taskId;

    /**
     * Current status of the task (e.g., QUEUED, RUNNING, COMPLETED, FAILED).
     */
    private String status;

    /**
     * Position of the task in the execution queue.
     * May be {@code null} if the task is not queued or queue position is irrelevant.
     */
    private Integer queuePosition;

    /**
     * Optional message providing additional information about the task.
     * Could be a success message, error description, or progress note.
     */
    private String message;

    /**
     * Constructs a task response without a queue position.
     *
     * @param taskId  the unique ID of the task
     * @param status  the current status of the task
     * @param message additional information about the task
     */
    public TaskResponseDto(String taskId, String status, String message) {
        this.taskId = taskId;
        this.status = status;
        this.message = message;
    }

    /**
     * Constructs a task response including the queue position.
     *
     * @param taskId        the unique ID of the task
     * @param status        the current status of the task
     * @param queuePosition the task's position in the queue
     * @param message       additional information about the task
     */
    public TaskResponseDto(String taskId, String status, Integer queuePosition, String message) {
        this.taskId = taskId;
        this.status = status;
        this.queuePosition = queuePosition;
        this.message = message;
    }
}
