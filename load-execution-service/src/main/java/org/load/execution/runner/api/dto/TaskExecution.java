package org.load.execution.runner.api.dto;

import lombok.Data;
import org.load.execution.runner.core.model.TaskStatus;
import org.load.execution.runner.core.model.TaskType;

import java.time.LocalDateTime;

@Data
public class TaskExecution {
    private final String taskId;
    private final TaskType taskType;
    private final TaskStatus status;
    private final LocalDateTime queuedAt;
    private final LocalDateTime startedAt;
    private final LocalDateTime completedAt;
    private final long processingTimeMs;
    private final String errorMessage;
    private final int queuePosition;
    
    // Constructor and getters
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
    
    // Getters
    public String getTaskId() { return taskId; }
    public TaskType getTaskType() { return taskType; }
    public TaskStatus getStatus() { return status; }
    public LocalDateTime getQueuedAt() { return queuedAt; }
    public LocalDateTime getStartedAt() { return startedAt; }
    public LocalDateTime getCompletedAt() { return completedAt; }
    public long getProcessingTimeMs() { return processingTimeMs; }
    public String getErrorMessage() { return errorMessage; }
    public int getQueuePosition() { return queuePosition; }
}
