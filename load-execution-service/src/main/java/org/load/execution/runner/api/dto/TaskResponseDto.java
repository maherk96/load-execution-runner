package org.load.execution.runner.api.dto;


public class TaskResponseDto {
    private String taskId;
    private String status;
    private Integer queuePosition;
    private String message;

    public TaskResponseDto(String taskId, String status, String message) {
        this.taskId = taskId;
        this.status = status;
        this.message = message;
    }

    public TaskResponseDto(String taskId, String status, Integer queuePosition, String message) {
        this.taskId = taskId;
        this.status = status;
        this.queuePosition = queuePosition;
        this.message = message;
    }

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Integer getQueuePosition() { return queuePosition; }
    public void setQueuePosition(Integer queuePosition) { this.queuePosition = queuePosition; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
}