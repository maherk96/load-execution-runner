package org.load.execution.runner.api.exception;

public class TaskCancelledException extends Exception {
    private final String reason;
    
    public TaskCancelledException(String reason) {
        super("Task was cancelled: " + reason);
        this.reason = reason;
    }
    
    public String getReason() {
        return reason;
    }
}