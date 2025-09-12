package org.load.execution.runner.core.model;

public enum TaskStatus {
    QUEUED,
    PROCESSING, 
    COMPLETED,
    FAILED,
    CANCELLED,
    TIMEOUT
}
