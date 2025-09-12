package org.load.execution.runner;

public enum TaskStatus {
    QUEUED,
    PROCESSING, 
    COMPLETED,
    FAILED,
    CANCELLED,
    TIMEOUT
}
