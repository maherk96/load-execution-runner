package org.load.execution.runner;

import java.time.LocalDateTime;
import java.util.concurrent.Future;

class TaskWrapper {
    private final TaskDto task;
    private final LocalDateTime queuedAt;
    private volatile Future<Void> executionFuture;
    private volatile boolean cancelled = false;

    TaskWrapper(TaskDto task) {
        this.task = task;
        this.queuedAt = LocalDateTime.now();
    }

    public TaskDto getTask() {
        return task;
    }

    public LocalDateTime getQueuedAt() {
        return queuedAt;
    }

    void setExecutionFuture(Future<Void> future) {
        this.executionFuture = future;
    }

    /**
     * Cancel this task execution
     * @return true if cancellation was successful
     */
    boolean cancel() {
        cancelled = true;
        if (executionFuture != null) {
            return executionFuture.cancel(true); // Interrupt if running
        }
        return true;
    }

    boolean isCancelled() {
        return cancelled;
    }
}