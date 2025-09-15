package org.load.execution.runner.core.queue;

import lombok.Data;
import lombok.Getter;
import org.load.execution.runner.api.dto.TaskDto;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

@Data
class TaskWrapper {

    private final TaskDto task;
    private final LocalDateTime queuedAt;
    private volatile Future<Void> executionFuture;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    TaskWrapper(TaskDto task) {
        this.task = task;
        this.queuedAt = LocalDateTime.now();
    }

    /**
     * Cancel this task execution - thread-safe implementation
     * @return true if THIS call successfully cancelled the task, false if already cancelled or cancellation failed
     */
    boolean cancel() {
        // Atomically set cancelled flag - only first thread succeeds
        boolean thisCallPerformedCancellation = cancelled.compareAndSet(false, true);

        if (!thisCallPerformedCancellation) {
            // Already cancelled by another thread
            return false;
        }

        // We successfully set cancelled=true, now cancel the future if it exists
        if (executionFuture != null) {
            return executionFuture.cancel(true);
        }

        return true;
    }

    /**
     * Check if this task has been cancelled
     */
    boolean isCancelled() {
        return cancelled.get();
    }

    /**
     * Set the execution future - should only be called once
     */
    void setExecutionFuture(Future<Void> future) {
        this.executionFuture = future;

        // If already cancelled, immediately cancel the future
        if (cancelled.get() && future != null) {
            future.cancel(true);
        }
    }
}