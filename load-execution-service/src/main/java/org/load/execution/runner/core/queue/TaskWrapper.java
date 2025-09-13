package org.load.execution.runner.core.queue;

import lombok.Data;
import lombok.Getter;
import org.load.execution.runner.api.dto.TaskDto;

import java.time.LocalDateTime;
import java.util.concurrent.Future;

@Data
class TaskWrapper {

    private final TaskDto task;
    private final LocalDateTime queuedAt;
    private volatile Future<Void> executionFuture;
    private volatile boolean cancelled = false;

    TaskWrapper(TaskDto task) {
        this.task = task;
        this.queuedAt = LocalDateTime.now();
    }

    /**
     * Cancel this task execution
     * @return true if cancellation was successful
     */
    boolean cancel() {
        if (cancelled) {
            return executionFuture == null || executionFuture.isCancelled();
        }

        if (executionFuture != null) {
            boolean success = executionFuture.cancel(true);
            if (success) {
                cancelled = true;
            }
            return success;
        }

        cancelled = true;
        return true;
    }
}