package org.load.execution.runner.core.queue;

import lombok.Data;
import lombok.Getter;
import org.load.execution.runner.api.dto.TaskDto;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * <p>This class wraps a {@link TaskDto} with additional runtime information required for
 * task queue processing, including timing metadata and cancellation state. It serves as
 * the internal representation of tasks within the queue processing system.
 *
 */
@Data
class TaskWrapper {

    /** The original task being processed - immutable after construction */
    private final TaskDto task;

    /** Timestamp when this task was queued - immutable after construction */
    private final LocalDateTime queuedAt;

    /**
     * Future representing the actual task execution.
     * Volatile to ensure proper visibility across threads when set.
     * Can be null if execution hasn't started yet.
     */
    private volatile Future<Void> executionFuture;

    /**
     * Atomic flag indicating if this task has been cancelled.
     */
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    /**
     * Creates a new TaskWrapper for the specified task.
     *
     * <p>The wrapper captures the current timestamp as the queue time, which is used
     * for metrics, timeouts, and debugging purposes.
     *
     * @param task the task to wrap, must not be null
     * @throws NullPointerException if task is null
     */
    TaskWrapper(TaskDto task) {
        if (task == null) {
            throw new NullPointerException("Task cannot be null");
        }
        this.task = task;
        this.queuedAt = LocalDateTime.now();
    }

    /**
     * Attempts to cancel this task execution using thread-safe atomic operations.
     * @return true if THIS call successfully cancelled the task, false if already cancelled or cancellation failed
     */
    boolean cancel() {
        // Atomically set cancelled flag - only the first thread to call this succeeds
        // This prevents race conditions where multiple threads try to cancel simultaneously
        boolean andSet = cancelled.compareAndSet(false, true);

        if (!andSet) {
            // Another thread already set cancelled=true, so this call is redundant
            return false;
        }

        // We successfully set cancelled=true, now attempt to cancel the execution future
        // Note: executionFuture might be null if execution hasn't started yet
        if (executionFuture != null) {
            // Attempt to interrupt the running task
            // Parameter 'true' means interrupt the thread if it's currently running
            return executionFuture.cancel(true);
        }

        // No execution future yet, but cancellation flag is set
        // When setExecutionFuture() is called later, it will immediately cancel the future
        return true;
    }

    /**
     * Checks if this task has been cancelled.
     * @return true if the task has been cancelled, false otherwise
     */
    boolean isCancelled() {
        return cancelled.get();
    }

    /**
     * Sets the execution future for this task wrapper.
     * @param future the execution future to associate with this task, may be null
     */
    void setExecutionFuture(Future<Void> future) {
        this.executionFuture = future;
        if (cancelled.get() && future != null) {
            // Cancel with interruption to stop execution
            future.cancel(true);
        }
    }

    /**
     * Returns the task ID for logging and debugging purposes.
     *
     * @return the task ID from the wrapped task
     */
    String getTaskId() {
        return task != null ? task.getTaskId() : "unknown";
    }

    /**
     * Checks if the execution future exists and is done (completed, cancelled, or failed).
     *
     * @return true if execution future exists and is done, false otherwise
     */
    boolean isExecutionComplete() {
        return executionFuture != null && executionFuture.isDone();
    }

}