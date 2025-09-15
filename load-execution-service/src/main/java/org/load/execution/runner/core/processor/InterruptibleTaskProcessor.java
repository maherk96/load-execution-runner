package org.load.execution.runner.core.processor;

import org.load.execution.runner.api.dto.TaskDto;

/**
 * Extension of {@link TaskProcessor} that supports task interruption and cancellation.
 * <p>
 * Implementations of this interface are capable of responding to cancellation requests,
 * allowing tasks to gracefully stop execution, release resources, and avoid leaving the
 * system in an inconsistent state.
 * </p>
 *
 * <p>This is particularly useful for long-running or blocking tasks where cancellation
 * should be cooperative rather than abrupt termination.</p>
 */
public interface InterruptibleTaskProcessor extends TaskProcessor {

    /**
     * Called when a running task should be cancelled.
     * <p>
     * Implementations should use this callback to:
     * <ul>
     *   <li>Clean up allocated resources (threads, I/O streams, database connections, etc.)</li>
     *   <li>Interrupt background work if possible</li>
     *   <li>Mark internal state so that the task stops processing gracefully</li>
     * </ul>
     *
     * <p>The default implementation does nothing, allowing subclasses to override
     * only when cancellation logic is necessary.</p>
     *
     * @param task the {@link TaskDto} representing the task to cancel
     */
    default void cancelTask(TaskDto task) {
        // Default implementation - subclasses can override for cleanup
    }
}
