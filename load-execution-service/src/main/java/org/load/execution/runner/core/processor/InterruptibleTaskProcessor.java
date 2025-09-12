package org.load.execution.runner.core.processor;

import org.load.execution.runner.api.dto.TaskDto;

public interface InterruptibleTaskProcessor extends TaskProcessor {
    /**
     * Called when task should be cancelled.
     * Implementations should clean up resources and prepare for interruption.
     */
    default void cancelTask(TaskDto task) {
        // Default implementation - subclasses can override for cleanup
    }
}