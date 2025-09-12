package org.load.execution.runner.core.processor;

import org.load.execution.runner.api.dto.TaskDto;

public interface ValidatableTaskProcessor {
    /**
     * Validates task data before processing.
     * @param task the task to validate
     * @throws IllegalArgumentException if validation fails
     */
    void validateTask(TaskDto task) throws IllegalArgumentException;
}