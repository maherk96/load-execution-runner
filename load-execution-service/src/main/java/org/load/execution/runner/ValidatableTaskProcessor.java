package org.load.execution.runner;

public interface ValidatableTaskProcessor {
    /**
     * Validates task data before processing.
     * @param task the task to validate
     * @throws IllegalArgumentException if validation fails
     */
    void validateTask(TaskDto task) throws IllegalArgumentException;
}