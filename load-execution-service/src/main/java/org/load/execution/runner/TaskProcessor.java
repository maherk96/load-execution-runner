package org.load.execution.runner;

public interface TaskProcessor {
    TaskType getTaskType();

    /**
     * Process a task. Implementations MUST:
     * 1. Check Thread.currentThread().isInterrupted() periodically
     * 2. Throw InterruptedException if interrupted
     * 3. Handle cancellation gracefully
     */
    void processTask(TaskDto task) throws Exception;

}