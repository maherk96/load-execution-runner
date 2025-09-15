package org.load.execution.runner.core.processor;

import org.load.execution.runner.api.dto.TaskDto;
import org.load.execution.runner.config.TaskQueueConfig;
import org.load.execution.runner.core.model.TaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Central manager responsible for registering and delegating tasks to their appropriate {@link TaskProcessor}.
 * <p>
 * This class acts as a registry and dispatcher:
 * <ul>
 *   <li>Registers all available {@link TaskProcessor} beans on application startup</li>
 *   <li>Provides lookup by {@link TaskType} to find the correct processor</li>
 *   <li>Supports validation and processing of tasks by delegating to the proper processor</li>
 *   <li>Logs all registered processors for easier troubleshooting</li>
 * </ul>
 *
 */
@Component
public class TaskProcessorManager {
    private static final Logger logger = LoggerFactory.getLogger(TaskProcessorManager.class);

    private final TaskQueueConfig config;
    private final Map<TaskType, TaskProcessor> taskProcessors = new ConcurrentHashMap<>();

    /**
     * Constructs the manager and registers all available task processors.
     *
     * @param config     the task queue configuration
     * @param processors a list of discovered {@link TaskProcessor} beans
     */
    public TaskProcessorManager(TaskQueueConfig config, List<TaskProcessor> processors) {
        this.config = config;
        registerTaskProcessors(processors);
    }

    /**
     * Registers all discovered task processors, mapping them by their {@link TaskType}.
     * Logs all registered processors for visibility.
     *
     * @param processors the list of processors to register
     */
    private void registerTaskProcessors(List<TaskProcessor> processors) {
        if (processors == null || processors.isEmpty()) {
            logger.warn("No task processors found! Service will reject all tasks.");
            return;
        }

        for (TaskProcessor processor : processors) {
            try {
                TaskType taskType = processor.getTaskType();
                taskProcessors.put(taskType, processor);
                logger.info("Registered processor for task type: {} (Interruptible: {})",
                        taskType, processor instanceof InterruptibleTaskProcessor);
            } catch (Exception e) {
                logger.error("Failed to register processor: {}",
                        processor.getClass().getSimpleName(), e);
            }
        }

        logRegisteredProcessors();
    }

    /**
     * Logs all registered processors with their associated task types.
     */
    private void logRegisteredProcessors() {
        logger.info("=== Registered Task Processors ===");
        taskProcessors.forEach((type, processor) ->
                logger.info("  {} -> {} {}", type, processor.getClass().getSimpleName(),
                        processor instanceof InterruptibleTaskProcessor ? "(Interruptible)" : ""));
        logger.info("=== Total: {} processors ===", taskProcessors.size());
    }

    /**
     * @return {@code true} if there are any registered processors
     */
    public boolean hasProcessors() {
        return !taskProcessors.isEmpty();
    }

    /**
     * Checks if there is a registered processor for a given {@link TaskType}.
     *
     * @param taskType the task type to check
     * @return {@code true} if a processor exists for this type
     */
    public boolean hasProcessor(TaskType taskType) {
        return taskProcessors.containsKey(taskType);
    }

    /**
     * Retrieves the processor registered for the given task type.
     *
     * @param taskType the type of task
     * @return the matching {@link TaskProcessor}, or {@code null} if none is registered
     */
    public TaskProcessor getProcessor(TaskType taskType) {
        return taskProcessors.get(taskType);
    }

    /**
     * @return a set of all available task types that currently have processors
     */
    public Set<TaskType> getAvailableTaskTypes() {
        return new HashSet<>(taskProcessors.keySet());
    }

    /**
     * @return a comma-separated string of all available task types (sorted alphabetically)
     */
    public String getAvailableTaskTypesString() {
        return taskProcessors.keySet().stream()
                .map(Enum::name)
                .sorted()
                .collect(Collectors.joining(", "));
    }

    /**
     * Validates a task if its associated processor supports validation
     * (i.e. implements {@link ValidatableTaskProcessor}).
     *
     * @param task the task to validate
     * @throws IllegalArgumentException if validation fails
     */
    public void validateTask(TaskDto task) {
        TaskProcessor processor = taskProcessors.get(task.getTaskType());
        if (processor instanceof ValidatableTaskProcessor) {
            ((ValidatableTaskProcessor) processor).validateTask(task);
        }
    }

    /**
     * Processes a task by delegating to the appropriate processor.
     *
     * @param task the task to process
     * @throws Exception if the processor fails to execute the task
     */
    public void processTask(TaskDto task) throws Exception {
        TaskProcessor processor = taskProcessors.get(task.getTaskType());
        if (processor == null) {
            throw new IllegalStateException("Processor not found for type: " + task.getTaskType());
        }

        processor.processTask(task);
    }
}
