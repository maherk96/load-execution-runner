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

@Component
public class TaskProcessorManager {
    private static final Logger logger = LoggerFactory.getLogger(TaskProcessorManager.class);

    private final TaskQueueConfig config;
    private final Map<TaskType, TaskProcessor> taskProcessors = new ConcurrentHashMap<>();

    public TaskProcessorManager(TaskQueueConfig config, List<TaskProcessor> processors) {
        this.config = config;
        registerTaskProcessors(processors);
    }

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

    private void logRegisteredProcessors() {
        logger.info("=== Registered Task Processors ===");
        taskProcessors.forEach((type, processor) ->
                logger.info("  {} -> {} {}", type, processor.getClass().getSimpleName(),
                        processor instanceof InterruptibleTaskProcessor ? "(Interruptible)" : ""));
        logger.info("=== Total: {} processors ===", taskProcessors.size());
    }

    public boolean hasProcessors() {
        return !taskProcessors.isEmpty();
    }

    public boolean hasProcessor(TaskType taskType) {
        return taskProcessors.containsKey(taskType);
    }

    public TaskProcessor getProcessor(TaskType taskType) {
        return taskProcessors.get(taskType);
    }

    public Set<TaskType> getAvailableTaskTypes() {
        return new HashSet<>(taskProcessors.keySet());
    }

    public String getAvailableTaskTypesString() {
        return taskProcessors.keySet().stream()
                .map(Enum::name)
                .sorted()
                .collect(Collectors.joining(", "));
    }

    public void validateTask(TaskDto task) {
        TaskProcessor processor = taskProcessors.get(task.getTaskType());
        if (processor instanceof ValidatableTaskProcessor) {
            ((ValidatableTaskProcessor) processor).validateTask(task);
        }
    }

    public void processTask(TaskDto task) throws Exception {
        TaskProcessor processor = taskProcessors.get(task.getTaskType());
        if (processor == null) {
            throw new IllegalStateException("Processor not found for type: " + task.getTaskType());
        }

        processor.processTask(task);
    }
}