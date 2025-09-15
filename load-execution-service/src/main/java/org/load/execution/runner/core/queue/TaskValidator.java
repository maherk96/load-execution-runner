package org.load.execution.runner.core.queue;

import lombok.Data;
import org.load.execution.runner.api.dto.TaskDto;
import org.load.execution.runner.api.dto.TaskExecution;
import org.load.execution.runner.api.exception.DuplicateTaskException;
import org.load.execution.runner.api.exception.InvalidTaskException;
import org.load.execution.runner.api.exception.NoProcessorException;
import org.load.execution.runner.api.exception.NoProcessorsException;
import org.load.execution.runner.api.exception.QueueCapacityException;
import org.load.execution.runner.api.exception.ServiceShutdownException;
import org.load.execution.runner.api.exception.TaskValidationException;
import org.load.execution.runner.config.TaskQueueConfig;
import org.load.execution.runner.core.history.TaskHistoryService;
import org.load.execution.runner.core.model.TaskType;
import org.load.execution.runner.core.processor.TaskProcessorManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

@Data
@Component
public class TaskValidator {
    private static final Logger logger = LoggerFactory.getLogger(TaskValidator.class);
    
    private final TaskQueueConfig config;
    private final TaskProcessorManager processorManager;
    private final TaskHistoryService historyService;
    private volatile boolean isShuttingDown = false;
    
    public TaskValidator(TaskQueueConfig config, TaskProcessorManager processorManager, TaskHistoryService historyService) {
        this.config = config;
        this.processorManager = processorManager;
        this.historyService = historyService;
    }
    

    public void validateServiceState() {
        if (isShuttingDown) {
            logger.warn("Task submission rejected - service is shutting down");
            throw new ServiceShutdownException("Service is shutting down");
        }
        
        if (!processorManager.hasProcessors()) {
            logger.error("Task submission rejected - no processors available");
            throw new NoProcessorsException("No task processors are available");
        }
    }
    
    public void validateTask(TaskDto task) {
        if (task == null) {
            logger.warn("Task submission rejected - task is null");
            throw new InvalidTaskException("Task cannot be null");
        }
        
        String taskId = task.getTaskId();
        if (taskId == null || taskId.trim().isEmpty()) {
            logger.warn("Task submission rejected - missing task ID");
            throw new InvalidTaskException("Task ID is required");
        }
        
        if (taskId.length() > config.getMaxTaskIdLength()) {
            logger.warn("Task submission rejected - task ID too long: {}", taskId.length());
            throw new InvalidTaskException(String.format("Task ID too long (max %d characters)", config.getMaxTaskIdLength()));
        }
        
        if (task.getTaskType() == null) {
            logger.warn("Task {} rejected - missing task type", taskId);
            throw new InvalidTaskException("Task type is required");
        }

        if (task.getTaskId() == null) {
            logger.warn("Task {} rejected - missing task ID", taskId);
            throw new InvalidTaskException("Task ID is required");
        }
        
        if (task.getCreatedAt() == null) {
            logger.warn("Task {} rejected - missing creation timestamp", taskId);
            throw new InvalidTaskException("Task creation timestamp is required");
        }
        
        // Check if task is too old
        LocalDateTime createdAt = task.getCreatedAt();
        long hoursOld = ChronoUnit.HOURS.between(createdAt, LocalDateTime.now());
        if (hoursOld > config.getMaxTaskAgeHours()) {
            logger.warn("Task {} rejected - too old: {} hours", taskId, hoursOld);
            throw new InvalidTaskException(String.format("Task too old (%d hours). Maximum age is %d hours.", 
                hoursOld, config.getMaxTaskAgeHours()));
        }
        
        if (task.getData() == null) {
            logger.warn("Task {} rejected - data map is null", taskId);
            throw new InvalidTaskException("Task data cannot be null");
        }
    }
    
    public void checkDuplicate(TaskDto task) {
        String taskId = task.getTaskId();
        
        if (historyService.isTaskActive(taskId)) {
            logger.warn("Task {} rejected - duplicate task ID (still active)", taskId);
            throw new DuplicateTaskException("Task with this ID is already active");
        }
        
        TaskExecution recentExecution = historyService.getTaskExecution(taskId);
        if (recentExecution != null && recentExecution.getCompletedAt() != null) {
            long hoursAgo = ChronoUnit.HOURS.between(recentExecution.getCompletedAt(), LocalDateTime.now());
            if (hoursAgo < 1) {
                logger.warn("Task {} rejected - duplicate task completed {} hours ago", taskId, hoursAgo);
                throw new DuplicateTaskException(String.format("Task with this ID was completed %.1f hours ago", hoursAgo / 60.0));
            }
        }
    }
    
    public void validateBusinessRules(TaskDto task, int currentQueueSize) {
        TaskType taskType = task.getTaskType();
        String taskId = task.getTaskId();
        
        if (!processorManager.hasProcessor(taskType)) {
            logger.warn("Task {} rejected - no processor for type: {}", taskId, taskType);
            throw new NoProcessorException(String.format("No processor available for task type: %s. Available types: %s",
                taskType, processorManager.getAvailableTaskTypes()));
        }
        
        if (currentQueueSize >= config.getMaxQueueSize()) {
            logger.warn("Task {} rejected - queue capacity exceeded: {}", taskId, currentQueueSize);
            throw new QueueCapacityException(String.format("Queue capacity exceeded (%d tasks). Please try again later.",
                currentQueueSize));
        }
        
        // Custom validation per task type
        try {
            processorManager.validateTask(task);
        } catch (IllegalArgumentException e) {
            logger.warn("Task {} rejected - processor validation failed: {}", taskId, e.getMessage());
            throw new TaskValidationException("Task validation failed: " + e.getMessage());
        }
    }
}
