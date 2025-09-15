package org.load.execution.runner.core.history;

import org.load.execution.runner.api.dto.TaskDto;
import org.load.execution.runner.api.dto.TaskExecution;
import org.load.execution.runner.config.TaskQueueConfig;
import org.load.execution.runner.core.model.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
public class TaskHistoryService {
    private static final Logger logger = LoggerFactory.getLogger(TaskHistoryService.class);
    
    private final TaskQueueConfig config;
    private final ConcurrentHashMap<String, TaskExecution> taskHistory = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LocalDateTime> activeTaskIds = new ConcurrentHashMap<>();
    
    public TaskHistoryService(TaskQueueConfig config) {
        this.config = config;
    }
    
    public void recordQueued(TaskDto task, int queuePosition) {
        String taskId = task.getTaskId();
        activeTaskIds.put(taskId, LocalDateTime.now());
        
        TaskExecution execution = new TaskExecution(
            taskId, task.getTaskType(), TaskStatus.QUEUED,
            LocalDateTime.now(), null, null, 0, null, queuePosition
        );
        taskHistory.put(taskId, execution);
        logger.info("Task {} queued at position {}", taskId, queuePosition);
    }
    
    public void markProcessing(String taskId) {
        LocalDateTime startTime = LocalDateTime.now();
        updateTaskHistory(taskId, TaskStatus.PROCESSING, startTime, null, 0, null);
        logger.info("STARTED processing task: {}", taskId);
    }
    
    public void markCompleted(String taskId, long processingTimeMs) {
        LocalDateTime completedTime = LocalDateTime.now();
        updateTaskHistory(taskId, TaskStatus.COMPLETED, null, completedTime, processingTimeMs, null);
        activeTaskIds.remove(taskId);
        logger.info("COMPLETED task: {} in {}ms", taskId, processingTimeMs);
    }

    public void markFailed(String taskId, long processingTimeMs, String errorMessage) {
        LocalDateTime completedTime = LocalDateTime.now();

        // Fix: Handle null error message safely
        String safeErrorMessage = errorMessage != null ? errorMessage : "Unknown error";
        TaskStatus status = safeErrorMessage.contains("timeout") ? TaskStatus.TIMEOUT : TaskStatus.FAILED;

        updateTaskHistory(taskId, status, null, completedTime, processingTimeMs, safeErrorMessage);
        activeTaskIds.remove(taskId);
        logger.error("FAILED task: {} after {}ms - Error: {}", taskId, processingTimeMs, safeErrorMessage);
    }

    public void markCancelled(String taskId, long processingTimeMs, String reason) {
        LocalDateTime completedTime = LocalDateTime.now();
        updateTaskHistory(taskId, TaskStatus.CANCELLED, null, completedTime, processingTimeMs, reason);
        activeTaskIds.remove(taskId);
        logger.info("CANCELLED task: {} after {}ms - Reason: {}", taskId, processingTimeMs, reason);  // Changed WARN to INFO
    }
    
    public boolean isTaskActive(String taskId) {
        return activeTaskIds.containsKey(taskId);
    }
    
    public TaskExecution getTaskExecution(String taskId) {
        return taskHistory.get(taskId);
    }
    
    public List<TaskExecution> getTaskHistory(int limit) {
        return taskHistory.values().stream()
            .sorted((a, b) -> b.getQueuedAt().compareTo(a.getQueuedAt()))
            .limit(limit)
            .collect(Collectors.toList());
    }
    
    public List<TaskExecution> getTasksByStatus(TaskStatus status, int limit) {
        return taskHistory.values().stream()
            .filter(task -> task.getStatus() == status)
            .sorted((a, b) -> b.getQueuedAt().compareTo(a.getQueuedAt()))
            .limit(limit)
            .collect(Collectors.toList());
    }
    
    public Map<TaskStatus, Long> getStatusBreakdown() {
        return taskHistory.values().stream()
            .collect(Collectors.groupingBy(TaskExecution::getStatus, Collectors.counting()));
    }
    
    public int getActiveTaskCount() {
        return activeTaskIds.size();
    }
    
    public int getHistorySize() {
        return taskHistory.size();
    }

    private void updateTaskHistory(String taskId, TaskStatus status,
                                   LocalDateTime startedAt, LocalDateTime completedAt,
                                   long processingTimeMs, String errorMessage) {
        taskHistory.computeIfPresent(taskId, (key, existing) ->
                new TaskExecution(
                        taskId, existing.getTaskType(), status, existing.getQueuedAt(),
                        startedAt != null ? startedAt : existing.getStartedAt(),
                        completedAt, processingTimeMs, errorMessage, existing.getQueuePosition()
                )
        );
    }
    
    @Scheduled(fixedRate = 3600000) // Every hour
    public void cleanupTaskHistory() {
        LocalDateTime cutoff = LocalDateTime.now().minusHours(config.getTaskHistoryRetentionHours());
        
        taskHistory.entrySet().removeIf(entry -> {
            TaskExecution execution = entry.getValue();
            LocalDateTime completedAt = execution.getCompletedAt();
            return completedAt != null && completedAt.isBefore(cutoff);
        });
        
        logger.debug("Task history cleanup completed. Remaining entries: {}", taskHistory.size());
    }
    
    private boolean isTaskFinished(TaskStatus status) {
        return status == TaskStatus.COMPLETED || 
               status == TaskStatus.FAILED || 
               status == TaskStatus.CANCELLED || 
               status == TaskStatus.TIMEOUT;
    }
}