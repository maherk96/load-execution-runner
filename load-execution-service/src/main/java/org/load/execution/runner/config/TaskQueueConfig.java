package org.load.execution.runner.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "task.queue")
public class TaskQueueConfig {
    private int maxQueueSize = 10000;
    private int maxTaskAgeHours = 24;
    private int maxTaskIdLength = 255;
    private int shutdownTimeoutSeconds = 30;
    private int taskHistoryRetentionHours = 168; // 7 days
    private long taskTimeoutMinutes = 60;
    private int maxConcurrentTasks = 2; // For single-task guarantee, but configurable
    private boolean drainQueueOnShutdown = true;

    // Getters and setters
    public int getMaxQueueSize() { return maxQueueSize; }
    public void setMaxQueueSize(int maxQueueSize) { this.maxQueueSize = maxQueueSize; }

    public int getMaxTaskAgeHours() { return maxTaskAgeHours; }
    public void setMaxTaskAgeHours(int maxTaskAgeHours) { this.maxTaskAgeHours = maxTaskAgeHours; }

    public int getMaxTaskIdLength() { return maxTaskIdLength; }
    public void setMaxTaskIdLength(int maxTaskIdLength) { this.maxTaskIdLength = maxTaskIdLength; }

    public int getShutdownTimeoutSeconds() { return shutdownTimeoutSeconds; }
    public void setShutdownTimeoutSeconds(int shutdownTimeoutSeconds) { this.shutdownTimeoutSeconds = shutdownTimeoutSeconds; }

    public int getTaskHistoryRetentionHours() { return taskHistoryRetentionHours; }
    public void setTaskHistoryRetentionHours(int taskHistoryRetentionHours) { this.taskHistoryRetentionHours = taskHistoryRetentionHours; }

    public long getTaskTimeoutMinutes() { return taskTimeoutMinutes; }
    public void setTaskTimeoutMinutes(long taskTimeoutMinutes) { this.taskTimeoutMinutes = taskTimeoutMinutes; }

    public int getMaxConcurrentTasks() { return maxConcurrentTasks; }
    public void setMaxConcurrentTasks(int maxConcurrentTasks) { this.maxConcurrentTasks = maxConcurrentTasks; }

    public boolean isDrainQueueOnShutdown() { return drainQueueOnShutdown; }
    public void setDrainQueueOnShutdown(boolean drainQueueOnShutdown) { this.drainQueueOnShutdown = drainQueueOnShutdown; }
}