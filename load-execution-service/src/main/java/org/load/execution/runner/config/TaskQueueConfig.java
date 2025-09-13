package org.load.execution.runner.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "task.queue")
public class TaskQueueConfig {
    private int maxQueueSize = 10000;
    private int maxTaskAgeHours = 24;
    private int maxTaskIdLength = 255;
    private int shutdownTimeoutSeconds = 30;
    private int taskHistoryRetentionHours = 168; // 7 days
    private long taskTimeoutMinutes = 60;
    private int maxConcurrentTasks = 1; // For single-task guarantee, but configurable
    private boolean drainQueueOnShutdown = true;

}