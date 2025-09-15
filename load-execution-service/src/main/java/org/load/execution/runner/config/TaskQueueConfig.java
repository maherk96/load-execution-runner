package org.load.execution.runner.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Configuration properties for the task queue system.
 * <p>
 * This class is bound to properties with the prefix {@code task.queue}
 * from the application configuration (e.g., {@code application.yml} or {@code application.properties}).
 * <br>
 * It provides centralized control over queue capacity, task lifecycle limits,
 * and shutdown behavior.
 * </p>
 *
 * <p>Example configuration in {@code application.yml}:</p>
 * <pre>{@code
 * task:
 *   queue:
 *     max-queue-size: 5000
 *     max-task-age-hours: 12
 *     max-task-id-length: 128
 *     shutdown-timeout-seconds: 60
 *     task-history-retention-hours: 72
 *     task-timeout-minutes: 30
 *     max-concurrent-tasks: 5
 *     drain-queue-on-shutdown: false
 * }</pre>
 */
@Data
@Component
@ConfigurationProperties(prefix = "task.queue")
public class TaskQueueConfig {

    /**
     * Maximum number of tasks allowed in the queue at any given time.
     * <p>Default: {@code 10000}</p>
     */
    private int maxQueueSize = 10000;

    /**
     * Maximum age (in hours) a task can stay in the queue before being discarded.
     * <p>Default: {@code 24}</p>
     */
    private int maxTaskAgeHours = 24;

    /**
     * Maximum allowed length of a task ID string.
     * <p>Default: {@code 255}</p>
     */
    private int maxTaskIdLength = 255;

    /**
     * Timeout (in seconds) to wait for ongoing tasks to complete
     * before forcefully shutting down the queue.
     * <p>Default: {@code 30}</p>
     */
    private int shutdownTimeoutSeconds = 30;

    /**
     * Duration (in hours) to retain historical task execution records.
     * <p>Default: {@code 168} (7 days)</p>
     */
    private int taskHistoryRetentionHours = 168;

    /**
     * Maximum allowed execution time for a task (in minutes).
     * Tasks exceeding this limit may be forcefully terminated.
     * <p>Default: {@code 60}</p>
     */
    private long taskTimeoutMinutes = 60;

    /**
     * Maximum number of tasks allowed to run concurrently.
     * <p>Default: {@code 1}</p>
     */
    private int maxConcurrentTasks = 1;

    /**
     * Whether the queue should be drained (allow remaining tasks to complete)
     * when shutting down the application.
     * <p>Default: {@code true}</p>
     */
    private boolean drainQueueOnShutdown = true;
}
