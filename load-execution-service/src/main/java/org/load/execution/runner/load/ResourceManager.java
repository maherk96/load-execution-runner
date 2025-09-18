package org.load.execution.runner.load;// File: org/load/execution/runner/load/resource/ResourceManager.java

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Enhanced resource manager with system monitoring, event publishing, and graceful shutdown handling.
 *
 * <p>This class manages the lifecycle of execution resources including virtual thread executors
 * for main workload execution and scheduled thread pools for timing operations. It provides
 * resource monitoring, task tracking, system metrics collection, and robust cleanup mechanisms
 * to ensure proper resource management during load test execution and cancellation scenarios.</p>
 *
 * @author Load Test Framework
 * @since 1.0
 */
public class ResourceManager {

    private static final Logger log = LoggerFactory.getLogger(ResourceManager.class);

    private static final int DEFAULT_SCHEDULER_THREADS = 4;
    private static final int MIN_SCHEDULER_THREADS = 2;
    private static final int MAX_SCHEDULER_THREADS = 16;
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 10;
    private static final int FORCE_SHUTDOWN_TIMEOUT_SECONDS = 3;

    private final String testId;
    private final SolacePublisher solacePublisher;
    private final ExecutorService mainExecutor;
    private final ScheduledExecutorService schedulerService;
    private final ResourceConfig config;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final AtomicInteger activeTaskCount = new AtomicInteger(0);

    private ScheduledExecutorService resourceMonitor;

    /**
     * Creates a new ResourceManager with default configuration.
     *
     * @param testId the test ID for event correlation
     * @param solacePublisher the publisher for resource metrics events
     */
    public ResourceManager(String testId, SolacePublisher solacePublisher) {
        this(testId, solacePublisher, ResourceConfig.defaultConfig());
    }

    /**
     * Creates a new ResourceManager with custom configuration.
     *
     * @param testId the test ID for event correlation
     * @param solacePublisher the publisher for resource metrics events
     * @param config the resource configuration
     */
    public ResourceManager(String testId, SolacePublisher solacePublisher, ResourceConfig config) {
        this.testId = testId;
        this.solacePublisher = solacePublisher;
        this.config = config;
        this.mainExecutor = createMainExecutor();
        this.schedulerService = createSchedulerExecutor();

        log.info("ResourceManager initialized - Main: {}, Scheduler threads: {}",
                getExecutorInfo(mainExecutor), config.getSchedulerThreads());

        // Start resource monitoring
        startResourceMonitoring();
    }

    /**
     * Starts periodic resource monitoring and publishing.
     */
    private void startResourceMonitoring() {
        resourceMonitor = Executors.newSingleThreadScheduledExecutor(
                r -> createNamedThread(r, "ResourceMonitor")
        );
        resourceMonitor.scheduleWithFixedDelay(this::publishResourceMetrics, 60, 60, TimeUnit.SECONDS);
        log.debug("Resource monitoring started - publishing metrics every 60 seconds");
    }

    /**
     * Collects and publishes current system resource metrics.
     */
    private void publishResourceMetrics() {
        try {
            var runtime = Runtime.getRuntime();
            var memoryMX = ManagementFactory.getMemoryMXBean();
            var osMX = ManagementFactory.getOperatingSystemMXBean();

            long memoryUsed = memoryMX.getHeapMemoryUsage().getUsed();
            double cpuLoad = osMX.getSystemLoadAverage() * 100;
            if (cpuLoad < 0) cpuLoad = 0; // Handle case where CPU load is not available

            int activeThreads = Thread.activeCount();

            var event = new ResourceMetricsEvent(
                    testId,
                    Instant.now(),
                    memoryUsed,
                    cpuLoad,
                    activeThreads,
                    getQueueSizes(),
                    getNetworkConnections()
            );

            solacePublisher.publishResourceMetrics(event);

            log.debug("Published resource metrics: Memory={}MB, CPU={}%, Threads={}",
                    memoryUsed / 1024 / 1024, (int) cpuLoad, activeThreads);

        } catch (Exception e) {
            log.debug("Error publishing resource metrics: {}", e.getMessage());
        }
    }

    /**
     * Calculates total queue sizes across all thread pool executors.
     *
     * @return total number of queued tasks
     */
    private int getQueueSizes() {
        int queueSize = 0;
        if (mainExecutor instanceof ThreadPoolExecutor tpe) {
            queueSize += tpe.getQueue().size();
        }
        if (schedulerService instanceof ThreadPoolExecutor tpe) {
            queueSize += tpe.getQueue().size();
        }
        return queueSize;
    }

    /**
     * Gets an estimate of network connections (currently uses active tasks as proxy).
     *
     * @return estimated network connection count
     */
    private int getNetworkConnections() {
        // Placeholder - could be enhanced with actual network monitoring
        // For load testing, active tasks is a reasonable proxy for network connections
        return activeTaskCount.get();
    }

    /**
     * Creates the main executor service, preferring virtual threads if available.
     *
     * @return the main executor service
     */
    private ExecutorService createMainExecutor() {
        try {
            return Executors.newVirtualThreadPerTaskExecutor();
        } catch (Exception e) {
            log.warn("Failed to create virtual thread executor, falling back to cached thread pool: {}", e.getMessage());
            return Executors.newCachedThreadPool(this::createNamedThread);
        }
    }

    /**
     * Creates the scheduled executor service for timing operations.
     *
     * @return the scheduled executor service
     */
    private ScheduledExecutorService createSchedulerExecutor() {
        int threads = calculateSchedulerThreads();
        return Executors.newScheduledThreadPool(threads, r -> createNamedThread(r, "LoadTest-Scheduler"));
    }

    /**
     * Calculates optimal number of scheduler threads based on available processors.
     *
     * @return number of scheduler threads to use
     */
    private int calculateSchedulerThreads() {
        int cpuThreads = Runtime.getRuntime().availableProcessors();
        int calculated = Math.max(MIN_SCHEDULER_THREADS, cpuThreads / 2);
        return Math.min(calculated, MAX_SCHEDULER_THREADS);
    }

    /**
     * Creates a named thread with default worker naming.
     *
     * @param r the runnable task
     * @return new thread with appropriate name and configuration
     */
    private Thread createNamedThread(Runnable r) {
        return createNamedThread(r, "LoadTest-Worker");
    }

    /**
     * Creates a named thread with custom prefix and proper configuration.
     *
     * @param r the runnable task
     * @param prefix the thread name prefix
     * @return new thread with appropriate name and configuration
     */
    private Thread createNamedThread(Runnable r, String prefix) {
        Thread thread = new Thread(r, prefix + "-" + System.currentTimeMillis());
        thread.setDaemon(false);
        thread.setUncaughtExceptionHandler(this::handleUncaughtException);
        return thread;
    }

    /**
     * Handles uncaught exceptions from managed threads.
     *
     * @param thread the thread that threw the exception
     * @param exception the uncaught exception
     */
    private void handleUncaughtException(Thread thread, Throwable exception) {
        log.error("Uncaught exception in thread {}: {}", thread.getName(), exception.getMessage(), exception);
    }

    /**
     * Gets the main executor service for task submission.
     *
     * @return the main executor service
     * @throws IllegalStateException if the resource manager has been shut down
     */
    public ExecutorService getMainExecutor() {
        checkNotShutdown();
        return mainExecutor;
    }

    /**
     * Gets the scheduler service for scheduled task execution.
     *
     * @return the scheduled executor service
     * @throws IllegalStateException if the resource manager has been shut down
     */
    public ScheduledExecutorService getSchedulerService() {
        checkNotShutdown();
        return schedulerService;
    }

    /**
     * Submits a task with automatic tracking for graceful shutdown.
     *
     * @param task the task to submit
     * @return CompletableFuture representing the task execution
     * @throws IllegalStateException if the resource manager has been shut down
     */
    public CompletableFuture<Void> submitTrackedTask(Runnable task) {
        checkNotShutdown();
        activeTaskCount.incrementAndGet();

        return CompletableFuture.runAsync(() -> {
            try {
                task.run();
            } finally {
                activeTaskCount.decrementAndGet();
            }
        }, mainExecutor);
    }

    /**
     * Gets current resource usage metrics.
     *
     * @return current resource metrics snapshot
     */
    public ResourceMetrics getMetrics() {
        return new ResourceMetrics(
                getExecutorInfo(mainExecutor),
                getExecutorInfo(schedulerService),
                activeTaskCount.get(),
                isShutdown.get()
        );
    }

    /**
     * Gets formatted information about an executor service.
     *
     * @param executor the executor to inspect
     * @return formatted executor information string
     */
    private String getExecutorInfo(ExecutorService executor) {
        if (executor instanceof ThreadPoolExecutor tpe) {
            return String.format("ThreadPool[active=%d, pool=%d, queue=%d]",
                    tpe.getActiveCount(), tpe.getPoolSize(), tpe.getQueue().size());
        }
        return executor.getClass().getSimpleName();
    }

    /**
     * Performs comprehensive cleanup of all managed resources.
     * This method is idempotent and safe to call multiple times.
     */
    public void cleanup() {
        if (!isShutdown.compareAndSet(false, true)) {
            log.debug("ResourceManager already shut down");
            return;
        }

        log.info("Starting ResourceManager cleanup...");

        try {
            // Stop resource monitoring first
            if (resourceMonitor != null && !resourceMonitor.isShutdown()) {
                resourceMonitor.shutdown();
                try {
                    if (!resourceMonitor.awaitTermination(5, TimeUnit.SECONDS)) {
                        resourceMonitor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    resourceMonitor.shutdownNow();
                }
                log.debug("Resource monitoring stopped");
            }

            // Wait briefly for active tasks to complete naturally
            waitForActiveTasksToComplete(Duration.ofSeconds(2));

            // Shutdown executors gracefully
            shutdownExecutorGracefully("Scheduler", schedulerService);
            shutdownExecutorGracefully("Main Executor", mainExecutor);

            log.info("ResourceManager cleanup completed successfully");

        } catch (Exception e) {
            log.error("Error during ResourceManager cleanup", e);
        }
    }

    /**
     * Waits for active tasks to complete within the specified timeout.
     *
     * @param timeout maximum time to wait
     */
    private void waitForActiveTasksToComplete(Duration timeout) {
        if (activeTaskCount.get() == 0) return;

        log.debug("Waiting up to {}s for {} active tasks to complete",
                timeout.getSeconds(), activeTaskCount.get());

        long endTime = System.currentTimeMillis() + timeout.toMillis();
        while (activeTaskCount.get() > 0 && System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        if (activeTaskCount.get() > 0) {
            log.debug("Still {} active tasks after timeout", activeTaskCount.get());
        }
    }

    /**
     * Attempts to shutdown an executor gracefully, with fallback to forced shutdown.
     *
     * @param name descriptive name for logging
     * @param executor the executor to shutdown
     */
    private void shutdownExecutorGracefully(String name, ExecutorService executor) {
        if (executor.isShutdown()) {
            log.debug("{} executor already shut down", name);
            return;
        }

        try {
            log.debug("Shutting down {} executor...", name);
            executor.shutdown();

            // Wait for graceful shutdown
            if (executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.debug("{} executor shut down gracefully", name);
                return;
            }

            // Force shutdown if graceful shutdown timed out
            log.debug("{} executor did not terminate within {}s, initiating forced shutdown",
                    name, SHUTDOWN_TIMEOUT_SECONDS);

            executor.shutdownNow();

            // Final wait for forced shutdown
            if (executor.awaitTermination(FORCE_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.debug("{} executor terminated after forced shutdown", name);
            } else {
                log.warn("{} executor did not terminate even after forced shutdown", name);
            }

        } catch (InterruptedException e) {
            // This is expected during cancellation - don't log as WARNING
            log.info("{} executor shutdown interrupted - performing immediate shutdown", name);
            Thread.currentThread().interrupt();
            executor.shutdownNow();

        } catch (Exception e) {
            log.error("Unexpected error shutting down {} executor", name, e);
            forceShutdownSafely(executor, name);
        }
    }

    /**
     * Safely attempts to force shutdown an executor, handling any exceptions.
     *
     * @param executor the executor to force shutdown
     * @param name descriptive name for logging
     */
    private void forceShutdownSafely(ExecutorService executor, String name) {
        try {
            executor.shutdownNow();
        } catch (Exception shutdownException) {
            log.error("Failed to force shutdown {} executor", name, shutdownException);
        }
    }

    /**
     * Checks if the resource manager has been shut down and throws if so.
     *
     * @throws IllegalStateException if shut down
     */
    private void checkNotShutdown() {
        if (isShutdown.get()) {
            throw new IllegalStateException("ResourceManager has been shut down");
        }
    }

    /**
     * Configuration class for ResourceManager behavior.
     */
    public static class ResourceConfig {
        private final int schedulerThreads;
        private final Duration shutdownTimeout;
        private final boolean enableMetrics;

        /**
         * Creates a new ResourceConfig with the specified parameters.
         *
         * @param schedulerThreads number of scheduler threads (clamped to valid range)
         * @param shutdownTimeout timeout for graceful shutdown
         * @param enableMetrics whether to enable metrics collection
         */
        public ResourceConfig(int schedulerThreads, Duration shutdownTimeout, boolean enableMetrics) {
            this.schedulerThreads = Math.max(MIN_SCHEDULER_THREADS,
                    Math.min(schedulerThreads, MAX_SCHEDULER_THREADS));
            this.shutdownTimeout = shutdownTimeout;
            this.enableMetrics = enableMetrics;
        }

        /**
         * Creates a default ResourceConfig with reasonable defaults.
         *
         * @return default configuration
         */
        public static ResourceConfig defaultConfig() {
            return new ResourceConfig(DEFAULT_SCHEDULER_THREADS,
                    Duration.ofSeconds(SHUTDOWN_TIMEOUT_SECONDS), true);
        }

        /**
         * Creates a custom ResourceConfig with specified scheduler threads.
         *
         * @param schedulerThreads number of scheduler threads
         * @return custom configuration with default timeout and metrics enabled
         */
        public static ResourceConfig customConfig(int schedulerThreads) {
            return new ResourceConfig(schedulerThreads,
                    Duration.ofSeconds(SHUTDOWN_TIMEOUT_SECONDS), true);
        }

        public int getSchedulerThreads() { return schedulerThreads; }
        public Duration getShutdownTimeout() { return shutdownTimeout; }
        public boolean isMetricsEnabled() { return enableMetrics; }
    }

    /**
     * Record containing current resource usage metrics.
     */
    public record ResourceMetrics(
            String mainExecutorInfo,
            String schedulerExecutorInfo,
            int activeTaskCount,
            boolean isShutdown
    ) {
        @Override
        public String toString() {
            return String.format("ResourceMetrics[main=%s, scheduler=%s, active=%d, shutdown=%b]",
                    mainExecutorInfo, schedulerExecutorInfo, activeTaskCount, isShutdown);
        }
    }
}