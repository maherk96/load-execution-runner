// ===== RESOURCE MANAGEMENT =====
// File: org/load/execution/runner/load/resource/ResourceManager.java
package org.load.execution.runner.load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Enhanced resource manager with better configuration, monitoring, and graceful shutdown handling.
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

    private final ExecutorService mainExecutor;
    private final ScheduledExecutorService schedulerService;
    private final ResourceConfig config;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final AtomicInteger activeTaskCount = new AtomicInteger(0);

    public ResourceManager() {
        this(ResourceConfig.defaultConfig());
    }

    public ResourceManager(ResourceConfig config) {
        this.config = config;
        this.mainExecutor = createMainExecutor();
        this.schedulerService = createSchedulerExecutor();

        log.info("ResourceManager initialized - Main: {}, Scheduler threads: {}",
                getExecutorInfo(mainExecutor), config.getSchedulerThreads());
    }

    private ExecutorService createMainExecutor() {
        try {
            return Executors.newVirtualThreadPerTaskExecutor();
        } catch (Exception e) {
            log.warn("Failed to create virtual thread executor, falling back to cached thread pool: {}", e.getMessage());
            return Executors.newCachedThreadPool(this::createNamedThread);
        }
    }

    private ScheduledExecutorService createSchedulerExecutor() {
        int threads = calculateSchedulerThreads();
        return Executors.newScheduledThreadPool(threads, r -> createNamedThread(r, "LoadTest-Scheduler"));
    }

    private int calculateSchedulerThreads() {
        int cpuThreads = Runtime.getRuntime().availableProcessors();
        int calculated = Math.max(MIN_SCHEDULER_THREADS, cpuThreads / 2);
        return Math.min(calculated, MAX_SCHEDULER_THREADS);
    }

    private Thread createNamedThread(Runnable r) {
        return createNamedThread(r, "LoadTest-Worker");
    }

    private Thread createNamedThread(Runnable r, String prefix) {
        Thread thread = new Thread(r, prefix + "-" + System.currentTimeMillis());
        thread.setDaemon(false);
        thread.setUncaughtExceptionHandler(this::handleUncaughtException);
        return thread;
    }

    private void handleUncaughtException(Thread thread, Throwable exception) {
        log.error("Uncaught exception in thread {}: {}", thread.getName(), exception.getMessage(), exception);
    }

    public ExecutorService getMainExecutor() {
        checkNotShutdown();
        return mainExecutor;
    }

    public ScheduledExecutorService getSchedulerService() {
        checkNotShutdown();
        return schedulerService;
    }

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

    public ResourceMetrics getMetrics() {
        return new ResourceMetrics(
                getExecutorInfo(mainExecutor),
                getExecutorInfo(schedulerService),
                activeTaskCount.get(),
                isShutdown.get()
        );
    }

    private String getExecutorInfo(ExecutorService executor) {
        if (executor instanceof ThreadPoolExecutor tpe) {
            return String.format("ThreadPool[active=%d, pool=%d, queue=%d]",
                    tpe.getActiveCount(), tpe.getPoolSize(), tpe.getQueue().size());
        }
        return executor.getClass().getSimpleName();
    }

    public void cleanup() {
        if (!isShutdown.compareAndSet(false, true)) {
            log.debug("ResourceManager already shut down");
            return;
        }

        log.info("Starting ResourceManager cleanup...");

        try {
            waitForActiveTasksToComplete(Duration.ofSeconds(2));
            shutdownExecutorGracefully("Scheduler", schedulerService);
            shutdownExecutorGracefully("Main Executor", mainExecutor);
            log.info("ResourceManager cleanup completed successfully");
        } catch (Exception e) {
            log.error("Error during ResourceManager cleanup", e);
        }
    }

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

    private void shutdownExecutorGracefully(String name, ExecutorService executor) {
        if (executor.isShutdown()) {
            log.debug("{} executor already shut down", name);
            return;
        }

        try {
            log.debug("Shutting down {} executor...", name);
            executor.shutdown();

            if (executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.debug("{} executor shut down gracefully", name);
                return;
            }

            log.debug("{} executor did not terminate within {}s, initiating forced shutdown",
                    name, SHUTDOWN_TIMEOUT_SECONDS);

            executor.shutdownNow();

            if (executor.awaitTermination(FORCE_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.debug("{} executor terminated after forced shutdown", name);
            } else {
                log.warn("{} executor did not terminate even after forced shutdown", name);
            }

        } catch (InterruptedException e) {
            log.info("{} executor shutdown interrupted - performing immediate shutdown", name);
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        } catch (Exception e) {
            log.error("Unexpected error shutting down {} executor", name, e);
            forceShutdownSafely(executor, name);
        }
    }

    private void forceShutdownSafely(ExecutorService executor, String name) {
        try {
            executor.shutdownNow();
        } catch (Exception shutdownException) {
            log.error("Failed to force shutdown {} executor", name, shutdownException);
        }
    }

    private void checkNotShutdown() {
        if (isShutdown.get()) {
            throw new IllegalStateException("ResourceManager has been shut down");
        }
    }

    public static class ResourceConfig {
        private final int schedulerThreads;
        private final Duration shutdownTimeout;
        private final boolean enableMetrics;

        public ResourceConfig(int schedulerThreads, Duration shutdownTimeout, boolean enableMetrics) {
            this.schedulerThreads = Math.max(MIN_SCHEDULER_THREADS,
                    Math.min(schedulerThreads, MAX_SCHEDULER_THREADS));
            this.shutdownTimeout = shutdownTimeout;
            this.enableMetrics = enableMetrics;
        }

        public static ResourceConfig defaultConfig() {
            return new ResourceConfig(DEFAULT_SCHEDULER_THREADS,
                    Duration.ofSeconds(SHUTDOWN_TIMEOUT_SECONDS), true);
        }

        public int getSchedulerThreads() { return schedulerThreads; }
        public Duration getShutdownTimeout() { return shutdownTimeout; }
        public boolean isMetricsEnabled() { return enableMetrics; }
    }

    public record ResourceMetrics(
            String mainExecutorInfo,
            String schedulerExecutorInfo,
            int activeTaskCount,
            boolean isShutdown
    ) {}
}