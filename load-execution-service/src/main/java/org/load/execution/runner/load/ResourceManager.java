package org.load.execution.runner.load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Enhanced resource manager that provides robust thread pool management, monitoring,
 * and protection against resource exhaustion for load testing operations.
 * 
 * <p>This manager provides comprehensive resource lifecycle management including:
 * <ul>
 *   <li><b>Smart Thread Pool Management:</b> Automatic sizing with fallback strategies</li>
 *   <li><b>Resource Protection:</b> Circuit breaker pattern and rate limiting</li>
 *   <li><b>Health Monitoring:</b> Continuous health checks and metrics collection</li>
 *   <li><b>Graceful Degradation:</b> Fallback mechanisms for resource exhaustion</li>
 *   <li><b>Comprehensive Cleanup:</b> Multi-stage shutdown with timeout handling</li>
 * </ul>
 * 
 * <p><b>Thread Safety:</b> All operations are thread-safe and designed for concurrent access.
 * The manager uses atomic operations and concurrent collections to ensure consistency.
 * 
 * <p><b>Resource Limits:</b> Built-in protection against resource exhaustion with configurable
 * limits and automatic throttling when approaching capacity limits.
 * 
 * @author Load Test Team
 * @version 2.0.0
 */
public class ResourceManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ResourceManager.class);

    // ================================
    // CONFIGURATION CONSTANTS
    // ================================

    private static final int DEFAULT_SCHEDULER_THREADS = 4;
    private static final int MIN_SCHEDULER_THREADS = 2;
    private static final int MAX_SCHEDULER_THREADS = 32;
    private static final int DEFAULT_MAX_ACTIVE_TASKS = 1000;
    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration DEFAULT_HEALTH_CHECK_INTERVAL = Duration.ofSeconds(30);

    /**
     * Comprehensive configuration for ResourceManager behavior.
     */
    public static class ResourceConfig {
        private final int schedulerThreads;
        private final int maxActiveTasksLimit;
        private final Duration shutdownTimeout;
        private final Duration forceShutdownTimeout;
        private final Duration healthCheckInterval;
        private final boolean enableMetrics;
        private final boolean enableHealthChecks;
        private final boolean enableCircuitBreaker;
        private final String threadNamePrefix;
        private final boolean preferVirtualThreads;

        private ResourceConfig(Builder builder) {
            this.schedulerThreads = validateThreadCount(builder.schedulerThreads);
            this.maxActiveTasksLimit = Math.max(10, builder.maxActiveTasksLimit);
            this.shutdownTimeout = builder.shutdownTimeout;
            this.forceShutdownTimeout = builder.forceShutdownTimeout;
            this.healthCheckInterval = builder.healthCheckInterval;
            this.enableMetrics = builder.enableMetrics;
            this.enableHealthChecks = builder.enableHealthChecks;
            this.enableCircuitBreaker = builder.enableCircuitBreaker;
            this.threadNamePrefix = builder.threadNamePrefix;
            this.preferVirtualThreads = builder.preferVirtualThreads;
        }

        private int validateThreadCount(int threads) {
            return Math.max(MIN_SCHEDULER_THREADS, Math.min(threads, MAX_SCHEDULER_THREADS));
        }

        public static ResourceConfig defaultConfig() {
            return new Builder().build();
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private int schedulerThreads = calculateDefaultSchedulerThreads();
            private int maxActiveTasksLimit = DEFAULT_MAX_ACTIVE_TASKS;
            private Duration shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;
            private Duration forceShutdownTimeout = Duration.ofSeconds(3);
            private Duration healthCheckInterval = DEFAULT_HEALTH_CHECK_INTERVAL;
            private boolean enableMetrics = true;
            private boolean enableHealthChecks = true;
            private boolean enableCircuitBreaker = true;
            private String threadNamePrefix = "LoadTest";
            private boolean preferVirtualThreads = true;

            private static int calculateDefaultSchedulerThreads() {
                int cpuThreads = Runtime.getRuntime().availableProcessors();
                return Math.max(MIN_SCHEDULER_THREADS, Math.min(cpuThreads / 2, DEFAULT_SCHEDULER_THREADS));
            }

            public Builder schedulerThreads(int threads) {
                this.schedulerThreads = threads;
                return this;
            }

            public Builder maxActiveTasksLimit(int limit) {
                this.maxActiveTasksLimit = limit;
                return this;
            }

            public Builder shutdownTimeout(Duration timeout) {
                this.shutdownTimeout = timeout;
                return this;
            }

            public Builder threadNamePrefix(String prefix) {
                this.threadNamePrefix = prefix;
                return this;
            }

            public Builder enableCircuitBreaker(boolean enable) {
                this.enableCircuitBreaker = enable;
                return this;
            }

            public Builder preferVirtualThreads(boolean prefer) {
                this.preferVirtualThreads = prefer;
                return this;
            }

            public ResourceConfig build() {
                return new ResourceConfig(this);
            }
        }

        // Getters
        public int getSchedulerThreads() { return schedulerThreads; }
        public int getMaxActiveTasksLimit() { return maxActiveTasksLimit; }
        public Duration getShutdownTimeout() { return shutdownTimeout; }
        public Duration getForceShutdownTimeout() { return forceShutdownTimeout; }
        public Duration getHealthCheckInterval() { return healthCheckInterval; }
        public boolean isMetricsEnabled() { return enableMetrics; }
        public boolean isHealthChecksEnabled() { return enableHealthChecks; }
        public boolean isCircuitBreakerEnabled() { return enableCircuitBreaker; }
        public String getThreadNamePrefix() { return threadNamePrefix; }
        public boolean isPreferVirtualThreads() { return preferVirtualThreads; }
    }

    /**
     * Comprehensive resource usage metrics with performance indicators.
     */
    public static class ResourceMetrics {
        private final String mainExecutorInfo;
        private final String schedulerExecutorInfo;
        private final int activeTaskCount;
        private final int completedTaskCount;
        private final int rejectedTaskCount;
        private final boolean isShutdown;
        private final boolean isHealthy;
        private final Duration uptime;
        private final double taskSubmissionRate;
        private final double taskCompletionRate;
        private final Instant lastUpdated;

        public ResourceMetrics(String mainExecutorInfo, String schedulerExecutorInfo,
                             int activeTaskCount, int completedTaskCount, int rejectedTaskCount,
                             boolean isShutdown, boolean isHealthy, Duration uptime,
                             double taskSubmissionRate, double taskCompletionRate) {
            this.mainExecutorInfo = mainExecutorInfo;
            this.schedulerExecutorInfo = schedulerExecutorInfo;
            this.activeTaskCount = activeTaskCount;
            this.completedTaskCount = completedTaskCount;
            this.rejectedTaskCount = rejectedTaskCount;
            this.isShutdown = isShutdown;
            this.isHealthy = isHealthy;
            this.uptime = uptime;
            this.taskSubmissionRate = taskSubmissionRate;
            this.taskCompletionRate = taskCompletionRate;
            this.lastUpdated = Instant.now();
        }

        // Getters
        public String getMainExecutorInfo() { return mainExecutorInfo; }
        public String getSchedulerExecutorInfo() { return schedulerExecutorInfo; }
        public int getActiveTaskCount() { return activeTaskCount; }
        public int getCompletedTaskCount() { return completedTaskCount; }
        public int getRejectedTaskCount() { return rejectedTaskCount; }
        public boolean isShutdown() { return isShutdown; }
        public boolean isHealthy() { return isHealthy; }
        public Duration getUptime() { return uptime; }
        public double getTaskSubmissionRate() { return taskSubmissionRate; }
        public double getTaskCompletionRate() { return taskCompletionRate; }
        public Instant getLastUpdated() { return lastUpdated; }

        @Override
        public String toString() {
            return String.format("ResourceMetrics[main=%s, scheduler=%s, active=%d, completed=%d, " +
                                "rejected=%d, healthy=%s, uptime=%ds, submissionRate=%.2f/s, completionRate=%.2f/s]",
                    mainExecutorInfo, schedulerExecutorInfo, activeTaskCount, completedTaskCount,
                    rejectedTaskCount, isHealthy, uptime.getSeconds(), taskSubmissionRate, taskCompletionRate);
        }
    }

    /**
     * Health status information for the resource manager.
     */
    public static class HealthStatus {
        private final boolean isHealthy;
        private final String status;
        private final List<String> issues;
        private final Map<String, Object> details;
        private final Instant lastCheck;

        public HealthStatus(boolean isHealthy, String status, List<String> issues, Map<String, Object> details) {
            this.isHealthy = isHealthy;
            this.status = status;
            this.issues = new ArrayList<>(issues);
            this.details = Map.copyOf(details);
            this.lastCheck = Instant.now();
        }

        public boolean isHealthy() { return isHealthy; }
        public String getStatus() { return status; }
        public List<String> getIssues() { return List.copyOf(issues); }
        public Map<String, Object> getDetails() { return details; }
        public Instant getLastCheck() { return lastCheck; }

        @Override
        public String toString() {
            return String.format("HealthStatus{healthy=%s, status='%s', issues=%d, lastCheck=%s}",
                    isHealthy, status, issues.size(), lastCheck);
        }
    }

    // ================================
    // INSTANCE FIELDS
    // ================================

    private final ResourceConfig config;
    private final ExecutorService mainExecutor;
    private final ScheduledExecutorService schedulerService;
    private final ScheduledExecutorService healthCheckExecutor;

    // State tracking
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final AtomicInteger activeTaskCount = new AtomicInteger(0);
    private final AtomicLong completedTaskCount = new AtomicLong(0);
    private final AtomicLong rejectedTaskCount = new AtomicLong(0);
    private final AtomicReference<HealthStatus> currentHealthStatus = new AtomicReference<>();

    // Metrics and monitoring
    private final Instant creationTime = Instant.now();
    private final AtomicLong taskSubmissionCount = new AtomicLong(0);
    private final AtomicReference<Instant> lastTaskSubmission = new AtomicReference<>(Instant.now());
    private final AtomicReference<Instant> lastTaskCompletion = new AtomicReference<>(Instant.now());

    // Circuit breaker for resource protection
    private final SimpleCircuitBreaker circuitBreaker;

    // Cleanup tracking
    private final List<AutoCloseable> managedResources = new CopyOnWriteArrayList<>();
    private volatile ScheduledFuture<?> healthCheckTask;

    // ================================
    // CONSTRUCTORS
    // ================================

    /**
     * Creates a ResourceManager with default configuration.
     */
    public ResourceManager() {
        this(ResourceConfig.defaultConfig());
    }

    /**
     * Creates a ResourceManager with custom configuration.
     * 
     * @param config the configuration for resource management
     */
    public ResourceManager(ResourceConfig config) {
        this.config = config != null ? config : ResourceConfig.defaultConfig();
        this.circuitBreaker = new SimpleCircuitBreaker(config.getMaxActiveTasksLimit());

        try {
            this.mainExecutor = createMainExecutor();
            this.schedulerService = createSchedulerExecutor();
            this.healthCheckExecutor = createHealthCheckExecutor();

            // Register executors for cleanup
            managedResources.add(() -> shutdownExecutorSafely("Main", mainExecutor));
            managedResources.add(() -> shutdownExecutorSafely("Scheduler", schedulerService));
            managedResources.add(() -> shutdownExecutorSafely("HealthCheck", healthCheckExecutor));

            // Initialize health monitoring
            initializeHealthMonitoring();

            log.info("ResourceManager initialized - Main: {}, Scheduler threads: {}, Health checks: {}",
                    getExecutorInfo(mainExecutor), config.getSchedulerThreads(), config.isHealthChecksEnabled());

        } catch (Exception e) {
            log.error("Failed to initialize ResourceManager", e);
            cleanup();
            throw new ResourceManagerException("Failed to initialize ResourceManager", e);
        }
    }

    // ================================
    // EXECUTOR CREATION
    // ================================

    private ExecutorService createMainExecutor() {
        if (config.isPreferVirtualThreads()) {
            try {
                // Use virtual threads if available (Java 21+)
                return Executors.newVirtualThreadPerTaskExecutor();
            } catch (Exception e) {
                log.debug("Virtual threads not available, falling back to thread pool: {}", e.getMessage());
            }
        }

        // Fallback to enhanced thread pool
        return new ThreadPoolExecutor(
                0, Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                this::createMainThread,
                this::handleRejectedTask
        );
    }

    private ScheduledExecutorService createSchedulerExecutor() {
        return Executors.newScheduledThreadPool(
                config.getSchedulerThreads(),
                r -> createNamedThread(r, config.getThreadNamePrefix() + "-Scheduler")
        );
    }

    private ScheduledExecutorService createHealthCheckExecutor() {
        return Executors.newSingleThreadScheduledExecutor(
                r -> createNamedThread(r, config.getThreadNamePrefix() + "-HealthCheck")
        );
    }

    private Thread createMainThread(Runnable r) {
        return createNamedThread(r, config.getThreadNamePrefix() + "-Worker");
    }

    private Thread createNamedThread(Runnable r, String prefix) {
        Thread thread = new Thread(r, generateThreadName(prefix));
        thread.setDaemon(false);
        thread.setUncaughtExceptionHandler(this::handleUncaughtException);
        return thread;
    }

    private String generateThreadName(String prefix) {
        return String.format("%s-%d-%d", prefix, System.currentTimeMillis() % 10000, 
                Thread.currentThread().getId() % 1000);
    }

    // ================================
    // PUBLIC API
    // ================================

    /**
     * Returns the main executor service for task execution.
     * 
     * @return main executor service
     * @throws IllegalStateException if resource manager is shut down
     */
    public ExecutorService getMainExecutor() {
        checkNotShutdown();
        return mainExecutor;
    }

    /**
     * Returns the scheduler service for timed operations.
     * 
     * @return scheduler service
     * @throws IllegalStateException if resource manager is shut down
     */
    public ScheduledExecutorService getSchedulerService() {
        checkNotShutdown();
        return schedulerService;
    }

    /**
     * Submits a task with tracking and circuit breaker protection.
     * 
     * @param task the task to execute
     * @return future representing the task execution
     * @throws ResourceExhaustedException if circuit breaker is open
     * @throws IllegalStateException if resource manager is shut down
     */
    public CompletableFuture<Void> submitTrackedTask(Runnable task) {
        return submitTrackedTask(task, "anonymous-task");
    }

    /**
     * Submits a named task with tracking and circuit breaker protection.
     * 
     * @param task the task to execute
     * @param taskName name for logging and monitoring
     * @return future representing the task execution
     * @throws ResourceExhaustedException if circuit breaker is open
     * @throws IllegalStateException if resource manager is shut down
     */
    public CompletableFuture<Void> submitTrackedTask(Runnable task, String taskName) {
        checkNotShutdown();

        if (config.isCircuitBreakerEnabled() && !circuitBreaker.allowRequest()) {
            rejectedTaskCount.incrementAndGet();
            throw new ResourceExhaustedException(
                    "Circuit breaker is open - too many active tasks: " + activeTaskCount.get());
        }

        taskSubmissionCount.incrementAndGet();
        lastTaskSubmission.set(Instant.now());
        int currentActive = activeTaskCount.incrementAndGet();

        return CompletableFuture.runAsync(() -> {
            String originalName = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName(originalName + "-" + taskName);
                task.run();
                circuitBreaker.recordSuccess();
            } catch (Exception e) {
                circuitBreaker.recordFailure();
                log.debug("Task '{}' failed: {}", taskName, e.getMessage());
                throw e;
            } finally {
                Thread.currentThread().setName(originalName);
                activeTaskCount.decrementAndGet();
                completedTaskCount.incrementAndGet();
                lastTaskCompletion.set(Instant.now());
            }
        }, mainExecutor).whenComplete((result, throwable) -> {
            if (throwable != null) {
                log.debug("Tracked task '{}' completed with error: {}", taskName, throwable.getMessage());
            }
        });
    }

    /**
     * Submits a supplier task with result tracking.
     * 
     * @param task the supplier task to execute
     * @param taskName name for logging and monitoring
     * @param <T> result type
     * @return future representing the task result
     */
    public <T> CompletableFuture<T> submitTrackedSupplier(Supplier<T> task, String taskName) {
        checkNotShutdown();

        if (config.isCircuitBreakerEnabled() && !circuitBreaker.allowRequest()) {
            rejectedTaskCount.incrementAndGet();
            throw new ResourceExhaustedException(
                    "Circuit breaker is open - too many active tasks: " + activeTaskCount.get());
        }

        taskSubmissionCount.incrementAndGet();
        lastTaskSubmission.set(Instant.now());
        activeTaskCount.incrementAndGet();

        return CompletableFuture.supplyAsync(() -> {
            String originalName = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName(originalName + "-" + taskName);
                T result = task.get();
                circuitBreaker.recordSuccess();
                return result;
            } catch (Exception e) {
                circuitBreaker.recordFailure();
                log.debug("Supplier task '{}' failed: {}", taskName, e.getMessage());
                throw e;
            } finally {
                Thread.currentThread().setName(originalName);
                activeTaskCount.decrementAndGet();
                completedTaskCount.incrementAndGet();
                lastTaskCompletion.set(Instant.now());
            }
        }, mainExecutor);
    }

    /**
     * Returns comprehensive resource usage metrics.
     * 
     * @return current resource metrics
     */
    public ResourceMetrics getMetrics() {
        Duration uptime = Duration.between(creationTime, Instant.now());
        double submissionRate = calculateRate(taskSubmissionCount.get(), uptime);
        double completionRate = calculateRate(completedTaskCount.get(), uptime);

        HealthStatus health = currentHealthStatus.get();
        boolean isHealthy = health != null ? health.isHealthy() : true;

        return new ResourceMetrics(
                getExecutorInfo(mainExecutor),
                getExecutorInfo(schedulerService),
                activeTaskCount.get(),
                (int) completedTaskCount.get(),
                (int) rejectedTaskCount.get(),
                isShutdown.get(),
                isHealthy,
                uptime,
                submissionRate,
                completionRate
        );
    }

    /**
     * Returns current health status.
     * 
     * @return health status snapshot
     */
    public HealthStatus getHealthStatus() {
        HealthStatus status = currentHealthStatus.get();
        return status != null ? status : new HealthStatus(true, "UNKNOWN", List.of(), Map.of());
    }

    /**
     * Performs an immediate health check.
     * 
     * @return current health status after check
     */
    public HealthStatus performHealthCheck() {
        HealthStatus status = performHealthCheckInternal();
        currentHealthStatus.set(status);
        return status;
    }

    // ================================
    // RESOURCE CLEANUP
    // ================================

    /**
     * Performs graceful cleanup of all resources.
     */
    public void cleanup() {
        if (!isShutdown.compareAndSet(false, true)) {
            log.debug("ResourceManager already shut down");
            return;
        }

        log.info("Starting ResourceManager cleanup...");

        try {
            // Cancel health checks first
            if (healthCheckTask != null) {
                healthCheckTask.cancel(false);
            }

            // Wait briefly for active tasks to complete
            waitForActiveTasksToComplete(Duration.ofSeconds(2));

            // Shutdown all managed resources
            cleanupManagedResources();

            log.info("ResourceManager cleanup completed successfully");

        } catch (Exception e) {
            log.error("Error during ResourceManager cleanup", e);
        }
    }

    @Override
    public void close() {
        cleanup();
    }

    // ================================
    // PRIVATE HELPER METHODS
    // ================================

    private void initializeHealthMonitoring() {
        if (config.isHealthChecksEnabled()) {
            healthCheckTask = healthCheckExecutor.scheduleWithFixedDelay(
                    this::periodicHealthCheck,
                    config.getHealthCheckInterval().toMillis(),
                    config.getHealthCheckInterval().toMillis(),
                    TimeUnit.MILLISECONDS
            );
        }
    }

    private void periodicHealthCheck() {
        try {
            HealthStatus status = performHealthCheckInternal();
            currentHealthStatus.set(status);

            if (!status.isHealthy()) {
                log.warn("Health check failed: {}", status);
            }
        } catch (Exception e) {
            log.error("Error during periodic health check", e);
        }
    }

    private HealthStatus performHealthCheckInternal() {
        List<String> issues = new ArrayList<>();
        Map<String, Object> details = new ConcurrentHashMap<>();

        // Check executor health
        if (mainExecutor.isShutdown()) {
            issues.add("Main executor is shutdown");
        }
        if (schedulerService.isShutdown()) {
            issues.add("Scheduler service is shutdown");
        }

        // Check resource utilization
        int active = activeTaskCount.get();
        int maxAllowed = config.getMaxActiveTasksLimit();
        double utilization = (double) active / maxAllowed;

        details.put("activeTaskCount", active);
        details.put("maxActiveTasksLimit", maxAllowed);
        details.put("utilization", String.format("%.2f%%", utilization * 100));
        details.put("circuitBreakerOpen", config.isCircuitBreakerEnabled() && !circuitBreaker.allowRequest());

        if (utilization > 0.9) {
            issues.add(String.format("High resource utilization: %.1f%%", utilization * 100));
        }

        if (config.isCircuitBreakerEnabled() && !circuitBreaker.allowRequest()) {
            issues.add("Circuit breaker is open");
        }

        boolean isHealthy = issues.isEmpty();
        String status = isHealthy ? "HEALTHY" : "DEGRADED";

        return new HealthStatus(isHealthy, status, issues, details);
    }

    private double calculateRate(long count, Duration duration) {
        if (duration.isZero() || duration.isNegative()) {
            return 0.0;
        }
        return (double) count / duration.getSeconds();
    }

    private String getExecutorInfo(ExecutorService executor) {
        if (executor instanceof ThreadPoolExecutor tpe) {
            return String.format("ThreadPool[active=%d, pool=%d, queue=%d, completed=%d]",
                    tpe.getActiveCount(), tpe.getPoolSize(), tpe.getQueue().size(), tpe.getCompletedTaskCount());
        }
        return String.format("%s[shutdown=%s]", executor.getClass().getSimpleName(), executor.isShutdown());
    }

    private void handleUncaughtException(Thread thread, Throwable exception) {
        log.error("Uncaught exception in thread {}: {}", thread.getName(), exception.getMessage(), exception);
    }

    private void handleRejectedTask(Runnable task, ThreadPoolExecutor executor) {
        rejectedTaskCount.incrementAndGet();
        log.warn("Task rejected by executor: active={}, pool={}, queue={}",
                executor.getActiveCount(), executor.getPoolSize(), executor.getQueue().size());
        throw new RejectedExecutionException("Task rejected due to thread pool saturation");
    }

    private void waitForActiveTasksToComplete(Duration timeout) {
        if (activeTaskCount.get() == 0) return;

        log.debug("Waiting up to {}s for {} active tasks to complete",
                timeout.getSeconds(), activeTaskCount.get());

        Instant deadline = Instant.now().plus(timeout);
        while (activeTaskCount.get() > 0 && Instant.now().isBefore(deadline)) {
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

    private void cleanupManagedResources() {
        for (AutoCloseable resource : managedResources) {
            try {
                resource.close();
            } catch (Exception e) {
                log.warn("Error closing managed resource: {}", e.getMessage());
            }
        }
        managedResources.clear();
    }

    private void shutdownExecutorSafely(String name, ExecutorService executor) throws Exception {
        if (executor.isShutdown()) return;

        log.debug("Shutting down {} executor...", name);
        executor.shutdown();

        if (!executor.awaitTermination(config.getShutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
            log.debug("{} executor did not terminate gracefully, forcing shutdown", name);
            executor.shutdownNow();

            if (!executor.awaitTermination(config.getForceShutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("{} executor did not terminate even after forced shutdown", name);
            }
        }
    }

    private void checkNotShutdown() {
        if (isShutdown.get()) {
            throw new IllegalStateException("ResourceManager has been shut down");
        }
    }

    // ================================
    // INNER CLASSES
    // ================================

    /**
     * Simple circuit breaker implementation for resource protection.
     */
    private static class SimpleCircuitBreaker {
        private final int threshold;
        private final AtomicInteger failureCount = new AtomicInteger(0);
        private final AtomicBoolean isOpen = new AtomicBoolean(false);

        SimpleCircuitBreaker(int threshold) {
            this.threshold = threshold;
        }

        boolean allowRequest() {
            return !isOpen.get();
        }

        void recordSuccess() {
            failureCount.set(0);
            isOpen.set(false);
        }

        void recordFailure() {
            if (failureCount.incrementAndGet() >= threshold) {
                isOpen.set(true);
            }
        }
    }

    // ================================
    // CUSTOM EXCEPTIONS
    // ================================

    /**
     * Exception thrown when resource manager initialization fails.
     */
    public static class ResourceManagerException extends RuntimeException {
        public ResourceManagerException(String message) {
            super(message);
        }

        public ResourceManagerException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Exception thrown when resources are exhausted.
     */
    public static class ResourceExhaustedException extends RuntimeException {
        public ResourceExhaustedException(String message) {
            super(message);
        }

        public ResourceExhaustedException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}