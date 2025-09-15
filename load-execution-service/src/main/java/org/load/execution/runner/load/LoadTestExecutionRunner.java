// Main orchestrator class - with cancellation mechanism
package org.load.execution.runner.load;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Simplified Load Test Execution Runner that orchestrates load tests with cancellation support.
 *
 * This refactored version focuses purely on coordination and delegates
 * specific responsibilities to specialized components.
 */
public class LoadTestExecutionRunner {

    private static final Logger log = LoggerFactory.getLogger(LoadTestExecutionRunner.class);

    private final TestPlanSpec testPlanSpec;
    private final TestPhaseManager phaseManager;
    private final RequestExecutor requestExecutor;
    private final ResourceManager resourceManager;
    private final WorkloadStrategyFactory strategyFactory;

    private final AtomicReference<String> terminationReason = new AtomicReference<>();
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    /**
     * Constructs a new LoadTestExecutionRunner with the specified test plan.
     */
    public LoadTestExecutionRunner(TestPlanSpec testPlanSpec) {
        this.testPlanSpec = testPlanSpec;
        this.phaseManager = new TestPhaseManager();
        this.requestExecutor = new RequestExecutor(testPlanSpec);
        this.resourceManager = new ResourceManager();
        this.strategyFactory = new WorkloadStrategyFactory();
    }

    /**
     * Main execution method that orchestrates the entire load test.
     */
    public CompletableFuture<Void> execute() {
        return CompletableFuture.runAsync(() -> {
            try {
                log.info("Starting load test execution: {}", testPlanSpec.getTestSpec().getId());

                phaseManager.startTest();
                var testStartTime = Instant.now();

                // Create and execute workload strategy
                var strategy = strategyFactory.createStrategy(
                        testPlanSpec.getExecution().getLoadModel(),
                        phaseManager,
                        requestExecutor,
                        resourceManager,
                        cancelled
                );

                strategy.execute(testPlanSpec);

                // Wait for completion
                phaseManager.waitForCompletion();

                var testEndTime = Instant.now();
                var totalDuration = Duration.between(testStartTime, testEndTime);

                // FIX: Ensure termination reason is always set


                phaseManager.completeTest();
                log.info("Load test completed. Duration: {}s, Reason: {}",
                        totalDuration.getSeconds(), getTerminationReason());

            } catch (Exception e) {
                log.error("Load test execution failed", e);
                terminateTest("EXECUTION_ERROR: " + e.getMessage());
                throw new RuntimeException("Load test execution failed", e);
            }
        }, resourceManager.getMainExecutor());
    }

    /**
     * Cancels the load test execution gracefully.
     * This will stop further request submissions and let running requests complete.
     */
    public void cancel() {
        if (cancelled.compareAndSet(false, true)) {
            log.info("Load test cancellation requested - stopping execution gracefully");
            terminateTest("CANCELLED");
        } else {
            log.debug("Load test cancellation already in progress");
        }
    }

    /**
     * Returns true if the test has been cancelled.
     */
    public boolean isCancelled() {
        return cancelled.get();
    }

    /**
     * Terminates the test with the specified reason.
     */
    public void terminateTest(String reason) {
        terminationReason.compareAndSet(null, reason);
        phaseManager.terminateTest(reason);
    }

    public String getTerminationReason() {
        // Always check phase manager first since that's where strategies set the reason
        String reason = phaseManager.getTerminationReason();
        if (reason == null) {
            reason = terminationReason.get();
        }
        if (reason == null) {
            reason = "UNKNOWN";
        }
        return reason;
    }

    /**
     * Cleanup all resources.
     */
    public void cleanup() {
        log.info("Starting load test cleanup...");
        try {
            phaseManager.cleanup();
            requestExecutor.cleanup();
            resourceManager.cleanup();
            log.info("Load test cleanup completed successfully");
        } catch (Exception e) {
            log.error("Unexpected error during cleanup", e);
        }
    }
}

// ===== PHASE MANAGEMENT =====

/**
 * Manages test phases and state transitions.
 */
class TestPhaseManager {
    private static final Logger log = LoggerFactory.getLogger(TestPhaseManager.class);

    public enum TestPhase {
        INITIALIZING, WARMUP, RAMP_UP, HOLD, COMPLETED, TERMINATED
    }

    private final AtomicReference<TestPhase> currentPhase = new AtomicReference<>(TestPhase.INITIALIZING);
    private final AtomicBoolean testRunning = new AtomicBoolean(false);
    private final CountDownLatch testCompletionLatch = new CountDownLatch(1);
    private final AtomicReference<String> terminationReason = new AtomicReference<>(); // ADD THIS

    public void startTest() {
        testRunning.set(true);
        currentPhase.set(TestPhase.INITIALIZING);
    }

    public void setPhase(TestPhase phase) {
        currentPhase.set(phase);
        log.debug("Phase transition: {}", phase);
    }

    public TestPhase getCurrentPhase() {
        return currentPhase.get();
    }

    public boolean isTestRunning() {
        return testRunning.get();
    }

    public void terminateTest(String reason) {
        if (testRunning.compareAndSet(true, false)) {
            currentPhase.set(TestPhase.TERMINATED);
            terminationReason.set(reason); // STORE THE REASON
            testCompletionLatch.countDown();
            log.info("Test termination initiated: {}", reason);
        }
    }

    public void completeTest() {
        if (testRunning.compareAndSet(true, false)) {
            currentPhase.set(TestPhase.COMPLETED);
            // Set a default reason if none was set
            terminationReason.compareAndSet(null, "COMPLETED");
            testCompletionLatch.countDown();
        }
    }

    public String getTerminationReason() { // ADD THIS METHOD
        return terminationReason.get();
    }

    public void waitForCompletion() {
        try {
            testCompletionLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void cleanup() {
        testRunning.set(false);
    }
}
// ===== REQUEST EXECUTION =====

/**
 * Handles HTTP request execution and logging.
 */
class RequestExecutor {

    private static final Logger log = LoggerFactory.getLogger(RequestExecutor.class);

    private final LoadHttpClient httpClient;
    private final AtomicInteger activeRequests = new AtomicInteger(0);

    public RequestExecutor(TestPlanSpec testPlanSpec) {
        var globalConfig = testPlanSpec.getTestSpec().getGlobalConfig();
        this.httpClient = new LoadHttpClient(
                globalConfig.getBaseUrl(),
                globalConfig.getTimeouts().getConnectionTimeoutMs() / 1000,
                globalConfig.getHeaders(),
                globalConfig.getVars()
        );
    }

    /**
     * Executes all requests across all scenarios.
     */
    public void executeAllRequests(TestPlanSpec testPlanSpec, TestPhaseManager phaseManager,
                                   int userId, boolean isWarmup, Semaphore concurrencyLimiter,
                                   AtomicBoolean cancelled) {
        try {
            activeRequests.incrementAndGet();

            for (var scenario : testPlanSpec.getTestSpec().getScenarios()) {
                if (!phaseManager.isTestRunning() || cancelled.get()) break;

                for (var request : scenario.getRequests()) {
                    if (!phaseManager.isTestRunning() || cancelled.get()) break;
                    executeRequest(request, phaseManager.getCurrentPhase(), userId, isWarmup);
                }
            }
        } finally {
            activeRequests.decrementAndGet();
            if (concurrencyLimiter != null) {
                concurrencyLimiter.release();
            }
        }
    }

    /**
     * Executes a single HTTP request and logs the execution details.
     */
    private void executeRequest(TestPlanSpec.Request request, TestPhaseManager.TestPhase phase,
                                int userId, boolean isWarmup) {
        var requestStart = Instant.now();
        boolean success = false;
        int statusCode = 0;

        try {
            var response = httpClient.execute(request);
            var requestEnd = Instant.now();
            long responseTime = Duration.between(requestStart, requestEnd).toMillis();

            statusCode = response.getStatusCode();
            success = true;

            logRequestExecution(phase, userId, request, responseTime, false, success, statusCode, isWarmup);
            log.debug("Request completed: {} ms, status: {}, user: {}", responseTime, statusCode, userId);

        } catch (Exception e) {
            var requestEnd = Instant.now();
            long responseTime = Duration.between(requestStart, requestEnd).toMillis();

            logRequestExecution(phase, userId, request, responseTime, false, success, statusCode, isWarmup);
            log.debug("Request failed: {}, user: {}", e.getMessage(), userId);
        }
    }

    private void logRequestExecution(TestPhaseManager.TestPhase phase, int userId,
                                     TestPlanSpec.Request request, long responseTime,
                                     boolean backPressured, boolean success, int statusCode, boolean isWarmup) {
        var executionLog = new RequestExecutionLog(
                Instant.now(), phase, userId, request.getMethod().name(),
                request.getPath(), responseTime, backPressured, success, statusCode
        );

        if (!isWarmup) {
            log.info("Request execution: {}", executionLog);
        } else {
            log.debug("Warmup request execution: {}", executionLog);
        }
    }

    public void logBackPressureEvent(TestPhaseManager.TestPhase phase) {
        log.debug("Back-pressure: max concurrent requests reached");
        var backPressureLog = new RequestExecutionLog(
                Instant.now(), phase, -1, "N/A", "N/A", 0, true, false, 0
        );
        log.info("Request execution: {}", backPressureLog);
    }

    public int getActiveRequestCount() {
        return activeRequests.get();
    }

    public void cleanup() {
        try {
            httpClient.close();
        } catch (Exception e) {
            log.warn("Error closing HTTP client: {}", e.getMessage());
        }
    }

    /**
     * Record for logging detailed request execution information.
     */
    public record RequestExecutionLog(
            Instant timestamp,
            TestPhaseManager.TestPhase phase,
            int userId,
            String method,
            String path,
            long durationMs,
            boolean backPressured,
            boolean success,
            int statusCode
    ) {}
}

// ===== RESOURCE MANAGEMENT =====

/**
 * Manages executors, rate limiters, and other shared resources.
 */
/**
 * Enhanced Resource Manager with better configuration, monitoring, and graceful shutdown handling.
 */
class ResourceManager {

    private static final Logger log = LoggerFactory.getLogger(ResourceManager.class);

    // Configuration constants
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

    /**
     * Submit a task and track it for graceful shutdown
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
     * Get current resource usage metrics
     */
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

    /**
     * Enhanced cleanup with better cancellation handling
     */
    public void cleanup() {
        if (!isShutdown.compareAndSet(false, true)) {
            log.debug("ResourceManager already shut down");
            return;
        }

        log.info("Starting ResourceManager cleanup...");

        try {
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
     * Improved shutdown logic that avoids WARN logs during normal cancellation
     */
    private void shutdownExecutorGracefully(String name, ExecutorService executor) {
        if (executor.isShutdown()) {
            log.debug("{} executor already shut down", name);
            return;
        }

        try {
            log.debug("Shutting down {} executor...", name);
            executor.shutdown();

            // Wait for normal shutdown
            if (executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.debug("{} executor shut down gracefully", name);
                return;
            }

            // Force shutdown if needed, but log appropriately for cancellation vs timeout
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

    /**
     * Configuration for ResourceManager behavior
     */
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

        public static ResourceConfig customConfig(int schedulerThreads) {
            return new ResourceConfig(schedulerThreads,
                    Duration.ofSeconds(SHUTDOWN_TIMEOUT_SECONDS), true);
        }

        public int getSchedulerThreads() { return schedulerThreads; }
        public Duration getShutdownTimeout() { return shutdownTimeout; }
        public boolean isMetricsEnabled() { return enableMetrics; }
    }

    /**
     * Resource usage metrics
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
// ===== WORKLOAD STRATEGIES =====

/**
 * Factory for creating workload strategies.
 */
class WorkloadStrategyFactory {

    public WorkloadStrategy createStrategy(TestPlanSpec.LoadModel loadModel,
                                           TestPhaseManager phaseManager,
                                           RequestExecutor requestExecutor,
                                           ResourceManager resourceManager,
                                           AtomicBoolean cancelled) {
        return switch (loadModel.getType()) {
            case CLOSED -> new ClosedWorkloadStrategy(phaseManager, requestExecutor, resourceManager, cancelled);
            case OPEN -> new OpenWorkloadStrategy(phaseManager, requestExecutor, resourceManager, cancelled);
        };
    }
}

/**
 * Base interface for workload execution strategies.
 */
interface WorkloadStrategy {
    void execute(TestPlanSpec testPlanSpec);
}

/**
 * Strategy for executing CLOSED workload model with cancellation support.
 */
class ClosedWorkloadStrategy implements WorkloadStrategy {

    private static final Logger log = LoggerFactory.getLogger(ClosedWorkloadStrategy.class);

    private final TestPhaseManager phaseManager;
    private final RequestExecutor requestExecutor;
    private final ResourceManager resourceManager;
    private final TimingUtils timingUtils;
    private final AtomicBoolean cancelled;

    private CountDownLatch userCompletionLatch;
    private ScheduledFuture<?> warmupTask;
    private ScheduledFuture<?> phaseTransitionTask;
    private ScheduledFuture<?> holdTimeTask;

    public ClosedWorkloadStrategy(TestPhaseManager phaseManager, RequestExecutor requestExecutor,
                                  ResourceManager resourceManager, AtomicBoolean cancelled) {
        this.phaseManager = phaseManager;
        this.requestExecutor = requestExecutor;
        this.resourceManager = resourceManager;
        this.timingUtils = new TimingUtils();
        this.cancelled = cancelled;
    }

    @Override
    public void execute(TestPlanSpec testPlanSpec) {
        try {
            var loadModel = testPlanSpec.getExecution().getLoadModel();

            var warmupDuration = timingUtils.parseDuration(loadModel.getWarmup());
            var rampUpDuration = timingUtils.parseDuration(loadModel.getRampUp());
            var holdDuration = timingUtils.parseDuration(loadModel.getHoldFor());

            int totalUsers = loadModel.getUsers();
            int iterationsPerUser = loadModel.getIterations();

            userCompletionLatch = new CountDownLatch(totalUsers);

            log.info("Executing CLOSED workload: {} users, {} iterations per user, ramp-up: {}s, hold: {}s",
                    totalUsers, iterationsPerUser, rampUpDuration.getSeconds(), holdDuration.getSeconds());

            // Phase 1: Warmup
            if (warmupDuration.toMillis() > 0 && !cancelled.get()) {
                executeWarmup(testPlanSpec, warmupDuration);
            }

            if (!phaseManager.isTestRunning() || cancelled.get()) return;

            // Phase 2: Ramp-up and Hold
            executeRampUpAndHold(testPlanSpec, rampUpDuration, holdDuration, totalUsers, iterationsPerUser);

            // Wait for completion
            waitForUserCompletion();
        } finally {
            cancelScheduledTasks();
        }
    }

    private void executeWarmup(TestPlanSpec testPlanSpec, Duration warmupDuration) {
        phaseManager.setPhase(TestPhaseManager.TestPhase.WARMUP);
        log.info("Starting warmup phase for {} seconds", warmupDuration.getSeconds());

        var warmupEnd = Instant.now().plus(warmupDuration);
        warmupTask = resourceManager.getSchedulerService().scheduleWithFixedDelay(() -> {
            if (!phaseManager.isTestRunning() || cancelled.get() || Instant.now().isAfter(warmupEnd)) {
                return;
            }
            try {
                requestExecutor.executeAllRequests(testPlanSpec, phaseManager, -1, true, null, cancelled);
            } catch (Exception e) {
                log.debug("Warmup request failed: {}", e.getMessage());
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);

        timingUtils.sleep(warmupDuration, cancelled);

        if (warmupTask != null) {
            warmupTask.cancel(false);
        }
        log.info("Warmup phase completed");
    }

    private void executeRampUpAndHold(TestPlanSpec testPlanSpec, Duration rampUpDuration,
                                      Duration holdDuration, int totalUsers, int iterationsPerUser) {
        if (cancelled.get()) return;

        phaseManager.setPhase(TestPhaseManager.TestPhase.RAMP_UP);

        var rampUpStart = Instant.now();
        var holdStart = rampUpStart.plus(rampUpDuration);
        var testEndTime = holdStart.plus(holdDuration);

        // Schedule phase transition and termination
        schedulePhaseTransition(rampUpDuration);
        scheduleHoldTimeTermination(testEndTime);

        // Start user threads
        for (int userId = 0; userId < totalUsers; userId++) {
            if (cancelled.get()) break;

            long userStartDelay = (userId * rampUpDuration.toMillis()) / totalUsers;
            startUserThread(testPlanSpec, userId, userStartDelay, iterationsPerUser, testEndTime);
        }
    }

    private void startUserThread(TestPlanSpec testPlanSpec, int userId, long startDelay,
                                 int iterations, Instant testEndTime) {
        resourceManager.getMainExecutor().submit(() -> {
            executeUserThread(testPlanSpec, userId, startDelay, iterations, testEndTime);
        });
    }

    private void executeUserThread(TestPlanSpec testPlanSpec, int userId, long startDelay,
                                   int iterations, Instant testEndTime) {
        try {
            if (startDelay > 0 && !cancelled.get()) {
                Thread.sleep(startDelay);
            }

            if (cancelled.get()) return;

            log.debug("User {} started with {} iterations (delay: {}ms)", userId, iterations, startDelay);

            int completedIterations = 0;
            for (int i = 0; i < iterations && phaseManager.isTestRunning() && !cancelled.get(); i++) {
                if (Instant.now().isAfter(testEndTime)) {
                    log.debug("User {} stopping due to hold time expiration after {} iterations",
                            userId, completedIterations);
                    break;
                }

                requestExecutor.executeAllRequests(testPlanSpec, phaseManager, userId, false, null, cancelled);
                completedIterations++;

                if (i < iterations - 1 && !cancelled.get()) {
                    timingUtils.applyThinkTime(testPlanSpec.getExecution().getThinkTime(), cancelled);
                }
            }

            log.debug("User {} completed {} out of {} iterations", userId, completedIterations, iterations);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("User {} interrupted", userId);
        } finally {
            userCompletionLatch.countDown();
            log.debug("User {} finished", userId);
        }
    }

    private void schedulePhaseTransition(Duration rampUpDuration) {
        phaseTransitionTask = resourceManager.getSchedulerService().schedule(() -> {
            if (phaseManager.isTestRunning() && !cancelled.get()) {
                phaseManager.setPhase(TestPhaseManager.TestPhase.HOLD);
            }
        }, rampUpDuration.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void scheduleHoldTimeTermination(Instant testEndTime) {
        long delayMs = Duration.between(Instant.now(), testEndTime).toMillis();
        holdTimeTask = resourceManager.getSchedulerService().schedule(() -> {
            if (phaseManager.isTestRunning() && !cancelled.get()) {
                phaseManager.terminateTest("HOLD_TIME_EXPIRED");
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private void waitForUserCompletion() {
        try {
            userCompletionLatch.await();
            if (phaseManager.isTestRunning() && !cancelled.get()) {
                // FIX: Use terminateTest instead of completeTest to set reason properly
                phaseManager.terminateTest("ALL_ITERATIONS_COMPLETED");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (phaseManager.isTestRunning()) {
                log.warn("User completion wait interrupted");
            }
        }
    }

    private void cancelScheduledTasks() {
        if (warmupTask != null) {
            warmupTask.cancel(false);
        }
        if (phaseTransitionTask != null) {
            phaseTransitionTask.cancel(false);
        }
        if (holdTimeTask != null) {
            holdTimeTask.cancel(false);
        }
    }
}

/**
 * Strategy for executing OPEN workload model with cancellation support.
 */
class OpenWorkloadStrategy implements WorkloadStrategy {

    private static final Logger log = LoggerFactory.getLogger(OpenWorkloadStrategy.class);

    private final TestPhaseManager phaseManager;
    private final RequestExecutor requestExecutor;
    private final ResourceManager resourceManager;
    private final TimingUtils timingUtils;
    private final AtomicBoolean cancelled;

    private RateLimiter rateLimiter;
    private ScheduledFuture<?> warmupTask;
    private ScheduledFuture<?> durationTask;

    public OpenWorkloadStrategy(TestPhaseManager phaseManager, RequestExecutor requestExecutor,
                                ResourceManager resourceManager, AtomicBoolean cancelled) {
        this.phaseManager = phaseManager;
        this.requestExecutor = requestExecutor;
        this.resourceManager = resourceManager;
        this.timingUtils = new TimingUtils();
        this.cancelled = cancelled;
    }

    @Override
    public void execute(TestPlanSpec testPlanSpec) {
        try {
            var loadModel = testPlanSpec.getExecution().getLoadModel();

            var warmupDuration = timingUtils.parseDuration(loadModel.getWarmup());
            var testDuration = timingUtils.parseDuration(loadModel.getDuration());

            int arrivalRate = loadModel.getArrivalRatePerSec();
            int maxConcurrent = loadModel.getMaxConcurrent();

            log.info("Executing OPEN workload: {} req/sec, max {} concurrent, duration: {}s",
                    arrivalRate, maxConcurrent, testDuration.getSeconds());

            rateLimiter = RateLimiter.create(arrivalRate);

            // Phase 1: Warmup
            if (warmupDuration.toMillis() > 0 && !cancelled.get()) {
                executeWarmup(testPlanSpec, warmupDuration);
            }

            if (!phaseManager.isTestRunning() || cancelled.get()) return;

            // Phase 2: Main execution
            executeMainLoad(testPlanSpec, testDuration, maxConcurrent);
        } finally {
            cancelScheduledTasks();
        }
    }

    private void executeWarmup(TestPlanSpec testPlanSpec, Duration warmupDuration) {
        phaseManager.setPhase(TestPhaseManager.TestPhase.WARMUP);
        log.info("Starting warmup phase for {} seconds", warmupDuration.getSeconds());

        var warmupEnd = Instant.now().plus(warmupDuration);
        warmupTask = resourceManager.getSchedulerService().scheduleWithFixedDelay(() -> {
            if (!phaseManager.isTestRunning() || cancelled.get() || Instant.now().isAfter(warmupEnd)) {
                return;
            }
            try {
                requestExecutor.executeAllRequests(testPlanSpec, phaseManager, -1, true, null, cancelled);
            } catch (Exception e) {
                log.debug("Warmup request failed: {}", e.getMessage());
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);

        timingUtils.sleep(warmupDuration, cancelled);

        if (warmupTask != null) {
            warmupTask.cancel(false);
        }
        log.info("Warmup phase completed");
    }

    private void executeMainLoad(TestPlanSpec testPlanSpec, Duration testDuration, int maxConcurrent) {
        if (cancelled.get()) return;

        phaseManager.setPhase(TestPhaseManager.TestPhase.HOLD);
        var testEndTime = Instant.now().plus(testDuration);

        scheduleDurationTermination(testDuration);

        var concurrencyLimiter = new Semaphore(maxConcurrent);

        // Main request generation loop
        while (phaseManager.isTestRunning() && !cancelled.get() && Instant.now().isBefore(testEndTime)) {
            if (cancelled.get()) {
                log.debug("Request generation loop cancelled");
                break;
            }

            try {
                rateLimiter.acquire();
            } catch (Exception e) {
                log.debug("Rate limiter acquisition interrupted: {}", e.getMessage());
                break;
            }

            if (!phaseManager.isTestRunning() || cancelled.get() || Instant.now().isAfter(testEndTime)) {
                break;
            }

            if (concurrencyLimiter.tryAcquire()) {
                resourceManager.getMainExecutor().submit(() -> {
                    requestExecutor.executeAllRequests(testPlanSpec, phaseManager, -1, false, concurrencyLimiter, cancelled);
                });
            } else {
                requestExecutor.logBackPressureEvent(phaseManager.getCurrentPhase());
            }
        }

        // Wait for remaining requests
        waitForActiveRequestsToComplete(Duration.ofSeconds(30));

        if (phaseManager.isTestRunning() && !cancelled.get()) {
            // FIX: Use terminateTest to ensure reason is propagated
            phaseManager.terminateTest("DURATION_COMPLETED");
        }
    }


    private void scheduleDurationTermination(Duration testDuration) {
        durationTask = resourceManager.getSchedulerService().schedule(() -> {
            if (phaseManager.isTestRunning() && !cancelled.get()) {
                phaseManager.terminateTest("DURATION_COMPLETED");
            }
        }, testDuration.toMillis(), TimeUnit.MILLISECONDS);
    }


    private void waitForActiveRequestsToComplete(Duration timeout) {
        var deadline = Instant.now().plus(timeout);

        while (requestExecutor.getActiveRequestCount() > 0 && !cancelled.get() && Instant.now().isBefore(deadline)) {
            timingUtils.sleep(Duration.ofMillis(100), cancelled);
        }

        if (requestExecutor.getActiveRequestCount() > 0) {
            log.warn("Timeout waiting for {} active requests to complete", requestExecutor.getActiveRequestCount());
        }
    }

    private void cancelScheduledTasks() {
        if (warmupTask != null) {
            warmupTask.cancel(false);
        }
        if (durationTask != null) {
            durationTask.cancel(false);
        }
    }
}

// ===== TIMING UTILITIES =====

/**
 * Utility class for timing and duration operations with cancellation support.
 */
class TimingUtils {

    private static final Logger log = LoggerFactory.getLogger(TimingUtils.class);

    public Duration parseDuration(String duration) {
        if (duration == null || duration.trim().isEmpty()) {
            return Duration.ZERO;
        }

        var trimmed = duration.trim().toLowerCase();
        if (trimmed.endsWith("s")) {
            return Duration.ofSeconds(Integer.parseInt(trimmed.substring(0, trimmed.length() - 1)));
        } else if (trimmed.endsWith("m")) {
            return Duration.ofMinutes(Integer.parseInt(trimmed.substring(0, trimmed.length() - 1)));
        } else {
            return Duration.ofSeconds(Integer.parseInt(trimmed));
        }
    }

    public void applyThinkTime(TestPlanSpec.ThinkTime thinkTime, AtomicBoolean cancelled) {
        if (thinkTime == null || cancelled.get()) return;

        try {
            int delay;
            if (thinkTime.getType() == TestPlanSpec.ThinkTimeType.FIXED) {
                delay = thinkTime.getMin();
            } else {
                delay = ThreadLocalRandom.current().nextInt(thinkTime.getMin(), thinkTime.getMax() + 1);
            }

            if (delay > 0 && !cancelled.get()) {
                log.debug("Applying think time: {} ms", delay);
                Thread.sleep(delay);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void sleep(Duration duration, AtomicBoolean cancelled) {
        if (cancelled.get()) return;

        try {
            long totalMillis = duration.toMillis();
            long sleptMillis = 0;
            long checkInterval = Math.min(100, totalMillis); // Check cancellation every 100ms or total duration

            while (sleptMillis < totalMillis && !cancelled.get()) {
                long remainingMillis = totalMillis - sleptMillis;
                long sleepTime = Math.min(checkInterval, remainingMillis);

                Thread.sleep(sleepTime);
                sleptMillis += sleepTime;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void sleep(Duration duration) {
        sleep(duration, new AtomicBoolean(false)); // Fallback for non-cancellable sleeps
    }
}

