package org.load.execution.runner.load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Improved closed workload strategy that executes a fixed number of virtual users
 * with configurable iterations, ramp-up, and hold periods.
 * 
 * <p>This strategy simulates real user behavior by:
 * <ul>
 *   <li>Gradually ramping up users over a specified period</li>
 *   <li>Executing multiple iterations per user with think time</li>
 *   <li>Supporting warmup periods for system preparation</li>
 *   <li>Providing comprehensive progress tracking and metrics</li>
 * </ul>
 * 
 * <p>The strategy supports graceful cancellation and cleanup of all resources.
 * 
 * @author Load Test Team
 * @version 2.0.0
 */
public class ClosedWorkloadStrategy implements WorkloadStrategyFactory.WorkloadStrategy {
    private static final Logger log = LoggerFactory.getLogger(ClosedWorkloadStrategy.class);

    // Configuration defaults
    private static final int DEFAULT_WARMUP_INTERVAL_MS = 1000;
    private static final int DEFAULT_PROGRESS_LOG_INTERVAL_MS = 30000; // 30 seconds
    private static final Duration DEFAULT_USER_START_TIMEOUT = Duration.ofMinutes(1);

    /**
     * Configuration for closed workload strategy behavior.
     */
    public static class ClosedWorkloadConfig {
        private final int warmupIntervalMs;
        private final int progressLogIntervalMs;
        private final Duration userStartTimeout;
        private final boolean enableProgressLogging;
        private final boolean enableDetailedMetrics;

        public ClosedWorkloadConfig(int warmupIntervalMs, int progressLogIntervalMs, 
                                   Duration userStartTimeout, boolean enableProgressLogging,
                                   boolean enableDetailedMetrics) {
            this.warmupIntervalMs = Math.max(100, warmupIntervalMs);
            this.progressLogIntervalMs = Math.max(1000, progressLogIntervalMs);
            this.userStartTimeout = userStartTimeout != null ? userStartTimeout : DEFAULT_USER_START_TIMEOUT;
            this.enableProgressLogging = enableProgressLogging;
            this.enableDetailedMetrics = enableDetailedMetrics;
        }

        public static ClosedWorkloadConfig defaultConfig() {
            return new ClosedWorkloadConfig(DEFAULT_WARMUP_INTERVAL_MS, DEFAULT_PROGRESS_LOG_INTERVAL_MS,
                    DEFAULT_USER_START_TIMEOUT, true, true);
        }

        // Getters
        public int getWarmupIntervalMs() { return warmupIntervalMs; }
        public int getProgressLogIntervalMs() { return progressLogIntervalMs; }
        public Duration getUserStartTimeout() { return userStartTimeout; }
        public boolean isProgressLoggingEnabled() { return enableProgressLogging; }
        public boolean isDetailedMetricsEnabled() { return enableDetailedMetrics; }
    }

    /**
     * Progress tracking for the closed workload execution.
     */
    public static class WorkloadProgress {
        private final int totalUsers;
        private final int activeUsers;
        private final int completedUsers;
        private final long totalIterationsExpected;
        private final long completedIterations;
        private final TestPhaseManager.TestPhase currentPhase;
        private final Duration elapsed;
        private final Duration estimated;

        public WorkloadProgress(int totalUsers, int activeUsers, int completedUsers,
                              long totalIterationsExpected, long completedIterations,
                              TestPhaseManager.TestPhase currentPhase, Duration elapsed, Duration estimated) {
            this.totalUsers = totalUsers;
            this.activeUsers = activeUsers;
            this.completedUsers = completedUsers;
            this.totalIterationsExpected = totalIterationsExpected;
            this.completedIterations = completedIterations;
            this.currentPhase = currentPhase;
            this.elapsed = elapsed;
            this.estimated = estimated;
        }

        // Getters
        public int getTotalUsers() { return totalUsers; }
        public int getActiveUsers() { return activeUsers; }
        public int getCompletedUsers() { return completedUsers; }
        public long getTotalIterationsExpected() { return totalIterationsExpected; }
        public long getCompletedIterations() { return completedIterations; }
        public TestPhaseManager.TestPhase getCurrentPhase() { return currentPhase; }
        public Duration getElapsed() { return elapsed; }
        public Duration getEstimated() { return estimated; }

        public double getCompletionPercentage() {
            return totalIterationsExpected > 0 ? 
                (double) completedIterations / totalIterationsExpected * 100 : 0;
        }

        @Override
        public String toString() {
            return String.format("Progress[users=%d/%d, iterations=%d/%d (%.1f%%), phase=%s, elapsed=%ds]",
                    completedUsers + activeUsers, totalUsers, completedIterations, totalIterationsExpected,
                    getCompletionPercentage(), currentPhase, elapsed.getSeconds());
        }
    }

    // Dependencies
    private final TestPhaseManager phaseManager;
    private final RequestExecutor requestExecutor;
    private final ResourceManager resourceManager;
    private final TimingUtils timingUtils;
    private final AtomicBoolean cancelled;
    private final ClosedWorkloadConfig config;

    // Execution state
    private CountDownLatch userCompletionLatch;
    private ScheduledFuture<?> warmupTask;
    private ScheduledFuture<?> phaseTransitionTask;
    private ScheduledFuture<?> holdTimeTask;
    private ScheduledFuture<?> progressLogTask;

    // Metrics
    private final AtomicInteger activeUserCount = new AtomicInteger(0);
    private final AtomicInteger completedUserCount = new AtomicInteger(0);
    private final AtomicLong completedIterations = new AtomicLong(0);
    private volatile Instant executionStartTime;

    // ================================
    // CONSTRUCTORS
    // ================================

    public ClosedWorkloadStrategy(TestPhaseManager phaseManager, RequestExecutor requestExecutor,
                                  ResourceManager resourceManager, AtomicBoolean cancelled) {
        this(phaseManager, requestExecutor, resourceManager, cancelled, ClosedWorkloadConfig.defaultConfig());
    }

    public ClosedWorkloadStrategy(TestPhaseManager phaseManager, RequestExecutor requestExecutor,
                                  ResourceManager resourceManager, AtomicBoolean cancelled,
                                  ClosedWorkloadConfig config) {
        this.phaseManager = phaseManager;
        this.requestExecutor = requestExecutor;
        this.resourceManager = resourceManager;
        this.timingUtils = new TimingUtils();
        this.cancelled = cancelled;
        this.config = config != null ? config : ClosedWorkloadConfig.defaultConfig();
    }

    // ================================
    // WORKLOAD STRATEGY IMPLEMENTATION
    // ================================

    @Override
    public void execute(TestPlanSpec testPlanSpec) {
        executionStartTime = Instant.now();
        
        try {
            ExecutionPlan plan = createExecutionPlan(testPlanSpec);
            validateExecutionPlan(plan);

            userCompletionLatch = new CountDownLatch(plan.totalUsers);

            log.info("Executing CLOSED workload: {} users, {} iterations per user, ramp-up: {}s, hold: {}s",
                    plan.totalUsers, plan.iterationsPerUser, plan.rampUpDuration.getSeconds(), plan.holdDuration.getSeconds());

            // Start progress logging if enabled
            startProgressLogging(plan);

            // Phase 1: Warmup (optional)
            if (plan.warmupDuration.toMillis() > 0 && !cancelled.get()) {
                executeWarmupPhase(testPlanSpec, plan);
            }

            if (!phaseManager.isTestRunning() || cancelled.get()) {
                return;
            }

            // Phase 2: Main execution (ramp-up + hold)
            executeMainPhase(testPlanSpec, plan);

            // Phase 3: Wait for completion
            waitForUserCompletion(plan);

        } catch (Exception e) {
            log.error("Closed workload execution failed", e);
            phaseManager.terminateTest("EXECUTION_ERROR: " + e.getMessage());
        } finally {
            cleanup();
        }
    }

    @Override
    public void validate(TestPlanSpec testPlanSpec) throws WorkloadStrategyFactory.StrategyValidationException {
        if (testPlanSpec == null || testPlanSpec.getExecution() == null || testPlanSpec.getExecution().getLoadModel() == null) {
            throw new WorkloadStrategyFactory.StrategyValidationException("Test plan specification is incomplete");
        }

        var loadModel = testPlanSpec.getExecution().getLoadModel();
        
        if (loadModel.getUsers() <= 0) {
            throw new WorkloadStrategyFactory.StrategyValidationException("Users must be greater than 0");
        }
        
        if (loadModel.getIterations() <= 0) {
            throw new WorkloadStrategyFactory.StrategyValidationException("Iterations must be greater than 0");
        }

        // Validate durations
        try {
            timingUtils.parseDuration(loadModel.getWarmup());
            timingUtils.parseDuration(loadModel.getRampUp());
            timingUtils.parseDuration(loadModel.getHoldFor());
        } catch (Exception e) {
            throw new WorkloadStrategyFactory.StrategyValidationException("Invalid duration format: " + e.getMessage());
        }
    }

    @Override
    public void cleanup() {
        cancelAllScheduledTasks();
        
        if (progressLogTask != null) {
            progressLogTask.cancel(false);
        }
        
        log.debug("ClosedWorkloadStrategy cleanup completed");
    }

    // ================================
    // PRIVATE IMPLEMENTATION
    // ================================

    /**
     * Execution plan containing all calculated parameters.
     */
    private static class ExecutionPlan {
        final Duration warmupDuration;
        final Duration rampUpDuration;
        final Duration holdDuration;
        final int totalUsers;
        final int iterationsPerUser;
        final long totalIterationsExpected;
        final Instant rampUpStart;
        final Instant holdStart;
        final Instant testEndTime;

        ExecutionPlan(Duration warmupDuration, Duration rampUpDuration, Duration holdDuration,
                     int totalUsers, int iterationsPerUser) {
            this.warmupDuration = warmupDuration;
            this.rampUpDuration = rampUpDuration;
            this.holdDuration = holdDuration;
            this.totalUsers = totalUsers;
            this.iterationsPerUser = iterationsPerUser;
            this.totalIterationsExpected = (long) totalUsers * iterationsPerUser;
            
            // Calculate phase timing
            Instant now = Instant.now();
            this.rampUpStart = now.plus(warmupDuration);
            this.holdStart = rampUpStart.plus(rampUpDuration);
            this.testEndTime = holdStart.plus(holdDuration);
        }
    }

    private ExecutionPlan createExecutionPlan(TestPlanSpec testPlanSpec) {
        var loadModel = testPlanSpec.getExecution().getLoadModel();

        Duration warmupDuration = timingUtils.parseDuration(loadModel.getWarmup());
        Duration rampUpDuration = timingUtils.parseDuration(loadModel.getRampUp());
        Duration holdDuration = timingUtils.parseDuration(loadModel.getHoldFor());
        int totalUsers = loadModel.getUsers();
        int iterationsPerUser = loadModel.getIterations();

        return new ExecutionPlan(warmupDuration, rampUpDuration, holdDuration, totalUsers, iterationsPerUser);
    }

    private void validateExecutionPlan(ExecutionPlan plan) {
        if (plan.totalUsers > 10000) {
            log.warn("Large number of users ({}). Consider system capacity.", plan.totalUsers);
        }
        
        if (plan.totalIterationsExpected > 1_000_000) {
            log.warn("Large number of total iterations ({}). Consider execution time.", plan.totalIterationsExpected);
        }
    }

    private void startProgressLogging(ExecutionPlan plan) {
        if (!config.isProgressLoggingEnabled()) {
            return;
        }

        progressLogTask = resourceManager.getSchedulerService().scheduleWithFixedDelay(() -> {
            try {
                WorkloadProgress progress = getCurrentProgress(plan);
                log.info("Workload progress: {}", progress);
            } catch (Exception e) {
                log.debug("Error logging progress: {}", e.getMessage());
            }
        }, config.getProgressLogIntervalMs(), config.getProgressLogIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private WorkloadProgress getCurrentProgress(ExecutionPlan plan) {
        Duration elapsed = Duration.between(executionStartTime, Instant.now());
        
        // Simple estimation based on current progress
        double completionRate = plan.totalIterationsExpected > 0 ? 
            (double) completedIterations.get() / plan.totalIterationsExpected : 0;
        
        Duration estimated = completionRate > 0.01 ? // Avoid division by very small numbers
            Duration.ofMillis((long) (elapsed.toMillis() / completionRate)) : Duration.ZERO;

        return new WorkloadProgress(
            plan.totalUsers,
            activeUserCount.get(),
            completedUserCount.get(),
            plan.totalIterationsExpected,
            completedIterations.get(),
            phaseManager.getCurrentPhase(),
            elapsed,
            estimated
        );
    }

    private void executeWarmupPhase(TestPlanSpec testPlanSpec, ExecutionPlan plan) {
        phaseManager.setPhase(TestPhaseManager.TestPhase.WARMUP);
        log.info("Starting warmup phase for {} seconds", plan.warmupDuration.getSeconds());

        Instant warmupEnd = Instant.now().plus(plan.warmupDuration);
        
        warmupTask = resourceManager.getSchedulerService().scheduleWithFixedDelay(() -> {
            if (!phaseManager.isTestRunning() || cancelled.get() || Instant.now().isAfter(warmupEnd)) {
                return;
            }
            
            try {
                // Execute warmup requests with the enhanced request executor
                requestExecutor.executeAllRequests(testPlanSpec, phaseManager, -1, true, null, cancelled);
            } catch (Exception e) {
                log.debug("Warmup request failed: {}", e.getMessage());
            }
        }, 0, config.getWarmupIntervalMs(), TimeUnit.MILLISECONDS);

        // Wait for warmup completion
        timingUtils.sleep(plan.warmupDuration, cancelled);

        if (warmupTask != null) {
            warmupTask.cancel(false);
        }
        
        log.info("Warmup phase completed");
    }

    private void executeMainPhase(TestPlanSpec testPlanSpec, ExecutionPlan plan) {
        if (cancelled.get()) return;

        phaseManager.setPhase(TestPhaseManager.TestPhase.RAMP_UP);

        // Schedule phase transitions
        schedulePhaseTransition(plan.rampUpDuration);
        scheduleHoldTimeTermination(plan.testEndTime);

        // Start all user threads with calculated delays
        startUserThreads(testPlanSpec, plan);
    }

    private void startUserThreads(TestPlanSpec testPlanSpec, ExecutionPlan plan) {
        for (int userId = 0; userId < plan.totalUsers; userId++) {
            if (cancelled.get()) break;

            // Calculate staggered start delay for ramp-up
            long userStartDelay = plan.rampUpDuration.toMillis() > 0 ? 
                (userId * plan.rampUpDuration.toMillis()) / plan.totalUsers : 0;

            startUserThread(testPlanSpec, userId, userStartDelay, plan.iterationsPerUser, plan.testEndTime);
        }
    }

    private void startUserThread(TestPlanSpec testPlanSpec, int userId, long startDelay,
                                 int iterations, Instant testEndTime) {
        
        // Use the resource manager's tracked task submission for better monitoring
        resourceManager.submitTrackedTask(() -> {
            executeUserThread(testPlanSpec, userId, startDelay, iterations, testEndTime);
        }, "user-" + userId);
    }

    private void executeUserThread(TestPlanSpec testPlanSpec, int userId, long startDelay,
                                   int iterations, Instant testEndTime) {
        try {
            // Apply start delay for ramp-up
            if (startDelay > 0 && !cancelled.get()) {
                Thread.sleep(startDelay);
            }

            if (cancelled.get()) return;

            activeUserCount.incrementAndGet();
            log.debug("User {} started with {} iterations (delay: {}ms)", userId, iterations, startDelay);

            int completedIterations = executeUserIterations(testPlanSpec, userId, iterations, testEndTime);
            
            log.debug("User {} completed {} out of {} iterations", userId, completedIterations, iterations);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("User {} interrupted", userId);
        } catch (Exception e) {
            log.warn("User {} failed with error: {}", userId, e.getMessage());
        } finally {
            activeUserCount.decrementAndGet();
            completedUserCount.incrementAndGet();
            userCompletionLatch.countDown();
            log.debug("User {} finished", userId);
        }
    }

    private int executeUserIterations(TestPlanSpec testPlanSpec, int userId, int iterations, Instant testEndTime) 
            throws InterruptedException {
        int completed = 0;

        for (int i = 0; i < iterations && phaseManager.isTestRunning() && !cancelled.get(); i++) {
            // Check if hold time has expired
            if (Instant.now().isAfter(testEndTime)) {
                log.debug("User {} stopping due to hold time expiration after {} iterations", userId, completed);
                break;
            }

            try {
                // Execute requests for this iteration
                requestExecutor.executeAllRequests(testPlanSpec, phaseManager, userId, false, null, cancelled);
                completed++;
                completedIterations.incrementAndGet();

                // Apply think time between iterations (except for the last one)
                if (i < iterations - 1 && !cancelled.get()) {
                    timingUtils.applyThinkTime(testPlanSpec.getExecution().getThinkTime(), cancelled);
                }

            } catch (Exception e) {
                log.debug("User {} iteration {} failed: {}", userId, i + 1, e.getMessage());
                // Continue with next iteration despite failure
            }
        }

        return completed;
    }

    private void schedulePhaseTransition(Duration rampUpDuration) {
        phaseTransitionTask = resourceManager.getSchedulerService().schedule(() -> {
            if (phaseManager.isTestRunning() && !cancelled.get()) {
                phaseManager.setPhase(TestPhaseManager.TestPhase.HOLD);
                log.info("Transitioned to HOLD phase");
            }
        }, rampUpDuration.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void scheduleHoldTimeTermination(Instant testEndTime) {
        long delayMs = Math.max(0, Duration.between(Instant.now(), testEndTime).toMillis());
        
        holdTimeTask = resourceManager.getSchedulerService().schedule(() -> {
            if (phaseManager.isTestRunning() && !cancelled.get()) {
                log.info("Hold time expired, terminating test");
                phaseManager.terminateTest("HOLD_TIME_EXPIRED");
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private void waitForUserCompletion(ExecutionPlan plan) {
        try {
            boolean completed = userCompletionLatch.await(
                config.getUserStartTimeout().plus(Duration.ofMinutes(5)).toMillis(), 
                TimeUnit.MILLISECONDS
            );

            if (!completed) {
                log.warn("Timeout waiting for user completion. Active users: {}", activeUserCount.get());
                phaseManager.terminateTest("USER_COMPLETION_TIMEOUT");
            } else if (phaseManager.isTestRunning() && !cancelled.get()) {
                log.info("All users completed their iterations");
                phaseManager.terminateTest("ALL_ITERATIONS_COMPLETED");
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (phaseManager.isTestRunning()) {
                log.warn("User completion wait interrupted");
                phaseManager.terminateTest("INTERRUPTED");
            }
        }
    }

    private void cancelAllScheduledTasks() {
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

    // ================================
    // PUBLIC GETTERS FOR MONITORING
    // ================================

    /**
     * Returns current workload progress for external monitoring.
     */
    public WorkloadProgress getCurrentProgress() {
        if (executionStartTime == null) {
            return null;
        }
        
        // Create a simple execution plan for progress calculation
        ExecutionPlan plan = new ExecutionPlan(Duration.ZERO, Duration.ZERO, Duration.ZERO, 
            userCompletionLatch != null ? (int) userCompletionLatch.getCount() + completedUserCount.get() : 0, 1);
        
        return getCurrentProgress(plan);
    }

    /**
     * Returns current metrics for monitoring.
     */
    public ClosedWorkloadMetrics getMetrics() {
        return new ClosedWorkloadMetrics(
            activeUserCount.get(),
            completedUserCount.get(),
            completedIterations.get(),
            executionStartTime != null ? Duration.between(executionStartTime, Instant.now()) : Duration.ZERO
        );
    }

    /**
     * Simple metrics holder for closed workload execution.
     */
    public static class ClosedWorkloadMetrics {
        private final int activeUsers;
        private final int completedUsers;
        private final long completedIterations;
        private final Duration elapsedTime;

        public ClosedWorkloadMetrics(int activeUsers, int completedUsers, 
                                   long completedIterations, Duration elapsedTime) {
            this.activeUsers = activeUsers;
            this.completedUsers = completedUsers;
            this.completedIterations = completedIterations;
            this.elapsedTime = elapsedTime;
        }

        public int getActiveUsers() { return activeUsers; }
        public int getCompletedUsers() { return completedUsers; }
        public long getCompletedIterations() { return completedIterations; }
        public Duration getElapsedTime() { return elapsedTime; }

        @Override
        public String toString() {
            return String.format("ClosedWorkloadMetrics[active=%d, completed=%d, iterations=%d, elapsed=%ds]",
                    activeUsers, completedUsers, completedIterations, elapsedTime.getSeconds());
        }
    }
}