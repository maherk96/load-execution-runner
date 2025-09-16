package org.load.execution.runner.load;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Improved open workload strategy that generates requests at a specified arrival rate
 * with configurable concurrency limits and comprehensive monitoring.
 *
 * <p>This strategy simulates realistic load patterns by:
 * <ul>
 *   <li>Maintaining a consistent arrival rate regardless of response times</li>
 *   <li>Enforcing maximum concurrency limits to prevent resource exhaustion</li>
 *   <li>Supporting warmup periods for gradual load introduction</li>
 *   <li>Providing detailed metrics on arrival rates and concurrency utilization</li>
 * </ul>
 *
 * <p>The strategy handles back-pressure gracefully when concurrency limits are reached
 * and provides comprehensive progress tracking throughout execution.
 *
 * @author Load Test Team
 * @version 2.0.0
 */
public class OpenWorkloadStrategy implements WorkloadStrategyFactory.WorkloadStrategy {
    private static final Logger log = LoggerFactory.getLogger(OpenWorkloadStrategy.class);

    // Configuration defaults
    private static final int DEFAULT_WARMUP_INTERVAL_MS = 1000;
    private static final int DEFAULT_PROGRESS_LOG_INTERVAL_MS = 30000; // 30 seconds
    private static final Duration DEFAULT_REQUEST_COMPLETION_TIMEOUT = Duration.ofSeconds(30);
    private static final double DEFAULT_RATE_TOLERANCE = 0.1; // 10%

    /**
     * Configuration for open workload strategy behavior.
     */
    public static class OpenWorkloadConfig {
        private final int warmupIntervalMs;
        private final int progressLogIntervalMs;
        private final Duration requestCompletionTimeout;
        private final double rateTolerance;
        private final boolean enableProgressLogging;
        private final boolean enableDetailedMetrics;
        private final boolean enableRateAdjustment;

        public OpenWorkloadConfig(int warmupIntervalMs, int progressLogIntervalMs,
                                  Duration requestCompletionTimeout, double rateTolerance,
                                  boolean enableProgressLogging, boolean enableDetailedMetrics,
                                  boolean enableRateAdjustment) {
            this.warmupIntervalMs = Math.max(100, warmupIntervalMs);
            this.progressLogIntervalMs = Math.max(1000, progressLogIntervalMs);
            this.requestCompletionTimeout = requestCompletionTimeout != null ?
                    requestCompletionTimeout : DEFAULT_REQUEST_COMPLETION_TIMEOUT;
            this.rateTolerance = Math.max(0.0, Math.min(1.0, rateTolerance));
            this.enableProgressLogging = enableProgressLogging;
            this.enableDetailedMetrics = enableDetailedMetrics;
            this.enableRateAdjustment = enableRateAdjustment;
        }

        public static OpenWorkloadConfig defaultConfig() {
            return new OpenWorkloadConfig(DEFAULT_WARMUP_INTERVAL_MS, DEFAULT_PROGRESS_LOG_INTERVAL_MS,
                    DEFAULT_REQUEST_COMPLETION_TIMEOUT, DEFAULT_RATE_TOLERANCE, true, true, false);
        }

        // Getters
        public int getWarmupIntervalMs() { return warmupIntervalMs; }
        public int getProgressLogIntervalMs() { return progressLogIntervalMs; }
        public Duration getRequestCompletionTimeout() { return requestCompletionTimeout; }
        public double getRateTolerance() { return rateTolerance; }
        public boolean isProgressLoggingEnabled() { return enableProgressLogging; }
        public boolean isDetailedMetricsEnabled() { return enableDetailedMetrics; }
        public boolean isRateAdjustmentEnabled() { return enableRateAdjustment; }
    }

    /**
     * Progress tracking for open workload execution.
     */
    public static class WorkloadProgress {
        private final double targetRate;
        private final double actualRate;
        private final int maxConcurrency;
        private final int currentConcurrency;
        private final long totalRequests;
        private final long successfulRequests;
        private final long backPressureEvents;
        private final Duration elapsed;
        private final Duration remaining;
        private final TestPhaseManager.TestPhase currentPhase;

        public WorkloadProgress(double targetRate, double actualRate, int maxConcurrency,
                                int currentConcurrency, long totalRequests, long successfulRequests,
                                long backPressureEvents, Duration elapsed, Duration remaining,
                                TestPhaseManager.TestPhase currentPhase) {
            this.targetRate = targetRate;
            this.actualRate = actualRate;
            this.maxConcurrency = maxConcurrency;
            this.currentConcurrency = currentConcurrency;
            this.totalRequests = totalRequests;
            this.successfulRequests = successfulRequests;
            this.backPressureEvents = backPressureEvents;
            this.elapsed = elapsed;
            this.remaining = remaining;
            this.currentPhase = currentPhase;
        }

        // Getters
        public double getTargetRate() { return targetRate; }
        public double getActualRate() { return actualRate; }
        public int getMaxConcurrency() { return maxConcurrency; }
        public int getCurrentConcurrency() { return currentConcurrency; }
        public long getTotalRequests() { return totalRequests; }
        public long getSuccessfulRequests() { return successfulRequests; }
        public long getBackPressureEvents() { return backPressureEvents; }
        public Duration getElapsed() { return elapsed; }
        public Duration getRemaining() { return remaining; }
        public TestPhaseManager.TestPhase getCurrentPhase() { return currentPhase; }

        public double getConcurrencyUtilization() {
            return maxConcurrency > 0 ? (double) currentConcurrency / maxConcurrency * 100 : 0;
        }

        public double getRateAccuracy() {
            return targetRate > 0 ? Math.min(100, actualRate / targetRate * 100) : 100;
        }

        public double getSuccessRate() {
            return totalRequests > 0 ? (double) successfulRequests / totalRequests * 100 : 100;
        }

        @Override
        public String toString() {
            return String.format("Progress[rate=%.1f/%.1f req/s (%.1f%%), concurrency=%d/%d (%.1f%%), " +
                            "requests=%d, success=%.1f%%, phase=%s, elapsed=%ds, remaining=%ds]",
                    actualRate, targetRate, getRateAccuracy(), currentConcurrency, maxConcurrency,
                    getConcurrencyUtilization(), totalRequests, getSuccessRate(), currentPhase,
                    elapsed.getSeconds(), remaining.getSeconds());
        }
    }

    // Dependencies
    private final TestPhaseManager phaseManager;
    private final RequestExecutor requestExecutor;
    private final ResourceManager resourceManager;
    private final TimingUtils timingUtils;
    private final AtomicBoolean cancelled;
    private final OpenWorkloadConfig config;

    // Execution state
    private RateLimiter rateLimiter;
    private ScheduledFuture<?> warmupTask;
    private ScheduledFuture<?> durationTask;
    private ScheduledFuture<?> progressLogTask;
    private Semaphore concurrencyLimiter;

    // Metrics
    private final AtomicLong requestsGenerated = new AtomicLong(0);
    private final AtomicLong successfulRequests = new AtomicLong(0);
    private final AtomicLong backPressureEvents = new AtomicLong(0);
    private volatile Instant executionStartTime;
    private volatile Instant mainPhaseStartTime;

    // ================================
    // CONSTRUCTORS
    // ================================

    public OpenWorkloadStrategy(TestPhaseManager phaseManager, RequestExecutor requestExecutor,
                                ResourceManager resourceManager, AtomicBoolean cancelled) {
        this(phaseManager, requestExecutor, resourceManager, cancelled, OpenWorkloadConfig.defaultConfig());
    }

    public OpenWorkloadStrategy(TestPhaseManager phaseManager, RequestExecutor requestExecutor,
                                ResourceManager resourceManager, AtomicBoolean cancelled,
                                OpenWorkloadConfig config) {
        this.phaseManager = phaseManager;
        this.requestExecutor = requestExecutor;
        this.resourceManager = resourceManager;
        this.timingUtils = new TimingUtils();
        this.cancelled = cancelled;
        this.config = config != null ? config : OpenWorkloadConfig.defaultConfig();
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

            rateLimiter = RateLimiter.create(plan.arrivalRate);
            concurrencyLimiter = new Semaphore(plan.maxConcurrent);

            log.info("Executing OPEN workload: {} req/sec, max {} concurrent, duration: {}s",
                    plan.arrivalRate, plan.maxConcurrent, plan.testDuration.getSeconds());

            // Start progress logging if enabled
            startProgressLogging(plan);

            // Phase 1: Warmup (optional)
            if (plan.warmupDuration.toMillis() > 0 && !cancelled.get()) {
                executeWarmupPhase(testPlanSpec, plan);
            }

            if (!phaseManager.isTestRunning() || cancelled.get()) {
                return;
            }

            // Phase 2: Main execution
            executeMainPhase(testPlanSpec, plan);

        } catch (Exception e) {
            log.error("Open workload execution failed", e);
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

        if (loadModel.getArrivalRatePerSec() <= 0) {
            throw new WorkloadStrategyFactory.StrategyValidationException("Arrival rate must be greater than 0");
        }

        if (loadModel.getMaxConcurrent() <= 0) {
            throw new WorkloadStrategyFactory.StrategyValidationException("Max concurrent requests must be greater than 0");
        }

        if (loadModel.getDuration() == null || loadModel.getDuration().trim().isEmpty()) {
            throw new WorkloadStrategyFactory.StrategyValidationException("Duration must be specified");
        }

        // Validate durations
        try {
            Duration testDuration = timingUtils.parseDuration(loadModel.getDuration());
            if (testDuration.isZero() || testDuration.isNegative()) {
                throw new WorkloadStrategyFactory.StrategyValidationException("Test duration must be positive");
            }

            timingUtils.parseDuration(loadModel.getWarmup());
        } catch (Exception e) {
            throw new WorkloadStrategyFactory.StrategyValidationException("Invalid duration format: " + e.getMessage());
        }

        // Warn about potentially problematic configurations
        if (loadModel.getArrivalRatePerSec() > 1000) {
            log.warn("High arrival rate ({}). Ensure system can handle the load.", loadModel.getArrivalRatePerSec());
        }

        if (loadModel.getMaxConcurrent() > 1000) {
            log.warn("High concurrency limit ({}). Monitor resource usage.", loadModel.getMaxConcurrent());
        }
    }

    @Override
    public void cleanup() {
        cancelAllScheduledTasks();

        if (progressLogTask != null) {
            progressLogTask.cancel(false);
        }

        log.debug("OpenWorkloadStrategy cleanup completed");
    }

    // ================================
    // PRIVATE IMPLEMENTATION
    // ================================

    /**
     * Execution plan containing all calculated parameters.
     */
    private static class ExecutionPlan {
        final Duration warmupDuration;
        final Duration testDuration;
        final int arrivalRate;
        final int maxConcurrent;
        final Instant warmupEnd;
        final Instant testEndTime;

        ExecutionPlan(Duration warmupDuration, Duration testDuration, int arrivalRate, int maxConcurrent) {
            this.warmupDuration = warmupDuration;
            this.testDuration = testDuration;
            this.arrivalRate = arrivalRate;
            this.maxConcurrent = maxConcurrent;

            // Calculate phase timing
            Instant now = Instant.now();
            this.warmupEnd = now.plus(warmupDuration);
            this.testEndTime = warmupEnd.plus(testDuration);
        }
    }

    private ExecutionPlan createExecutionPlan(TestPlanSpec testPlanSpec) {
        var loadModel = testPlanSpec.getExecution().getLoadModel();

        Duration warmupDuration = timingUtils.parseDuration(loadModel.getWarmup());
        Duration testDuration = timingUtils.parseDuration(loadModel.getDuration());
        int arrivalRate = loadModel.getArrivalRatePerSec();
        int maxConcurrent = loadModel.getMaxConcurrent();

        return new ExecutionPlan(warmupDuration, testDuration, arrivalRate, maxConcurrent);
    }

    private void validateExecutionPlan(ExecutionPlan plan) {
        // Calculate expected total requests for capacity planning
        long expectedRequests = (long) plan.arrivalRate * plan.testDuration.getSeconds();
        if (expectedRequests > 100_000) {
            log.warn("Large number of expected requests ({}). Consider system capacity and test duration.", expectedRequests);
        }

        // Check if arrival rate vs concurrency makes sense
        if (plan.arrivalRate > plan.maxConcurrent * 10) {
            log.warn("Arrival rate ({}) may be too high for concurrency limit ({}). Consider increasing max concurrent requests.",
                    plan.arrivalRate, plan.maxConcurrent);
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

                // Warn if rate accuracy is low
                if (progress.getRateAccuracy() < (100 - config.getRateTolerance() * 100)) {
                    log.warn("Actual rate ({} req/s) significantly below target ({} req/s). Check system capacity.",
                            String.format("%.1f", progress.getActualRate()), String.format("%.1f", progress.getTargetRate()));
                }

            } catch (Exception e) {
                log.debug("Error logging progress: {}", e.getMessage());
            }
        }, config.getProgressLogIntervalMs(), config.getProgressLogIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private WorkloadProgress getCurrentProgress(ExecutionPlan plan) {
        Duration elapsed = Duration.between(executionStartTime, Instant.now());
        Duration remaining;

        if (mainPhaseStartTime != null) {
            Duration mainElapsed = Duration.between(mainPhaseStartTime, Instant.now());
            remaining = plan.testDuration.minus(mainElapsed);
            if (remaining.isNegative()) {
                remaining = Duration.ZERO;
            }
        } else {
            // Still in warmup or not started main phase
            remaining = plan.testDuration;
        }

        // Calculate actual arrival rate
        double actualRate = 0.0;
        if (mainPhaseStartTime != null && elapsed.getSeconds() > 0) {
            actualRate = (double) requestsGenerated.get() / elapsed.getSeconds();
        }

        int currentConcurrency = plan.maxConcurrent - concurrencyLimiter.availablePermits();

        return new WorkloadProgress(
                plan.arrivalRate,
                actualRate,
                plan.maxConcurrent,
                currentConcurrency,
                requestsGenerated.get(),
                successfulRequests.get(),
                backPressureEvents.get(),
                elapsed,
                remaining,
                phaseManager.getCurrentPhase()
        );
    }

    private void executeWarmupPhase(TestPlanSpec testPlanSpec, ExecutionPlan plan) {
        phaseManager.setPhase(TestPhaseManager.TestPhase.WARMUP);
        log.info("Starting warmup phase for {} seconds", plan.warmupDuration.getSeconds());

        warmupTask = resourceManager.getSchedulerService().scheduleWithFixedDelay(() -> {
            if (!phaseManager.isTestRunning() || cancelled.get() || Instant.now().isAfter(plan.warmupEnd)) {
                return;
            }

            try {
                // Execute warmup requests
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

        phaseManager.setPhase(TestPhaseManager.TestPhase.HOLD);
        mainPhaseStartTime = Instant.now();
        Instant testEndTime = mainPhaseStartTime.plus(plan.testDuration);

        // Schedule test termination
        scheduleDurationTermination(plan.testDuration);

        log.info("Starting main load generation phase");

        // Main request generation loop
        executeRequestGenerationLoop(testPlanSpec, plan, testEndTime);

        // Wait for remaining requests to complete
        waitForActiveRequestsToComplete(config.getRequestCompletionTimeout());

        // Mark completion if not already terminated
        if (phaseManager.isTestRunning() && !cancelled.get()) {
            log.info("Duration completed, terminating test");
            phaseManager.terminateTest("DURATION_COMPLETED");
        }
    }

    private void executeRequestGenerationLoop(TestPlanSpec testPlanSpec, ExecutionPlan plan, Instant testEndTime) {
        while (phaseManager.isTestRunning() && !cancelled.get() && Instant.now().isBefore(testEndTime)) {

            try {
                // Rate limiting - blocks until permit is available
                rateLimiter.acquire();

                if (!phaseManager.isTestRunning() || cancelled.get() || Instant.now().isAfter(testEndTime)) {
                    break;
                }

                // Try to acquire concurrency permit
                if (concurrencyLimiter.tryAcquire()) {
                    requestsGenerated.incrementAndGet();

                    // Submit request for execution
                    resourceManager.submitTrackedTask(() -> {
                        try {
                            requestExecutor.executeAllRequests(testPlanSpec, phaseManager, -1, false, concurrencyLimiter, cancelled);
                            successfulRequests.incrementAndGet();
                        } catch (Exception e) {
                            log.debug("Request execution failed: {}", e.getMessage());
                        }
                    }, "open-workload-request");

                } else {
                    // Concurrency limit reached - log back pressure
                    backPressureEvents.incrementAndGet();
                    requestExecutor.logBackPressureEvent(phaseManager.getCurrentPhase().name());
                }

            } catch (Exception e) {
                if (!cancelled.get()) {
                    log.debug("Request generation interrupted: {}", e.getMessage());
                }
                break;
            }
        }

        log.info("Request generation loop completed. Generated {} requests, {} back-pressure events",
                requestsGenerated.get(), backPressureEvents.get());
    }

    private void scheduleDurationTermination(Duration testDuration) {
        durationTask = resourceManager.getSchedulerService().schedule(() -> {
            if (phaseManager.isTestRunning() && !cancelled.get()) {
                log.info("Test duration expired, terminating");
                phaseManager.terminateTest("DURATION_COMPLETED");
            }
        }, testDuration.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void waitForActiveRequestsToComplete(Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        int activeRequests;

        log.debug("Waiting up to {}s for active requests to complete", timeout.getSeconds());

        while ((activeRequests = requestExecutor.getActiveRequestCount()) > 0 &&
                !cancelled.get() &&
                Instant.now().isBefore(deadline)) {

            timingUtils.sleep(Duration.ofMillis(100), cancelled);
        }

        if (activeRequests > 0) {
            log.warn("Timeout waiting for {} active requests to complete after {}s",
                    activeRequests, timeout.getSeconds());
        } else {
            log.debug("All active requests completed");
        }
    }

    private void cancelAllScheduledTasks() {
        if (warmupTask != null) {
            warmupTask.cancel(false);
        }
        if (durationTask != null) {
            durationTask.cancel(false);
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

        // Create a minimal execution plan for progress calculation
        ExecutionPlan plan = new ExecutionPlan(Duration.ZERO, Duration.ofMinutes(1),
                rateLimiter != null ? (int) rateLimiter.getRate() : 1,
                concurrencyLimiter != null ? concurrencyLimiter.availablePermits() : 1);

        return getCurrentProgress(plan);
    }

    /**
     * Returns current metrics for monitoring.
     */
    public OpenWorkloadMetrics getMetrics() {
        int currentConcurrency = concurrencyLimiter != null ?
                concurrencyLimiter.availablePermits() : 0;
        double currentRate = rateLimiter != null ? rateLimiter.getRate() : 0.0;

        return new OpenWorkloadMetrics(
                requestsGenerated.get(),
                successfulRequests.get(),
                backPressureEvents.get(),
                currentRate,
                currentConcurrency,
                executionStartTime != null ? Duration.between(executionStartTime, Instant.now()) : Duration.ZERO
        );
    }

    /**
     * Simple metrics holder for open workload execution.
     */
    public static class OpenWorkloadMetrics {
        private final long requestsGenerated;
        private final long successfulRequests;
        private final long backPressureEvents;
        private final double currentRate;
        private final int currentConcurrency;
        private final Duration elapsedTime;

        public OpenWorkloadMetrics(long requestsGenerated, long successfulRequests, long backPressureEvents,
                                   double currentRate, int currentConcurrency, Duration elapsedTime) {
            this.requestsGenerated = requestsGenerated;
            this.successfulRequests = successfulRequests;
            this.backPressureEvents = backPressureEvents;
            this.currentRate = currentRate;
            this.currentConcurrency = currentConcurrency;
            this.elapsedTime = elapsedTime;
        }

        public long getRequestsGenerated() { return requestsGenerated; }
        public long getSuccessfulRequests() { return successfulRequests; }
        public long getBackPressureEvents() { return backPressureEvents; }
        public double getCurrentRate() { return currentRate; }
        public int getCurrentConcurrency() { return currentConcurrency; }
        public Duration getElapsedTime() { return elapsedTime; }

        public double getSuccessRate() {
            return requestsGenerated > 0 ? (double) successfulRequests / requestsGenerated * 100 : 100;
        }

        public double getActualRate() {
            return elapsedTime.getSeconds() > 0 ? (double) requestsGenerated / elapsedTime.getSeconds() : 0;
        }

        @Override
        public String toString() {
            return String.format("OpenWorkloadMetrics[generated=%d, successful=%d, backpressure=%d, " +
                            "rate=%.1f req/s, concurrency=%d, elapsed=%ds]",
                    requestsGenerated, successfulRequests, backPressureEvents, getActualRate(),
                    currentConcurrency, elapsedTime.getSeconds());
        }
    }
}