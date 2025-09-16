package org.load.execution.runner.load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Enhanced test phase manager that provides robust lifecycle management with state validation,
 * metrics collection, and comprehensive error handling for load test execution.
 *
 * <p>This manager enforces valid phase transitions using a state machine approach and provides
 * detailed observability into test execution progress. It supports graceful termination,
 * timeout handling, and progress monitoring.
 *
 * <p><b>State Machine:</b>
 * <pre>
 * INITIALIZING → WARMUP → RAMP_UP → HOLD → COMPLETED
 *      ↓           ↓         ↓       ↓        ↓
 *   TERMINATED  TERMINATED TERMINATED TERMINATED TERMINATED
 * </pre>
 *
 * <p><b>Thread Safety:</b> All operations are thread-safe and can be called concurrently
 * from multiple threads. State transitions are atomic and validated.
 *
 * @author Load Test Team
 * @version 2.0.0
 */
public class TestPhaseManager {
    private static final Logger log = LoggerFactory.getLogger(TestPhaseManager.class);

    /**
     * Test execution phases with valid transition definitions.
     */
    public enum TestPhase {
        /** Initial phase when test is being set up */
        INITIALIZING,

        /** Optional warmup phase for system preparation */
        WARMUP,

        /** Gradual increase in load */
        RAMP_UP,

        /** Sustained load execution */
        HOLD,

        /** Successful test completion */
        COMPLETED,

        /** Test terminated before completion */
        TERMINATED;

        // Static map to avoid enum initialization issues
        private static final Map<TestPhase, Set<TestPhase>> ALLOWED_TRANSITIONS = Map.of(
                INITIALIZING, Set.of(WARMUP, RAMP_UP, HOLD, TERMINATED),
                WARMUP, Set.of(RAMP_UP, HOLD, TERMINATED),
                RAMP_UP, Set.of(HOLD, TERMINATED),
                HOLD, Set.of(COMPLETED, TERMINATED),
                COMPLETED, Set.of(),
                TERMINATED, Set.of()
        );

        /**
         * Checks if transition to target phase is valid.
         *
         * @param target the target phase
         * @return true if transition is allowed
         */
        public boolean canTransitionTo(TestPhase target) {
            return ALLOWED_TRANSITIONS.get(this).contains(target);
        }

        /**
         * Returns the set of allowed transition phases.
         *
         * @return immutable set of allowed next phases
         */
        public Set<TestPhase> getAllowedTransitions() {
            return ALLOWED_TRANSITIONS.get(this);
        }

        /**
         * Checks if this is a terminal phase (no further transitions allowed).
         *
         * @return true if this is a terminal phase
         */
        public boolean isTerminal() {
            return ALLOWED_TRANSITIONS.get(this).isEmpty();
        }
    }

    /**
     * Reasons for test termination.
     */
    public enum TerminationReason {
        COMPLETED("Test completed successfully"),
        CANCELLED("Test cancelled by user"),
        TIMEOUT("Test terminated due to timeout"),
        ERROR("Test terminated due to error"),
        DURATION_EXPIRED("Test duration limit reached"),
        ITERATIONS_COMPLETED("All iterations completed"),
        HOLD_TIME_EXPIRED("Hold time duration expired"),
        EXTERNAL_SIGNAL("External termination signal received"),
        RESOURCE_EXHAUSTION("System resources exhausted"),
        UNKNOWN("Unknown termination reason");

        private final String description;

        TerminationReason(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    /**
     * Configuration for test phase management behavior.
     */
    public static class PhaseManagerConfig {
        private final Duration maxWaitTimeout;
        private final boolean enableMetrics;
        private final boolean strictTransitions;

        public PhaseManagerConfig(Duration maxWaitTimeout, boolean enableMetrics, boolean strictTransitions) {
            this.maxWaitTimeout = maxWaitTimeout != null ? maxWaitTimeout : Duration.ofMinutes(5);
            this.enableMetrics = enableMetrics;
            this.strictTransitions = strictTransitions;
        }

        public static PhaseManagerConfig defaultConfig() {
            return new PhaseManagerConfig(Duration.ofMinutes(5), true, true);
        }

        public static PhaseManagerConfig lenientConfig() {
            return new PhaseManagerConfig(Duration.ofMinutes(10), true, false);
        }

        public Duration getMaxWaitTimeout() { return maxWaitTimeout; }
        public boolean isMetricsEnabled() { return enableMetrics; }
        public boolean isStrictTransitions() { return strictTransitions; }
    }

    /**
     * Listener interface for phase transition events.
     */
    @FunctionalInterface
    public interface PhaseTransitionListener {
        void onPhaseTransition(TestPhase fromPhase, TestPhase toPhase, Instant timestamp);
    }

    /**
     * Immutable snapshot of current test state.
     */
    public static class TestState {
        private final TestPhase currentPhase;
        private final boolean isRunning;
        private final String terminationReason;
        private final Instant startTime;
        private final Instant lastTransitionTime;
        private final Duration elapsedTime;

        public TestState(TestPhase currentPhase, boolean isRunning, String terminationReason,
                         Instant startTime, Instant lastTransitionTime) {
            this.currentPhase = currentPhase;
            this.isRunning = isRunning;
            this.terminationReason = terminationReason;
            this.startTime = startTime;
            this.lastTransitionTime = lastTransitionTime;
            this.elapsedTime = startTime != null ? Duration.between(startTime, Instant.now()) : Duration.ZERO;
        }

        public TestPhase getCurrentPhase() { return currentPhase; }
        public boolean isRunning() { return isRunning; }
        public String getTerminationReason() { return terminationReason; }
        public Instant getStartTime() { return startTime; }
        public Instant getLastTransitionTime() { return lastTransitionTime; }
        public Duration getElapsedTime() { return elapsedTime; }

        @Override
        public String toString() {
            return String.format("TestState{phase=%s, running=%s, reason='%s', elapsed=%ds}",
                    currentPhase, isRunning, terminationReason, elapsedTime.getSeconds());
        }
    }

    // ================================
    // INSTANCE FIELDS
    // ================================

    private final PhaseManagerConfig config;
    private final AtomicReference<TestPhase> currentPhase = new AtomicReference<>(TestPhase.INITIALIZING);
    private final AtomicBoolean testRunning = new AtomicBoolean(false);
    private final CountDownLatch testCompletionLatch = new CountDownLatch(1);
    private final AtomicReference<String> terminationReason = new AtomicReference<>();
    private final AtomicReference<PhaseTransitionListener> transitionListener = new AtomicReference<>();

    // Timing and metrics
    private volatile Instant testStartTime;
    private volatile Instant lastTransitionTime;
    private final AtomicReference<Exception> lastError = new AtomicReference<>();

    // ================================
    // CONSTRUCTORS
    // ================================

    /**
     * Creates a new TestPhaseManager with default configuration.
     */
    public TestPhaseManager() {
        this(PhaseManagerConfig.defaultConfig());
    }

    /**
     * Creates a new TestPhaseManager with specified configuration.
     *
     * @param config the configuration for phase management behavior
     */
    public TestPhaseManager(PhaseManagerConfig config) {
        this.config = config != null ? config : PhaseManagerConfig.defaultConfig();
        this.lastTransitionTime = Instant.now();
    }

    // ================================
    // LIFECYCLE MANAGEMENT
    // ================================

    /**
     * Starts the test execution and initializes timing.
     *
     * @throws IllegalStateException if test is already running
     */
    public void startTest() {
        if (testRunning.compareAndSet(false, true)) {
            testStartTime = Instant.now();
            setPhaseInternal(TestPhase.INITIALIZING, "Test started");
            log.info("Test execution started");
        } else {
            throw new IllegalStateException("Test is already running");
        }
    }

    /**
     * Attempts to transition to the specified phase with validation.
     *
     * @param targetPhase the phase to transition to
     * @throws InvalidPhaseTransitionException if transition is not allowed
     * @throws IllegalStateException if test is not running
     */
    public void setPhase(TestPhase targetPhase) {
        if (!testRunning.get()) {
            throw new IllegalStateException("Cannot change phase when test is not running");
        }

        TestPhase current = currentPhase.get();

        // Validate transition
        if (config.isStrictTransitions() && !current.canTransitionTo(targetPhase)) {
            String message = String.format("Invalid phase transition from %s to %s. Allowed transitions: %s",
                    current, targetPhase, current.getAllowedTransitions());
            throw new InvalidPhaseTransitionException(message);
        }

        setPhaseInternal(targetPhase, String.format("Phase transition: %s → %s", current, targetPhase));
    }

    /**
     * Terminates the test with the specified reason.
     *
     * @param reason the termination reason
     */
    public void terminateTest(String reason) {
        terminateTest(reason, null);
    }

    /**
     * Terminates the test with the specified reason and optional error.
     *
     * @param reason the termination reason
     * @param error optional error that caused termination
     */
    public void terminateTest(String reason, Exception error) {
        if (testRunning.compareAndSet(true, false)) {
            if (error != null) {
                lastError.set(error);
            }

            setPhaseInternal(TestPhase.TERMINATED, reason);
            terminationReason.set(reason);
            testCompletionLatch.countDown();

            log.info("Test termination initiated: {}", reason);
            if (error != null) {
                log.debug("Termination error details", error);
            }
        } else {
            log.debug("Test termination already in progress or completed");
        }
    }

    /**
     * Completes the test successfully.
     */
    public void completeTest() {
        if (testRunning.compareAndSet(true, false)) {
            setPhaseInternal(TestPhase.COMPLETED, "Test completed successfully");
            terminationReason.compareAndSet(null, TerminationReason.COMPLETED.name());
            testCompletionLatch.countDown();
            log.info("Test completed successfully");
        }
    }

    // ================================
    // STATE QUERY METHODS
    // ================================

    /**
     * Returns the current test phase.
     *
     * @return current phase
     */
    public TestPhase getCurrentPhase() {
        return currentPhase.get();
    }

    /**
     * Checks if the test is currently running.
     *
     * @return true if test is running
     */
    public boolean isTestRunning() {
        return testRunning.get();
    }

    /**
     * Returns the termination reason if test has terminated.
     *
     * @return termination reason or null if not terminated
     */
    public String getTerminationReason() {
        return terminationReason.get();
    }

    /**
     * Returns a complete snapshot of the current test state.
     *
     * @return immutable test state snapshot
     */
    public TestState getTestState() {
        return new TestState(
                currentPhase.get(),
                testRunning.get(),
                terminationReason.get(),
                testStartTime,
                lastTransitionTime
        );
    }

    /**
     * Returns the last error that occurred during test execution.
     *
     * @return last error or null if no error occurred
     */
    public Exception getLastError() {
        return lastError.get();
    }

    // ================================
    // WAITING AND SYNCHRONIZATION
    // ================================

    /**
     * Waits for test completion indefinitely.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    public void waitForCompletion() throws InterruptedException {
        testCompletionLatch.await();
    }

    /**
     * Waits for test completion with timeout.
     *
     * @param timeout maximum time to wait
     * @param unit time unit
     * @return true if test completed within timeout
     * @throws InterruptedException if interrupted while waiting
     */
    public boolean waitForCompletion(long timeout, TimeUnit unit) throws InterruptedException {
        return testCompletionLatch.await(timeout, unit);
    }

    /**
     * Waits for test completion with configured maximum timeout.
     *
     * @throws InterruptedException if interrupted while waiting
     * @throws TimeoutException if timeout is exceeded
     */
    public void waitForCompletionWithTimeout() throws InterruptedException, TimeoutException {
        if (!testCompletionLatch.await(config.getMaxWaitTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("Test completion wait timed out after " + config.getMaxWaitTimeout());
        }
    }

    // ================================
    // LISTENER MANAGEMENT
    // ================================

    /**
     * Sets a listener for phase transition events.
     *
     * @param listener the transition listener
     */
    public void setTransitionListener(PhaseTransitionListener listener) {
        this.transitionListener.set(listener);
    }

    /**
     * Removes the current transition listener.
     */
    public void removeTransitionListener() {
        this.transitionListener.set(null);
    }

    // ================================
    // CLEANUP AND RESOURCE MANAGEMENT
    // ================================

    /**
     * Performs cleanup operations and ensures test is stopped.
     * This method is idempotent and safe to call multiple times.
     */
    public void cleanup() {
        if (testRunning.get()) {
            log.warn("Cleanup called while test is still running - forcing termination");
            terminateTest("CLEANUP_FORCED");
        }

        // Clear any references to help GC
        transitionListener.set(null);
        lastError.set(null);

        log.debug("TestPhaseManager cleanup completed");
    }

    // ================================
    // PRIVATE HELPER METHODS
    // ================================

    /**
     * Internal phase transition method that handles common logic.
     */
    private void setPhaseInternal(TestPhase newPhase, String logMessage) {
        TestPhase previousPhase = currentPhase.getAndSet(newPhase);
        lastTransitionTime = Instant.now();

        // Notify listener if present
        PhaseTransitionListener listener = transitionListener.get();
        if (listener != null) {
            try {
                listener.onPhaseTransition(previousPhase, newPhase, lastTransitionTime);
            } catch (Exception e) {
                log.warn("Error in phase transition listener", e);
            }
        }

        log.debug("{}", logMessage);

        // Log metrics if enabled
        if (config.isMetricsEnabled()) {
            logPhaseMetrics(previousPhase, newPhase);
        }
    }

    /**
     * Logs phase transition metrics.
     */
    private void logPhaseMetrics(TestPhase from, TestPhase to) {
        if (testStartTime != null) {
            Duration totalElapsed = Duration.between(testStartTime, lastTransitionTime);
            log.info("Phase transition metrics: {} → {} (total elapsed: {}s)",
                    from, to, totalElapsed.getSeconds());
        }
    }

    // ================================
    // CUSTOM EXCEPTIONS
    // ================================

    /**
     * Exception thrown when an invalid phase transition is attempted.
     */
    public static class InvalidPhaseTransitionException extends RuntimeException {
        public InvalidPhaseTransitionException(String message) {
            super(message);
        }

        public InvalidPhaseTransitionException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // ================================
    // UTILITY METHODS
    // ================================

    /**
     * Returns a string representation of the current state for debugging.
     */
    @Override
    public String toString() {
        TestState state = getTestState();
        return String.format("TestPhaseManager{%s}", state);
    }

    /**
     * Checks if the current phase allows the test to continue processing.
     *
     * @return true if test should continue processing
     */
    public boolean shouldContinueProcessing() {
        TestPhase phase = currentPhase.get();
        return testRunning.get() && !phase.isTerminal();
    }

    /**
     * Returns the duration the test has been in the current phase.
     *
     * @return duration in current phase
     */
    public Duration getTimeInCurrentPhase() {
        return lastTransitionTime != null ?
                Duration.between(lastTransitionTime, Instant.now()) :
                Duration.ZERO;
    }
}