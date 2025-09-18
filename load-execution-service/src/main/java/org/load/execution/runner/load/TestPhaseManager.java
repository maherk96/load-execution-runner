// File: org/load/execution/runner/load/phase/TestPhaseManager.java
package org.load.execution.runner.load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages test phases and state transitions during load test execution.
 *
 * @author Load Test Framework
 * @since 1.0
 */
public class TestPhaseManager {
    private static final Logger log = LoggerFactory.getLogger(TestPhaseManager.class);

    public enum TestPhase {
        INITIALIZING, WARMUP, RAMP_UP, HOLD, COMPLETED, TERMINATED
    }

    private final String testId;
    private final SolacePublisher solacePublisher;
    private final AtomicReference<TestPhase> currentPhase = new AtomicReference<>(TestPhase.INITIALIZING);
    private final AtomicBoolean testRunning = new AtomicBoolean(false);
    private final CountDownLatch testCompletionLatch = new CountDownLatch(1);
    private final AtomicReference<String> terminationReason = new AtomicReference<>();
    private final AtomicInteger activeUsers = new AtomicInteger(0);

    public TestPhaseManager(String testId, SolacePublisher solacePublisher) {
        this.testId = testId;
        this.solacePublisher = solacePublisher;
    }

    public void startTest() {
        testRunning.set(true);
        setPhase(TestPhase.INITIALIZING);
    }

    public void setPhase(TestPhase newPhase) {
        TestPhase oldPhase = currentPhase.getAndSet(newPhase);
        log.debug("Phase transition: {} -> {}", oldPhase, newPhase);

        // Publish phase transition event
        var event = new PhaseTransitionEvent(
                testId,
                oldPhase,
                newPhase,
                Instant.now(),
                activeUsers.get()
        );
        solacePublisher.publishPhaseTransition(event);
    }

    public TestPhase getCurrentPhase() {
        return currentPhase.get();
    }

    public boolean isTestRunning() {
        return testRunning.get();
    }

    public void updateActiveUsers(int count) {
        activeUsers.set(count);
    }

    public int getActiveUsers() {
        return activeUsers.get();
    }

    public void terminateTest(String reason) {
        if (testRunning.compareAndSet(true, false)) {
            setPhase(TestPhase.TERMINATED);
            terminationReason.set(reason);
            testCompletionLatch.countDown();
            log.info("Test termination initiated: {}", reason);
        }
    }

    public void completeTest() {
        if (testRunning.compareAndSet(true, false)) {
            setPhase(TestPhase.COMPLETED);
            terminationReason.compareAndSet(null, "COMPLETED");
            testCompletionLatch.countDown();
        }
    }

    public String getTerminationReason() {
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