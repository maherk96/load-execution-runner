
// ===== LOAD METRICS =====
// File: org/load/execution/runner/load/metrics/LoadMetrics.java
package org.load.execution.runner.load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks load test metrics and provides progress reporting functionality.
 *
 * @author Load Test Framework
 * @since 1.0
 */
public class LoadMetrics {

    private static final Logger log = LoggerFactory.getLogger(LoadMetrics.class);

    private final TestPlanSpec testPlanSpec;
    private final int expectedTotalRequests;
    private final AtomicInteger completedRequests = new AtomicInteger(0);
    private final AtomicInteger successfulRequests = new AtomicInteger(0);
    private final AtomicInteger failedRequests = new AtomicInteger(0);
    private final AtomicLong totalResponseTime = new AtomicLong(0);
    private final AtomicLong minResponseTime = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxResponseTime = new AtomicLong(0);

    private volatile Instant testStartTime;
    private volatile Instant lastProgressLog = Instant.now();
    private volatile int lastLoggedProgress = 0;

    public LoadMetrics(TestPlanSpec testPlanSpec) {
        this.testPlanSpec = testPlanSpec;
        this.expectedTotalRequests = calculateExpectedRequests(testPlanSpec);
    }

    public void startTest() {
        this.testStartTime = Instant.now();
        log.info("Load test metrics started - Expected total requests: {}", expectedTotalRequests);
    }

    public void recordRequest(boolean success, long responseTimeMs) {
        int completed = completedRequests.incrementAndGet();
        
        if (success) {
            successfulRequests.incrementAndGet();
        } else {
            failedRequests.incrementAndGet();
        }

        totalResponseTime.addAndGet(responseTimeMs);
        updateMinMax(responseTimeMs);

        // Log progress periodically
        logProgressIfNeeded(completed);
    }

    public void completeTest() {
        log.info("Load test metrics completed - Final summary:");
        log.info(getFinalSummary());
    }

    public String getFinalSummary() {
        int completed = completedRequests.get();
        int successful = successfulRequests.get();
        int failed = failedRequests.get();
        double successRate = completed > 0 ? (successful * 100.0 / completed) : 0.0;
        double avgResponseTime = completed > 0 ? (totalResponseTime.get() / (double) completed) : 0.0;

        return String.format(
            "Requests: %d/%d (%.1f%%), Success: %d (%.1f%%), Failed: %d, " +
            "Response Times - Avg: %.1fms, Min: %dms, Max: %dms",
            completed, expectedTotalRequests, 
            expectedTotalRequests > 0 ? (completed * 100.0 / expectedTotalRequests) : 100.0,
            successful, successRate, failed,
            avgResponseTime, 
            minResponseTime.get() == Long.MAX_VALUE ? 0 : minResponseTime.get(),
            maxResponseTime.get()
        );
    }

    private int calculateExpectedRequests(TestPlanSpec testPlanSpec) {
        var loadModel = testPlanSpec.getExecution().getLoadModel();
        int requestsPerIteration = testPlanSpec.getTestSpec().getScenarios().stream()
            .mapToInt(scenario -> scenario.getRequests().size())
            .sum();

        return switch (loadModel.getType()) {
            case CLOSED -> loadModel.getUsers() * loadModel.getIterations() * requestsPerIteration;
            case OPEN -> {
                var duration = parseDurationSeconds(loadModel.getDuration());
                yield (int) (loadModel.getArrivalRatePerSec() * duration * requestsPerIteration);
            }
        };
    }

    private void logProgressIfNeeded(int completed) {
        Instant now = Instant.now();
        
        // Log every 10% progress or every 30 seconds, whichever comes first
        double currentProgress = expectedTotalRequests > 0 ? (completed * 100.0 / expectedTotalRequests) : 0.0;
        int currentProgressPercent = (int) currentProgress;
        boolean timeTrigger = now.minusSeconds(30).isAfter(lastProgressLog);
        boolean progressTrigger = currentProgressPercent >= lastLoggedProgress + 10;

        if (progressTrigger || timeTrigger) {
            double successRate = completed > 0 ? (successfulRequests.get() * 100.0 / completed) : 0.0;
            double avgResponseTime = completed > 0 ? (totalResponseTime.get() / (double) completed) : 0.0;

            log.info("Progress: {}/{} requests ({}%) - Success: {}% - Avg Response: {}ms",
                completed, expectedTotalRequests, (int) currentProgress,
                (int) successRate, (int) avgResponseTime);

            lastProgressLog = now;
            lastLoggedProgress = (currentProgressPercent / 10) * 10; // Round down to nearest 10%
        }
    }

    private void updateMinMax(long responseTime) {
        minResponseTime.updateAndGet(current -> Math.min(current, responseTime));
        maxResponseTime.updateAndGet(current -> Math.max(current, responseTime));
    }

    private long parseDurationSeconds(String duration) {
        if (duration == null || duration.trim().isEmpty()) return 0;
        
        String trimmed = duration.trim().toLowerCase();
        if (trimmed.endsWith("s")) {
            return Long.parseLong(trimmed.substring(0, trimmed.length() - 1));
        } else if (trimmed.endsWith("m")) {
            return Long.parseLong(trimmed.substring(0, trimmed.length() - 1)) * 60;
        } else {
            return Long.parseLong(trimmed);
        }
    }
}
