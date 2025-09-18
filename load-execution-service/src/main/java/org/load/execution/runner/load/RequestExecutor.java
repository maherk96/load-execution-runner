// ===== PHASE MANAGEMENT =====

// ===== REQUEST EXECUTION =====
// File: org/load/execution/runner/load/execution/RequestExecutor.java
package org.load.execution.runner.load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles HTTP request execution, logging, and metrics collection during load tests.
 *
 * @author Load Test Framework
 * @since 1.0
 */
public class RequestExecutor {

    private static final Logger log = LoggerFactory.getLogger(RequestExecutor.class);

    private final String testId;
    private final LoadHttpClient httpClient;
    private final SolacePublisher solacePublisher;
    private final AtomicInteger activeRequests = new AtomicInteger(0);

    public RequestExecutor(TestPlanSpec testPlanSpec, SolacePublisher solacePublisher) {
        this.testId = testPlanSpec.getTestSpec().getId();
        this.solacePublisher = solacePublisher;

        var globalConfig = testPlanSpec.getTestSpec().getGlobalConfig();
        this.httpClient = new LoadHttpClient(
                globalConfig.getBaseUrl(),
                globalConfig.getTimeouts().getConnectionTimeoutMs() / 1000,
                globalConfig.getHeaders(),
                globalConfig.getVars()
        );
    }

    public void executeAllRequests(TestPlanSpec testPlanSpec, TestPhaseManager phaseManager,
                                   int userId, boolean isWarmup, Semaphore concurrencyLimiter,
                                   AtomicBoolean cancelled, LoadMetrics loadMetrics) {
        try {
            activeRequests.incrementAndGet();

            for (var scenario : testPlanSpec.getTestSpec().getScenarios()) {
                if (!phaseManager.isTestRunning() || cancelled.get()) break;

                for (var request : scenario.getRequests()) {
                    if (!phaseManager.isTestRunning() || cancelled.get()) break;
                    executeRequest(request, phaseManager.getCurrentPhase(), userId, isWarmup, loadMetrics);
                }
            }
        } finally {
            activeRequests.decrementAndGet();
            if (concurrencyLimiter != null) {
                concurrencyLimiter.release();
            }
        }
    }

    private void executeRequest(TestPlanSpec.Request request, TestPhaseManager.TestPhase phase,
                                int userId, boolean isWarmup, LoadMetrics loadMetrics) {
        var requestStart = Instant.now();
        boolean success = false;
        int statusCode = 0;
        String errorMessage = null;

        try {
            var response = httpClient.execute(request);
            var requestEnd = Instant.now();
            long responseTime = Duration.between(requestStart, requestEnd).toMillis();

            statusCode = response.getStatusCode();
            success = statusCode >= 200 && statusCode < 300;

            if (!success) {
                errorMessage = "HTTP " + statusCode + " response";
            }

            if (!isWarmup && loadMetrics != null) {
                loadMetrics.recordRequest(success, responseTime);
            }

            logRequestExecution(phase, userId, request, responseTime, false, success, statusCode, isWarmup);

            // Publish error details for failed requests
            if (!success && !isWarmup) {
                publishErrorDetail(userId, statusCode, errorMessage, request.getPath());
            }

            log.debug("Request completed: {} ms, status: {}, user: {}", responseTime, statusCode, userId);

        } catch (Exception e) {
            var requestEnd = Instant.now();
            long responseTime = Duration.between(requestStart, requestEnd).toMillis();
            errorMessage = e.getMessage();

            if (!isWarmup && loadMetrics != null) {
                loadMetrics.recordRequest(success, responseTime);
            }

            logRequestExecution(phase, userId, request, responseTime, false, success, statusCode, isWarmup);

            // Publish error details for exceptions
            if (!isWarmup) {
                publishErrorDetail(userId, statusCode, "Exception: " + errorMessage, request.getPath());
            }

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
            log.debug("Request execution: {}", executionLog);
            solacePublisher.publishRequestExecution(executionLog);
        } else {
            log.debug("Warmup request execution: {}", executionLog);
        }
    }

    public void logBackPressureEvent(TestPhaseManager.TestPhase phase) {
        log.debug("Back-pressure: max concurrent requests reached");
        var backPressureLog = new RequestExecutionLog(
                Instant.now(), phase, -1, "N/A", "N/A", 0, true, false, 0
        );
        log.debug("Request execution: {}", backPressureLog);
        solacePublisher.publishRequestExecution(backPressureLog);
    }

    private void publishErrorDetail(int userId, int statusCode, String errorMessage, String requestUrl) {
        var errorEvent = new ErrorDetailEvent(
                testId,
                Instant.now(),
                userId,
                statusCode,
                statusCode > 0 ? "HTTP_ERROR" : "NETWORK_ERROR",
                errorMessage,
                requestUrl,
                0 // retryAttempt - could be enhanced later
        );

        solacePublisher.publishErrorDetail(errorEvent);
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
}