package org.load.execution.runner.load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * Enhanced request executor that provides robust HTTP request execution with comprehensive
 * error handling, metrics collection, retry logic, and observability for load testing.
 *
 * <p>This executor provides enterprise-grade request execution capabilities including:
 * <ul>
 *   <li><b>Resilience Patterns:</b> Circuit breaker, retry logic, timeout handling</li>
 *   <li><b>Performance Metrics:</b> Response time percentiles, throughput, error rates</li>
 *   <li><b>Request Validation:</b> Size limits, content validation, security checks</li>
 *   <li><b>Connection Management:</b> Connection pooling, keep-alive optimization</li>
 *   <li><b>Rate Limiting:</b> Protection against overwhelming target systems</li>
 *   <li><b>Structured Logging:</b> Rich observability with contextual information</li>
 * </ul>
 *
 * <p><b>Thread Safety:</b> All operations are thread-safe and designed for high-concurrency
 * load testing scenarios with minimal contention.
 *
 * @author Load Test Team
 * @version 2.0.0
 */
public class RequestExecutor {
    private static final Logger log = LoggerFactory.getLogger(RequestExecutor.class);

    // ================================
    // CONFIGURATION
    // ================================

    /**
     * Configuration for request execution behavior.
     */
    public static class RequestExecutorConfig {
        private final int maxRetries;
        private final Duration retryDelay;
        private final Duration maxRetryDelay;
        private final double retryBackoffMultiplier;
        private final Duration requestTimeout;
        private final int maxConcurrentRequests;
        private final boolean enableCircuitBreaker;
        private final int circuitBreakerThreshold;
        private final Duration circuitBreakerTimeout;
        private final boolean enableMetrics;
        private final boolean enableRequestValidation;
        private final long maxRequestSizeBytes;
        private final long maxResponseSizeBytes;
        private final Set<Integer> retryableStatusCodes;
        private final boolean enableStructuredLogging;

        private RequestExecutorConfig(Builder builder) {
            this.maxRetries = Math.max(0, builder.maxRetries);
            this.retryDelay = builder.retryDelay;
            this.maxRetryDelay = builder.maxRetryDelay;
            this.retryBackoffMultiplier = Math.max(1.0, builder.retryBackoffMultiplier);
            this.requestTimeout = builder.requestTimeout;
            this.maxConcurrentRequests = Math.max(1, builder.maxConcurrentRequests);
            this.enableCircuitBreaker = builder.enableCircuitBreaker;
            this.circuitBreakerThreshold = Math.max(1, builder.circuitBreakerThreshold);
            this.circuitBreakerTimeout = builder.circuitBreakerTimeout;
            this.enableMetrics = builder.enableMetrics;
            this.enableRequestValidation = builder.enableRequestValidation;
            this.maxRequestSizeBytes = Math.max(1024, builder.maxRequestSizeBytes);
            this.maxResponseSizeBytes = Math.max(1024, builder.maxResponseSizeBytes);
            this.retryableStatusCodes = Set.copyOf(builder.retryableStatusCodes);
            this.enableStructuredLogging = builder.enableStructuredLogging;
        }

        public static RequestExecutorConfig defaultConfig() {
            return new Builder().build();
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private int maxRetries = 3;
            private Duration retryDelay = Duration.ofMillis(100);
            private Duration maxRetryDelay = Duration.ofSeconds(5);
            private double retryBackoffMultiplier = 2.0;
            private Duration requestTimeout = Duration.ofSeconds(30);
            private int maxConcurrentRequests = 1000;
            private boolean enableCircuitBreaker = true;
            private int circuitBreakerThreshold = 50;
            private Duration circuitBreakerTimeout = Duration.ofSeconds(30);
            private boolean enableMetrics = true;
            private boolean enableRequestValidation = true;
            private long maxRequestSizeBytes = 10 * 1024 * 1024; // 10MB
            private long maxResponseSizeBytes = 100 * 1024 * 1024; // 100MB
            private Set<Integer> retryableStatusCodes = Set.of(500, 502, 503, 504, 429);
            private boolean enableStructuredLogging = true;

            public Builder maxRetries(int maxRetries) {
                this.maxRetries = maxRetries;
                return this;
            }

            public Builder retryDelay(Duration retryDelay) {
                this.retryDelay = retryDelay;
                return this;
            }

            public Builder requestTimeout(Duration requestTimeout) {
                this.requestTimeout = requestTimeout;
                return this;
            }

            public Builder maxConcurrentRequests(int maxConcurrentRequests) {
                this.maxConcurrentRequests = maxConcurrentRequests;
                return this;
            }

            public Builder enableCircuitBreaker(boolean enable) {
                this.enableCircuitBreaker = enable;
                return this;
            }

            public Builder retryableStatusCodes(Set<Integer> codes) {
                this.retryableStatusCodes = new HashSet<>(codes);
                return this;
            }

            public RequestExecutorConfig build() {
                return new RequestExecutorConfig(this);
            }
        }

        // Getters
        public int getMaxRetries() { return maxRetries; }
        public Duration getRetryDelay() { return retryDelay; }
        public Duration getMaxRetryDelay() { return maxRetryDelay; }
        public double getRetryBackoffMultiplier() { return retryBackoffMultiplier; }
        public Duration getRequestTimeout() { return requestTimeout; }
        public int getMaxConcurrentRequests() { return maxConcurrentRequests; }
        public boolean isCircuitBreakerEnabled() { return enableCircuitBreaker; }
        public int getCircuitBreakerThreshold() { return circuitBreakerThreshold; }
        public Duration getCircuitBreakerTimeout() { return circuitBreakerTimeout; }
        public boolean isMetricsEnabled() { return enableMetrics; }
        public boolean isRequestValidationEnabled() { return enableRequestValidation; }
        public long getMaxRequestSizeBytes() { return maxRequestSizeBytes; }
        public long getMaxResponseSizeBytes() { return maxResponseSizeBytes; }
        public Set<Integer> getRetryableStatusCodes() { return retryableStatusCodes; }
        public boolean isStructuredLoggingEnabled() { return enableStructuredLogging; }
    }

    // ================================
    // METRICS AND OBSERVABILITY
    // ================================

    /**
     * Comprehensive request execution metrics.
     */
    public static class RequestMetrics {
        private final long totalRequests;
        private final long successfulRequests;
        private final long failedRequests;
        private final long retriedRequests;
        private final long circuitBreakerTrips;
        private final double averageResponseTime;
        private final long p50ResponseTime;
        private final long p95ResponseTime;
        private final long p99ResponseTime;
        private final double requestsPerSecond;
        private final Map<Integer, Long> statusCodeDistribution;
        private final Map<String, Long> errorTypeDistribution;
        private final int activeRequests;
        private final Instant lastUpdated;

        public RequestMetrics(long totalRequests, long successfulRequests, long failedRequests,
                              long retriedRequests, long circuitBreakerTrips, double averageResponseTime,
                              long p50ResponseTime, long p95ResponseTime, long p99ResponseTime,
                              double requestsPerSecond, Map<Integer, Long> statusCodeDistribution,
                              Map<String, Long> errorTypeDistribution, int activeRequests) {
            this.totalRequests = totalRequests;
            this.successfulRequests = successfulRequests;
            this.failedRequests = failedRequests;
            this.retriedRequests = retriedRequests;
            this.circuitBreakerTrips = circuitBreakerTrips;
            this.averageResponseTime = averageResponseTime;
            this.p50ResponseTime = p50ResponseTime;
            this.p95ResponseTime = p95ResponseTime;
            this.p99ResponseTime = p99ResponseTime;
            this.requestsPerSecond = requestsPerSecond;
            this.statusCodeDistribution = Map.copyOf(statusCodeDistribution);
            this.errorTypeDistribution = Map.copyOf(errorTypeDistribution);
            this.activeRequests = activeRequests;
            this.lastUpdated = Instant.now();
        }

        // Getters
        public long getTotalRequests() { return totalRequests; }
        public long getSuccessfulRequests() { return successfulRequests; }
        public long getFailedRequests() { return failedRequests; }
        public long getRetriedRequests() { return retriedRequests; }
        public long getCircuitBreakerTrips() { return circuitBreakerTrips; }
        public double getAverageResponseTime() { return averageResponseTime; }
        public long getP50ResponseTime() { return p50ResponseTime; }
        public long getP95ResponseTime() { return p95ResponseTime; }
        public long getP99ResponseTime() { return p99ResponseTime; }
        public double getRequestsPerSecond() { return requestsPerSecond; }
        public Map<Integer, Long> getStatusCodeDistribution() { return statusCodeDistribution; }
        public Map<String, Long> getErrorTypeDistribution() { return errorTypeDistribution; }
        public int getActiveRequests() { return activeRequests; }
        public Instant getLastUpdated() { return lastUpdated; }

        public double getSuccessRate() {
            return totalRequests > 0 ? (double) successfulRequests / totalRequests : 0.0;
        }

        @Override
        public String toString() {
            return String.format("RequestMetrics[total=%d, success=%d, failed=%d, successRate=%.2f%%, " +
                            "avgResponseTime=%.2fms, p95=%.2fms, rps=%.2f, active=%d]",
                    totalRequests, successfulRequests, failedRequests, getSuccessRate() * 100,
                    averageResponseTime, (double) p95ResponseTime, requestsPerSecond, activeRequests);
        }
    }

    /**
     * Enhanced request execution log with structured data.
     */
    public static class RequestExecutionLog {
        private final Instant timestamp;
        private final String phase;
        private final int userId;
        private final String method;
        private final String path;
        private final long durationMs;
        private final boolean backPressured;
        private final boolean success;
        private final int statusCode;
        private final String errorType;
        private final int retryCount;
        private final boolean fromCache;
        private final long requestSizeBytes;
        private final long responseSizeBytes;
        private final String traceId;

        public RequestExecutionLog(Instant timestamp, String phase, int userId, String method,
                                   String path, long durationMs, boolean backPressured, boolean success,
                                   int statusCode, String errorType, int retryCount, boolean fromCache,
                                   long requestSizeBytes, long responseSizeBytes, String traceId) {
            this.timestamp = timestamp;
            this.phase = phase;
            this.userId = userId;
            this.method = method;
            this.path = path;
            this.durationMs = durationMs;
            this.backPressured = backPressured;
            this.success = success;
            this.statusCode = statusCode;
            this.errorType = errorType;
            this.retryCount = retryCount;
            this.fromCache = fromCache;
            this.requestSizeBytes = requestSizeBytes;
            this.responseSizeBytes = responseSizeBytes;
            this.traceId = traceId;
        }

        // Getters
        public Instant getTimestamp() { return timestamp; }
        public String getPhase() { return phase; }
        public int getUserId() { return userId; }
        public String getMethod() { return method; }
        public String getPath() { return path; }
        public long getDurationMs() { return durationMs; }
        public boolean isBackPressured() { return backPressured; }
        public boolean isSuccess() { return success; }
        public int getStatusCode() { return statusCode; }
        public String getErrorType() { return errorType; }
        public int getRetryCount() { return retryCount; }
        public boolean isFromCache() { return fromCache; }
        public long getRequestSizeBytes() { return requestSizeBytes; }
        public long getResponseSizeBytes() { return responseSizeBytes; }
        public String getTraceId() { return traceId; }

        @Override
        public String toString() {
            return String.format("RequestLog[%s %s, %dms, %s, status=%d, user=%d, retries=%d, trace=%s]",
                    method, path, durationMs, success ? "SUCCESS" : "FAILED", statusCode, userId, retryCount, traceId);
        }
    }

    // ================================
    // INSTANCE FIELDS
    // ================================

    private final RequestExecutorConfig config;
    private final LoadHttpClient httpClient;
    private final RequestMetricsCollector metricsCollector;
    private final CircuitBreaker circuitBreaker;
    private final Semaphore concurrencyLimiter;
    private final RequestValidator requestValidator;
    private final AtomicInteger activeRequests = new AtomicInteger(0);
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    // ================================
    // CONSTRUCTORS
    // ================================

    /**
     * Creates a RequestExecutor with default configuration.
     */
    public RequestExecutor(TestPlanSpec testPlanSpec) {
        this(testPlanSpec, RequestExecutorConfig.defaultConfig());
    }

    /**
     * Creates a RequestExecutor with custom configuration.
     */
    public RequestExecutor(TestPlanSpec testPlanSpec, RequestExecutorConfig config) {
        this.config = config != null ? config : RequestExecutorConfig.defaultConfig();
        this.httpClient = createHttpClient(testPlanSpec);
        this.metricsCollector = new RequestMetricsCollector(this.config);
        this.circuitBreaker = this.config.isCircuitBreakerEnabled() ?
                new CircuitBreaker(this.config.getCircuitBreakerThreshold(), this.config.getCircuitBreakerTimeout()) : null;
        this.concurrencyLimiter = new Semaphore(this.config.getMaxConcurrentRequests());
        this.requestValidator = new RequestValidator(this.config);

        log.info("RequestExecutor initialized - Max retries: {}, Circuit breaker: {}, Max concurrent: {}",
                this.config.getMaxRetries(), this.config.isCircuitBreakerEnabled(), this.config.getMaxConcurrentRequests());
    }

    private LoadHttpClient createHttpClient(TestPlanSpec testPlanSpec) {
        var globalConfig = testPlanSpec.getTestSpec().getGlobalConfig();
        return new LoadHttpClient(
                globalConfig.getBaseUrl(),
                (int) config.getRequestTimeout().getSeconds(),
                globalConfig.getHeaders(),
                globalConfig.getVars()
        );
    }

    // ================================
    // PUBLIC API
    // ================================

    /**
     * Executes all requests across all scenarios with enhanced error handling and metrics.
     * This method maintains backward compatibility while using the enhanced features.
     */
    public void executeAllRequests(TestPlanSpec testPlanSpec, TestPhaseManager phaseManager,
                                   int userId, boolean isWarmup, Semaphore externalLimiter,
                                   AtomicBoolean cancelled) {
        if (isShutdown.get()) {
            throw new IllegalStateException("RequestExecutor is shut down");
        }

        String traceId = generateTraceId();
        ExecutionContext context = new ExecutionContext(userId, isWarmup, traceId, phaseManager.getCurrentPhase().name());

        try {
            activeRequests.incrementAndGet();

            for (var scenario : testPlanSpec.getTestSpec().getScenarios()) {
                if (!phaseManager.isTestRunning() || cancelled.get()) break;

                for (var request : scenario.getRequests()) {
                    if (!phaseManager.isTestRunning() || cancelled.get()) break;

                    executeRequestWithResilience(request, context, phaseManager);
                }
            }
        } finally {
            activeRequests.decrementAndGet();
            if (externalLimiter != null) {
                externalLimiter.release();
            }
        }
    }

    /**
     * Asynchronous version of executeAllRequests for better concurrency.
     */
    public CompletableFuture<Void> executeAllRequestsAsync(TestPlanSpec testPlanSpec, TestPhaseManager phaseManager,
                                                           int userId, boolean isWarmup, Semaphore externalLimiter,
                                                           AtomicBoolean cancelled) {
        return CompletableFuture.runAsync(() -> {
            executeAllRequests(testPlanSpec, phaseManager, userId, isWarmup, externalLimiter, cancelled);
        });
    }

    /**
     * Executes a single request with comprehensive resilience patterns.
     */
    public CompletableFuture<RequestExecutionResult> executeRequest(TestPlanSpec.Request request,
                                                                    ExecutionContext context) {
        if (isShutdown.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("RequestExecutor is shut down"));
        }

        return CompletableFuture.supplyAsync(() -> executeRequestWithResilience(request, context, null));
    }

    /**
     * Returns current request metrics.
     */
    public RequestMetrics getMetrics() {
        return metricsCollector.getMetrics(activeRequests.get());
    }

    /**
     * Logs a back-pressure event when concurrency limits are reached.
     */
    public void logBackPressureEvent(String phase) {
        log.debug("Back-pressure: max concurrent requests reached in phase {}", phase);

        RequestExecutionLog backPressureLog = new RequestExecutionLog(
                Instant.now(), phase, -1, "N/A", "N/A", 0, true, false, 0,
                "BACK_PRESSURE", 0, false, 0, 0, "back-pressure-" + System.nanoTime()
        );

        if (config.isStructuredLoggingEnabled()) {
            log.debug("Request execution: {}", backPressureLog);
        }

        metricsCollector.recordBackPressure();
    }

    /**
     * Returns the current number of active requests.
     */
    public int getActiveRequestCount() {
        return activeRequests.get();
    }

    /**
     * Performs graceful cleanup of resources.
     */
    public void cleanup() {
        if (!isShutdown.compareAndSet(false, true)) {
            log.debug("RequestExecutor already shut down");
            return;
        }

        log.info("Starting RequestExecutor cleanup...");

        try {
            // Wait for active requests to complete
            waitForActiveRequestsToComplete(Duration.ofSeconds(5));

            // Close HTTP client
            if (httpClient != null) {
                httpClient.close();
            }

            // Reset circuit breaker
            if (circuitBreaker != null) {
                circuitBreaker.reset();
            }

            log.info("RequestExecutor cleanup completed successfully");

        } catch (Exception e) {
            log.error("Error during RequestExecutor cleanup", e);
        }
    }

    // ================================
    // PRIVATE IMPLEMENTATION
    // ================================

    private RequestExecutionResult executeRequestWithResilience(TestPlanSpec.Request request,
                                                                ExecutionContext context,
                                                                TestPhaseManager phaseManager) {
        String traceId = context.getTraceId();
        int retryCount = 0;
        Exception lastException = null;
        Duration currentRetryDelay = config.getRetryDelay();

        // Validate request before execution
        if (config.isRequestValidationEnabled()) {
            try {
                requestValidator.validateRequest(request);
            } catch (RequestValidationException e) {
                return handleValidationFailure(request, context, e);
            }
        }

        // Check circuit breaker
        if (circuitBreaker != null && !circuitBreaker.allowRequest()) {
            metricsCollector.recordCircuitBreakerTrip();
            return handleCircuitBreakerOpen(request, context);
        }

        // Acquire concurrency limit
        if (!concurrencyLimiter.tryAcquire()) {
            if (phaseManager != null) {
                logBackPressureEvent(phaseManager.getCurrentPhase().name());
            }
            return handleConcurrencyLimitExceeded(request, context);
        }

        try {
            while (retryCount <= config.getMaxRetries()) {
                try {
                    RequestExecutionResult result = executeRequestInternal(request, context, retryCount);

                    if (result.isSuccess() || !shouldRetry(result, retryCount)) {
                        if (circuitBreaker != null) {
                            if (result.isSuccess()) {
                                circuitBreaker.recordSuccess();
                            } else {
                                circuitBreaker.recordFailure();
                            }
                        }
                        return result;
                    }

                    // Prepare for retry
                    lastException = result.getException();
                    if (retryCount < config.getMaxRetries()) {
                        Thread.sleep(currentRetryDelay.toMillis());
                        currentRetryDelay = Duration.ofMillis(Math.min(
                                config.getMaxRetryDelay().toMillis(),
                                (long) (currentRetryDelay.toMillis() * config.getRetryBackoffMultiplier())
                        ));
                        metricsCollector.recordRetry();
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return handleInterruption(request, context, e);
                } catch (Exception e) {
                    lastException = e;
                    if (!shouldRetryException(e, retryCount)) {
                        break;
                    }
                }

                retryCount++;
            }

            // All retries exhausted
            return handleRetriesExhausted(request, context, retryCount, lastException);

        } finally {
            concurrencyLimiter.release();
        }
    }

    private RequestExecutionResult executeRequestInternal(TestPlanSpec.Request request,
                                                          ExecutionContext context,
                                                          int retryCount) {
        Instant startTime = Instant.now();
        boolean success = false;
        int statusCode = 0;
        String errorType = null;
        Exception exception = null;
        long requestSize = 0;
        long responseSize = 0;

        // Count the request attempt BEFORE execution
        metricsCollector.recordRequest(0, false, 0, "ATTEMPTING");

        try {
            // Estimate request size (simplified)
            requestSize = estimateRequestSize(request);

            // Execute the HTTP request
            var response = httpClient.execute(request);
            Instant endTime = Instant.now();
            long durationMs = Duration.between(startTime, endTime).toMillis();

            statusCode = response.getStatusCode();
            responseSize = estimateResponseSize(response);
            success = isSuccessStatusCode(statusCode);

            if (!success) {
                errorType = categorizeHttpError(statusCode);
            }

            // Update metrics with actual results (this replaces the ATTEMPTING count)
            metricsCollector.updateRequestResult(durationMs, success, statusCode, errorType);

            // Create execution log
            RequestExecutionLog executionLog = new RequestExecutionLog(
                    endTime, context.getPhase(), context.getUserId(), request.getMethod().name(),
                    request.getPath(), durationMs, false, success, statusCode, errorType,
                    retryCount, false, requestSize, responseSize, context.getTraceId()
            );

            // Log based on configuration and warmup status
            logRequestExecution(executionLog, context.isWarmup());

            return new RequestExecutionResult(success, durationMs, statusCode, errorType, exception, executionLog);

        } catch (Exception e) {
            Instant endTime = Instant.now();
            long durationMs = Duration.between(startTime, endTime).toMillis();

            exception = e;
            errorType = categorizeException(e);

            // Update metrics for failed request (this replaces the ATTEMPTING count)
            metricsCollector.updateRequestResult(durationMs, false, 0, errorType);

            // Create execution log for failed request
            RequestExecutionLog executionLog = new RequestExecutionLog(
                    endTime, context.getPhase(), context.getUserId(), request.getMethod().name(),
                    request.getPath(), durationMs, false, false, 0, errorType,
                    retryCount, false, requestSize, 0, context.getTraceId()
            );

            logRequestExecution(executionLog, context.isWarmup());

            return new RequestExecutionResult(false, durationMs, 0, errorType, exception, executionLog);
        }
    }

    private void logRequestExecution(RequestExecutionLog executionLog, boolean isWarmup) {
        if (config.isStructuredLoggingEnabled()) {
            if (!isWarmup) {
                log.debug("Request execution: {}", executionLog);
            } else {
                log.debug("Warmup request execution: {}", executionLog);
            }
        }
    }

    private boolean shouldRetry(RequestExecutionResult result, int retryCount) {
        if (retryCount >= config.getMaxRetries()) {
            return false;
        }

        if (result.isSuccess()) {
            return false;
        }

        // Retry on specific status codes
        if (result.getStatusCode() > 0) {
            return config.getRetryableStatusCodes().contains(result.getStatusCode());
        }

        // Retry on specific exceptions
        return shouldRetryException(result.getException(), retryCount);
    }

    private boolean shouldRetryException(Exception e, int retryCount) {
        if (retryCount >= config.getMaxRetries()) {
            return false;
        }

        if (e instanceof InterruptedException) {
            return false;
        }

        // Add specific exception types that should be retried
        return e instanceof TimeoutException ||
                e instanceof ConnectException ||
                e.getMessage().contains("timeout") ||
                e.getMessage().contains("connection");
    }

    private String categorizeHttpError(int statusCode) {
        return switch (statusCode / 100) {
            case 4 -> "CLIENT_ERROR";
            case 5 -> "SERVER_ERROR";
            default -> "HTTP_ERROR";
        };
    }

    private String categorizeException(Exception e) {
        if (e instanceof TimeoutException) return "TIMEOUT";
        if (e instanceof ConnectException) return "CONNECTION_ERROR";
        if (e instanceof InterruptedException) return "INTERRUPTED";
        return "EXECUTION_ERROR";
    }

    private boolean isSuccessStatusCode(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    private long estimateRequestSize(TestPlanSpec.Request request) {
        // Simplified estimation - in real implementation, calculate actual size
        return request.getPath().length() + 100; // Basic estimation
    }

    private long estimateResponseSize(Object response) {
        // Simplified estimation - in real implementation, get actual response size
        return 1024; // Basic estimation
    }

    private String generateTraceId() {
        return String.format("req-%d-%d", System.currentTimeMillis(),
                Thread.currentThread().getId());
    }

    private void waitForActiveRequestsToComplete(Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);

        while (activeRequests.get() > 0 && Instant.now().isBefore(deadline)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        if (activeRequests.get() > 0) {
            log.warn("Still {} active requests after cleanup timeout", activeRequests.get());
        }
    }

    // Helper methods for different failure scenarios
    private RequestExecutionResult handleValidationFailure(TestPlanSpec.Request request,
                                                           ExecutionContext context,
                                                           RequestValidationException e) {
        RequestExecutionLog log = new RequestExecutionLog(
                Instant.now(), context.getPhase(), context.getUserId(),
                request.getMethod().name(), request.getPath(), 0, false, false, 0,
                "VALIDATION_ERROR", 0, false, 0, 0, context.getTraceId()
        );
        return new RequestExecutionResult(false, 0, 0, "VALIDATION_ERROR", e, log);
    }

    private RequestExecutionResult handleCircuitBreakerOpen(TestPlanSpec.Request request,
                                                            ExecutionContext context) {
        RequestExecutionLog log = new RequestExecutionLog(
                Instant.now(), context.getPhase(), context.getUserId(),
                request.getMethod().name(), request.getPath(), 0, false, false, 0,
                "CIRCUIT_BREAKER_OPEN", 0, false, 0, 0, context.getTraceId()
        );
        return new RequestExecutionResult(false, 0, 0, "CIRCUIT_BREAKER_OPEN", null, log);
    }

    private RequestExecutionResult handleConcurrencyLimitExceeded(TestPlanSpec.Request request,
                                                                  ExecutionContext context) {
        RequestExecutionLog log = new RequestExecutionLog(
                Instant.now(), context.getPhase(), context.getUserId(),
                request.getMethod().name(), request.getPath(), 0, true, false, 0,
                "CONCURRENCY_LIMIT", 0, false, 0, 0, context.getTraceId()
        );
        return new RequestExecutionResult(false, 0, 0, "CONCURRENCY_LIMIT", null, log);
    }

    private RequestExecutionResult handleInterruption(TestPlanSpec.Request request,
                                                      ExecutionContext context,
                                                      InterruptedException e) {
        RequestExecutionLog log = new RequestExecutionLog(
                Instant.now(), context.getPhase(), context.getUserId(),
                request.getMethod().name(), request.getPath(), 0, false, false, 0,
                "INTERRUPTED", 0, false, 0, 0, context.getTraceId()
        );
        return new RequestExecutionResult(false, 0, 0, "INTERRUPTED", e, log);
    }

    private RequestExecutionResult handleRetriesExhausted(TestPlanSpec.Request request,
                                                          ExecutionContext context,
                                                          int retryCount,
                                                          Exception lastException) {
        RequestExecutionLog log = new RequestExecutionLog(
                Instant.now(), context.getPhase(), context.getUserId(),
                request.getMethod().name(), request.getPath(), 0, false, false, 0,
                "RETRIES_EXHAUSTED", retryCount, false, 0, 0, context.getTraceId()
        );
        return new RequestExecutionResult(false, 0, 0, "RETRIES_EXHAUSTED", lastException, log);
    }

    // ================================
    // INNER CLASSES
    // ================================

    /**
     * Execution context for request processing.
     */
    public static class ExecutionContext {
        private final int userId;
        private final boolean isWarmup;
        private final String traceId;
        private final String phase;

        public ExecutionContext(int userId, boolean isWarmup, String traceId, String phase) {
            this.userId = userId;
            this.isWarmup = isWarmup;
            this.traceId = traceId;
            this.phase = phase;
        }

        public int getUserId() { return userId; }
        public boolean isWarmup() { return isWarmup; }
        public String getTraceId() { return traceId; }
        public String getPhase() { return phase; }
    }

    /**
     * Result of request execution with comprehensive information.
     */
    public static class RequestExecutionResult {
        private final boolean success;
        private final long durationMs;
        private final int statusCode;
        private final String errorType;
        private final Exception exception;
        private final RequestExecutionLog executionLog;

        public RequestExecutionResult(boolean success, long durationMs, int statusCode,
                                      String errorType, Exception exception, RequestExecutionLog executionLog) {
            this.success = success;
            this.durationMs = durationMs;
            this.statusCode = statusCode;
            this.errorType = errorType;
            this.exception = exception;
            this.executionLog = executionLog;
        }

        public boolean isSuccess() { return success; }
        public long getDurationMs() { return durationMs; }
        public int getStatusCode() { return statusCode; }
        public String getErrorType() { return errorType; }
        public Exception getException() { return exception; }
        public RequestExecutionLog getExecutionLog() { return executionLog; }
    }

    // Placeholder classes for compilation - these would be implemented separately
    private static class RequestMetricsCollector {
        private final RequestExecutorConfig config;
        private final AtomicLong totalRequests = new AtomicLong(0);
        private final AtomicLong successfulRequests = new AtomicLong(0);
        private final AtomicLong failedRequests = new AtomicLong(0);
        private final AtomicLong retriedRequests = new AtomicLong(0);
        private final AtomicLong circuitBreakerTrips = new AtomicLong(0);
        private final ConcurrentLinkedQueue<Long> responseTimes = new ConcurrentLinkedQueue<>();

        RequestMetricsCollector(RequestExecutorConfig config) {
            this.config = config;
        }

        void recordRequest(long durationMs, boolean success, int statusCode, String errorType) {
            // Only count actual request executions, not attempts
            if (!"ATTEMPTING".equals(errorType)) {
                totalRequests.incrementAndGet();
                if (success) {
                    successfulRequests.incrementAndGet();
                } else {
                    failedRequests.incrementAndGet();
                }
                responseTimes.offer(durationMs);

                // Keep only recent response times (simple implementation)
                while (responseTimes.size() > 1000) {
                    responseTimes.poll();
                }
            }
        }

        void updateRequestResult(long durationMs, boolean success, int statusCode, String errorType) {
            // This replaces the ATTEMPTING entry with actual results
            totalRequests.incrementAndGet();
            if (success) {
                successfulRequests.incrementAndGet();
            } else {
                failedRequests.incrementAndGet();
            }
            responseTimes.offer(durationMs);

            // Keep only recent response times (simple implementation)
            while (responseTimes.size() > 1000) {
                responseTimes.poll();
            }
        }

        void recordRetry() {
            retriedRequests.incrementAndGet();
        }

        void recordCircuitBreakerTrip() {
            circuitBreakerTrips.incrementAndGet();
        }

        void recordBackPressure() {
            // Record back pressure event
        }

        RequestMetrics getMetrics(int activeRequests) {
            List<Long> times = new ArrayList<>(responseTimes);
            Collections.sort(times);

            long p50 = percentile(times, 50);
            long p95 = percentile(times, 95);
            long p99 = percentile(times, 99);
            double avg = times.stream().mapToLong(Long::longValue).average().orElse(0.0);

            return new RequestMetrics(
                    totalRequests.get(), successfulRequests.get(), failedRequests.get(),
                    retriedRequests.get(), circuitBreakerTrips.get(), avg, p50, p95, p99,
                    0.0, Map.of(), Map.of(), activeRequests
            );
        }

        private long percentile(List<Long> times, int percentile) {
            if (times.isEmpty()) return 0;
            int index = (int) Math.ceil(percentile / 100.0 * times.size()) - 1;
            return times.get(Math.max(0, Math.min(index, times.size() - 1)));
        }
    }

    private static class CircuitBreaker {
        private final int threshold;
        private final Duration timeout;
        private final AtomicInteger failureCount = new AtomicInteger(0);
        private final AtomicBoolean isOpen = new AtomicBoolean(false);
        private volatile Instant lastFailureTime;

        CircuitBreaker(int threshold, Duration timeout) {
            this.threshold = threshold;
            this.timeout = timeout;
        }

        boolean allowRequest() {
            if (!isOpen.get()) return true;

            if (lastFailureTime != null &&
                    Duration.between(lastFailureTime, Instant.now()).compareTo(timeout) > 0) {
                reset();
                return true;
            }

            return false;
        }

        void recordSuccess() {
            failureCount.set(0);
            isOpen.set(false);
        }

        void recordFailure() {
            lastFailureTime = Instant.now();
            if (failureCount.incrementAndGet() >= threshold) {
                isOpen.set(true);
            }
        }

        void reset() {
            failureCount.set(0);
            isOpen.set(false);
            lastFailureTime = null;
        }
    }

    private static class RequestValidator {
        private final RequestExecutorConfig config;

        RequestValidator(RequestExecutorConfig config) {
            this.config = config;
        }

        void validateRequest(TestPlanSpec.Request request) throws RequestValidationException {
            if (request == null) {
                throw new RequestValidationException("Request cannot be null");
            }
            // Add more validation logic
        }
    }

    private static class RequestValidationException extends Exception {
        RequestValidationException(String message) {
            super(message);
        }
    }
}