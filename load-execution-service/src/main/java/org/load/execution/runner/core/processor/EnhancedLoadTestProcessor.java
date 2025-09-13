package org.load.execution.runner.core.processor;

import org.load.execution.runner.api.dto.TaskDto;
import org.load.execution.runner.core.model.TaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class EnhancedLoadTestProcessor implements InterruptibleTaskProcessor, ValidatableTaskProcessor {
    private static final Logger logger = LoggerFactory.getLogger(EnhancedLoadTestProcessor.class);

    private final Random random = new Random();
    private volatile ExecutorService loadTestExecutor;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    @Override
    public TaskType getTaskType() {
        return TaskType.LOAD_TEST; // Fixed: should match the actual task type
    }

    @Override
    public void processTask(TaskDto task) throws Exception {
        String taskId = task.getTaskId();
        Map<String, Object> data = task.getData();

        logger.info("Starting load test execution for task: {}", taskId);
        cancelled.set(false);

        // Extract load test parameters
        String targetUrl = (String) data.getOrDefault("targetUrl", "https://httpbin.org/delay/1");
        int concurrentUsers = (Integer) data.getOrDefault("concurrentUsers", 10);
        int durationSeconds = (Integer) data.getOrDefault("durationSeconds", 30);
        int requestsPerSecond = (Integer) data.getOrDefault("requestsPerSecond", 5);

        logger.info("Load test parameters - URL: {}, Users: {}, Duration: {}s, RPS: {}",
                targetUrl, concurrentUsers, durationSeconds, requestsPerSecond);

        // Create thread pool for simulating concurrent users
        loadTestExecutor = Executors.newFixedThreadPool(concurrentUsers, r -> {
            Thread t = new Thread(r, "LoadTestWorker-" + taskId);
            t.setDaemon(false);
            return t;
        });

        try {
            executeLoadTest(taskId, targetUrl, concurrentUsers, durationSeconds, requestsPerSecond);
        } finally {
            shutdownExecutor();
        }

        logger.info("Load test completed for task: {}", taskId);
    }

    private void executeLoadTest(String taskId, String targetUrl, int concurrentUsers,
                                 int durationSeconds, int requestsPerSecond) throws Exception {

        AtomicInteger totalRequests = new AtomicInteger(0);
        AtomicInteger successfulRequests = new AtomicInteger(0);
        AtomicInteger failedRequests = new AtomicInteger(0);
        AtomicLong totalResponseTime = new AtomicLong(0);

        long testStartTime = System.currentTimeMillis();
        long testEndTime = testStartTime + (durationSeconds * 1000L);

        // Calculate delay between requests per user
        long requestDelayMs = Math.max(1000 / (requestsPerSecond / concurrentUsers), 100);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(concurrentUsers);

        // Start concurrent users
        for (int userId = 0; userId < concurrentUsers; userId++) {
            final int userNumber = userId + 1;

            loadTestExecutor.submit(() -> {
                try {
                    MDC.put("taskId", taskId);
                    MDC.put("userId", String.valueOf(userNumber));

                    // Wait for all users to be ready
                    startLatch.await();

                    logger.debug("User {} started load testing", userNumber);

                    while (System.currentTimeMillis() < testEndTime && !cancelled.get()) {
                        // Check for interruption
                        if (Thread.currentThread().isInterrupted()) {
                            logger.info("User {} interrupted", userNumber);
                            break;
                        }

                        try {
                            // Simulate HTTP request
                            long requestStart = System.currentTimeMillis();
                            simulateHttpRequest(targetUrl, userNumber);
                            long requestTime = System.currentTimeMillis() - requestStart;

                            totalRequests.incrementAndGet();
                            successfulRequests.incrementAndGet();
                            totalResponseTime.addAndGet(requestTime);

                            // Wait before next request
                            Thread.sleep(requestDelayMs + random.nextInt(100)); // Add some jitter

                        } catch (InterruptedException e) {
                            logger.info("User {} interrupted during request", userNumber);
                            Thread.currentThread().interrupt();
                            break;
                        } catch (Exception e) {
                            failedRequests.incrementAndGet();
                            totalRequests.incrementAndGet();
                            logger.debug("User {} request failed: {}", userNumber, e.getMessage());
                        }
                    }

                    logger.debug("User {} finished load testing", userNumber);

                } catch (Exception e) {
                    logger.error("Error in user {} execution: {}", userNumber, e.getMessage());
                } finally {
                    MDC.remove("userId");
                    MDC.remove("taskId");
                    completionLatch.countDown();
                }
            });
        }

        // Start all users simultaneously
        startLatch.countDown();

        // Monitor progress and check for cancellation
        monitorProgress(taskId, testEndTime, totalRequests, successfulRequests, failedRequests);

        // Wait for all users to complete or timeout
        boolean completed = completionLatch.await(durationSeconds + 30, TimeUnit.SECONDS);
        if (!completed) {
            logger.warn("Load test did not complete within expected time");
        }

        // Log final results
        logTestResults(taskId, totalRequests.get(), successfulRequests.get(),
                failedRequests.get(), totalResponseTime.get(), durationSeconds);
    }

    private void simulateHttpRequest(String targetUrl, int userId) throws Exception {
        // Simulate different types of requests with realistic timing
        int requestType = random.nextInt(4);

        switch (requestType) {
            case 0: // Fast request (GET)
                Thread.sleep(50 + random.nextInt(100));
                break;
            case 1: // Medium request (POST)
                Thread.sleep(100 + random.nextInt(200));
                break;
            case 2: // Slow request (complex query)
                Thread.sleep(200 + random.nextInt(300));
                break;
            case 3: // Very slow request (file upload simulation)
                Thread.sleep(500 + random.nextInt(500));
                break;
        }

        // Randomly simulate some failures (5% failure rate)
        if (random.nextInt(100) < 5) {
            throw new RuntimeException("Simulated HTTP 500 error");
        }
    }

    private void monitorProgress(String taskId, long testEndTime, AtomicInteger totalRequests,
                                 AtomicInteger successfulRequests, AtomicInteger failedRequests) {

        CompletableFuture.runAsync(() -> {
            try {
                while (System.currentTimeMillis() < testEndTime && !cancelled.get()) {
                    Thread.sleep(5000); // Report every 5 seconds

                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }

                    int total = totalRequests.get();
                    int successful = successfulRequests.get();
                    int failed = failedRequests.get();
                    double successRate = total > 0 ? (successful * 100.0 / total) : 0;

                    logger.info("Load test progress - Total: {}, Success: {}, Failed: {}, Success Rate: {:.1f}%",
                            total, successful, failed, successRate);
                }
            } catch (InterruptedException e) {
                logger.info("Progress monitoring interrupted");
                Thread.currentThread().interrupt();
            }
        });
    }

    private void logTestResults(String taskId, int totalRequests, int successfulRequests,
                                int failedRequests, long totalResponseTime, int durationSeconds) {

        double successRate = totalRequests > 0 ? (successfulRequests * 100.0 / totalRequests) : 0;
        double avgResponseTime = successfulRequests > 0 ? (totalResponseTime / (double) successfulRequests) : 0;
        double throughput = totalRequests / (double) durationSeconds;

        logger.info("=== Load Test Results for {} ===", taskId);
        logger.info("Duration: {} seconds", durationSeconds);
        logger.info("Total Requests: {}", totalRequests);
        logger.info("Successful Requests: {}", successfulRequests);
        logger.info("Failed Requests: {}", failedRequests);
        logger.info("Success Rate: {:.2f}%", successRate);
        logger.info("Average Response Time: {:.2f} ms", avgResponseTime);
        logger.info("Throughput: {:.2f} requests/second", throughput);
        logger.info("=== End Results ===");
    }

    @Override
    public void validateTask(TaskDto task) throws IllegalArgumentException {
        logger.info("Validating load test task: {}", task.getTaskId());
        Map<String, Object> data = task.getData();

        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Task data cannot be null or empty");
        }

        // Validate target URL
        String targetUrl = (String) data.get("targetUrl");
        if (targetUrl != null && !isValidUrl(targetUrl)) {
            throw new IllegalArgumentException("Invalid target URL: " + targetUrl);
        }

        // Validate concurrentUsers
        Object concurrentUsersObj = data.get("concurrentUsers");
        if (concurrentUsersObj != null) {
            try {
                int concurrentUsers = (Integer) concurrentUsersObj;
                if (concurrentUsers < 1 || concurrentUsers > 1000) {
                    throw new IllegalArgumentException("Concurrent users must be between 1 and 1000");
                }
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("concurrentUsers must be an integer");
            }
        }

        // Validate duration
        Object durationObj = data.get("durationSeconds");
        if (durationObj != null) {
            try {
                int duration = (Integer) durationObj;
                if (duration < 1 || duration > 3600) { // Max 1 hour
                    throw new IllegalArgumentException("Duration must be between 1 and 3600 seconds");
                }
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("durationSeconds must be an integer");
            }
        }

        // Validate requests per second
        Object rpsObj = data.get("requestsPerSecond");
        if (rpsObj != null) {
            try {
                int rps = (Integer) rpsObj;
                if (rps < 1 || rps > 10000) {
                    throw new IllegalArgumentException("Requests per second must be between 1 and 10000");
                }
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("requestsPerSecond must be an integer");
            }
        }
    }

    private boolean isValidUrl(String url) {
        if (url == null || url.trim().isEmpty()) {
            return false;
        }
        return url.matches("^https?://[\\w\\.-]+(:\\d+)?(/.*)?$");
    }

    @Override
    public void cancelTask(TaskDto task) {
        logger.info("Cancelling load test task: {}", task.getTaskId());
        cancelled.set(true);
        shutdownExecutor();
    }

    private void shutdownExecutor() {
        if (loadTestExecutor != null && !loadTestExecutor.isShutdown()) {
            logger.info("Shutting down load test executor");
            loadTestExecutor.shutdownNow();

            try {
                if (!loadTestExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    logger.warn("Load test executor did not terminate within 10 seconds");
                }
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for executor termination");
                Thread.currentThread().interrupt();
            }
        }
    }
}