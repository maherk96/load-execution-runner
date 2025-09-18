package org.load.execution.runner.load;

import java.time.Instant;

public record MetricsSummaryEvent(
    String testId,
    Instant timestamp,
    double currentTPS,
    double averageResponseTime,
    double errorRate,
    int activeUsers,
    int completedRequests,
    long p50ResponseTime,
    long p95ResponseTime,
    long p99ResponseTime
) {}