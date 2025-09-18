package org.load.execution.runner.load;

import java.time.Instant;

public record ResourceMetricsEvent(
    String testId,
    Instant timestamp,
    long memoryUsage,
    double cpuUtilization,
    int activeThreads,
    int queueSizes,
    int networkConnections
) {}