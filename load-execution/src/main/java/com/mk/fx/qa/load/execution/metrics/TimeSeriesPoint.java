package com.mk.fx.qa.load.execution.metrics;

import java.time.Instant;

public record TimeSeriesPoint(
    Instant timestamp,
    long totalRequests,
    long totalErrors,
    long latMinMs,
    long latMaxMs,
    long latAvgMs,
    int usersStarted,
    int usersCompleted) {}
