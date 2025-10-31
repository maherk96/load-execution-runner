package com.mk.fx.qa.load.execution.metrics;

import java.time.Instant;

/** One time-window sample used to plot evolution of load and latency. */
public record TimeSeriesPoint(
    Instant timestamp,
    long totalRequests,
    long totalErrors,
    long latMinMs,
    long latMaxMs,
    long latAvgMs,
    int usersStarted,
    int usersCompleted) {}
