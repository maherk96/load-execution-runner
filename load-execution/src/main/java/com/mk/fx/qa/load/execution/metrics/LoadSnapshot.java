package com.mk.fx.qa.load.execution.metrics;

import java.util.Map;

public record LoadSnapshot(
    TaskConfig config,
    int usersStarted,
    int usersCompleted,
    long totalRequests,
    long totalErrors,
    Double achievedRps,
    Long latencyMinMs,
    Long latencyAvgMs,
    Long latencyMaxMs,
    Map<Integer, Integer> activeUserIterations) {}
