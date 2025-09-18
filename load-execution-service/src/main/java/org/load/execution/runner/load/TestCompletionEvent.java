package org.load.execution.runner.load;

import java.time.Instant;

public record TestCompletionEvent(
    String testId,
    Instant endTime,
    String finalMetrics,
    String terminationReason,
    long totalDurationSeconds
) {}