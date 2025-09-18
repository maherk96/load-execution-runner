package org.load.execution.runner.load;

import java.time.Instant;

public record TestStartEvent(
    String testId,
    String loadModelType,
    int users,
    String duration,
    int expectedTPS,
    String[] targetEndpoints,
    String environment,
    Instant startTime
) {}