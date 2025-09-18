package org.load.execution.runner.load;

import java.time.Instant;

public record CriticalEvent(
    String testId,
    Instant timestamp,
    String eventType,
    String severity,
    String description,
    String impactLevel
) {}