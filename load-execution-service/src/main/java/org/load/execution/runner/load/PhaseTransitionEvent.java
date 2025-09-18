package org.load.execution.runner.load;

import java.time.Instant;

public record PhaseTransitionEvent(
    String testId,
    TestPhaseManager.TestPhase fromPhase,
    TestPhaseManager.TestPhase toPhase,
    Instant timestamp,
    int activeUsers
) {}