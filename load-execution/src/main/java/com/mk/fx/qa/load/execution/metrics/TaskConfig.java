package com.mk.fx.qa.load.execution.metrics;

import com.mk.fx.qa.load.execution.model.LoadModelType;
import java.time.Duration;

public record TaskConfig(
    String taskId,
    String taskType,
    String baseUrl,
    LoadModelType model,
    Integer users,
    Integer iterationsPerUser,
    Duration warmup,
    Duration rampUp,
    Duration holdFor,
    Double arrivalRatePerSec,
    Duration duration,
    int requestsPerIteration,
    long expectedTotalRequests,
    Double expectedRps) {}
