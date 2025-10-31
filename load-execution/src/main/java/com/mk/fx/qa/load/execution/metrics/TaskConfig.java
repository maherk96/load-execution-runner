package com.mk.fx.qa.load.execution.metrics;

import com.mk.fx.qa.load.execution.model.LoadModelType;
import java.time.Duration;

/**
 * Immutable configuration used to initialise metrics for a task run, derived from the execution
 * request. Contains task identifiers, model and timing parameters as well as expectations used for
 * reporting (e.g., expected total requests and RPS).
 */
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
