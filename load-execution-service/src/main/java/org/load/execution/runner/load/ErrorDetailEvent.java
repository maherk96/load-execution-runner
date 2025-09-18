package org.load.execution.runner.load;

import java.time.Instant;

public record ErrorDetailEvent(
    String testId,
    Instant timestamp,
    int userId,
    int httpStatusCode,
    String errorType,
    String errorMessage,
    String requestUrl,
    int retryAttempt
) {}