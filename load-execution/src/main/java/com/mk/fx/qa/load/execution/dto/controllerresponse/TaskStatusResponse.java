package com.mk.fx.qa.load.execution.dto.controllerresponse;

import com.mk.fx.qa.load.execution.model.TaskStatus;
import java.time.Instant;
import java.util.UUID;

/**
 * Represents the status of a task in the system. Contains details such as task ID, type, status,
 * timestamps, processing time, and error message if any.
 */
public record TaskStatusResponse(
    UUID taskId,
    String taskType,
    TaskStatus status,
    Instant submittedAt,
    Instant startedAt,
    Instant completedAt,
    Long processingTimeMillis,
    String errorMessage) {}
