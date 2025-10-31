package com.mk.fx.qa.load.execution.dto.controllerresponse;

import com.mk.fx.qa.load.execution.model.TaskStatus;
import java.time.Instant;
import java.util.UUID;

/**
 * Represents a single entry in the task history. Contains details about the task execution,
 * including its status, timing, and any error messages.
 */
public record TaskHistoryEntry(
    UUID taskId,
    String taskType,
    TaskStatus status,
    Instant startedAt,
    Instant completedAt,
    long processingTimeMillis,
    String errorMessage) {}
