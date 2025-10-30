package com.mk.fx.qa.load.execution.dto.controllerresponse;

import com.mk.fx.qa.load.execution.model.TaskStatus;

import java.time.Instant;
import java.util.UUID;

/**
 * Represents a summary of a task, including its ID, type, status, and submission time.
 */
public record TaskSummaryResponse(UUID taskId, String taskType, TaskStatus status, Instant submittedAt) {
}