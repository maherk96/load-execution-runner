package com.mk.fx.qa.load.execution.dto.controllerresponse;

import com.mk.fx.qa.load.execution.model.TaskStatus;

import java.util.UUID;

/**
 * Response object for task submission.
 * Contains the task ID, status, and an optional message.
 */
public record TaskSubmissionResponse(UUID taskId, TaskStatus status, String message) {
}