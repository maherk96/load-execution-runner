package com.mk.fx.qa.load.execution.dto.controllerresponse;

import com.mk.fx.qa.load.execution.model.TaskStatus;
import java.util.UUID;

/**
 * Represents the outcome of a task submission request. Contains the task ID, resulting status, and
 * an optional message.
 */
public record TaskSubmissionOutcome(UUID taskId, TaskStatus status, String message) {}
