package com.mk.fx.qa.load.execution.dto.controllerresponse;

import com.mk.fx.qa.load.execution.model.TaskStatus;
import java.util.UUID;

/**
 * Response object for task cancellation requests. Contains the task ID, status of the cancellation,
 * and an optional message.
 */
public record TaskCancellationResponse(UUID taskId, TaskStatus status, String message) {}
