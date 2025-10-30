package com.mk.fx.qa.load.execution.dto.controllerresponse;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.time.Instant;
import java.util.Map;

/**
 * Represents a request to submit a task.
 * Includes fields for the task ID, task type, creation timestamp, and associated data.
 */
@Data
public class TaskSubmissionRequest {

    @JsonProperty(value = "taskId", access = JsonProperty.Access.READ_ONLY)
    private String taskId;

    @NotBlank
    @JsonProperty("taskType")
    private String taskType;

    @JsonProperty(value = "createdAt", access = JsonProperty.Access.READ_ONLY)
    private Instant createdAt;

    @NotNull
    @JsonProperty("data")
    private Map<String, Object> data;
}