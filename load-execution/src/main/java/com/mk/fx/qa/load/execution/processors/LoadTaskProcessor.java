package com.mk.fx.qa.load.execution.processors;


import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionRequest;
import com.mk.fx.qa.load.execution.model.TaskType;

import java.util.UUID;

public interface LoadTaskProcessor {

    TaskType supportedTaskType();

    void execute(TaskSubmissionRequest request) throws Exception;

    void cancel(UUID taskId);
}