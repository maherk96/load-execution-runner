package com.mk.fx.qa.load.execution.resource;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionRequest;
import com.mk.fx.qa.load.execution.model.LoadTask;
import com.mk.fx.qa.load.execution.model.TaskType;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;

@Mapper(componentModel = "spring")
public interface TaskMapper {

  @Mapping(target = "id", expression = "java(UUID.randomUUID())")
  @Mapping(target = "createdAt", expression = "java(Instant.now())")
  @Mapping(target = "taskType", source = "taskType", qualifiedByName = "mapTaskType")
  LoadTask toDomain(TaskSubmissionRequest request);

  @Named("mapTaskType")
  default TaskType mapTaskType(String taskType) {
    return TaskType.fromValue(taskType);
  }
}
