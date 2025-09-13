package org.load.execution.runner.api.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.load.execution.runner.core.model.TaskType;

import java.time.LocalDateTime;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TaskDtoTest {

    @Test
    void defaultConstructor_ShouldGenerateTaskIdAndCreatedAt() {
        TaskDto dto = new TaskDto();

        assertThat(dto.getTaskId()).isNotNull();
        assertThat(dto.getTaskId()).hasSizeGreaterThan(10); // UUID-like
        assertThat(dto.getCreatedAt()).isBeforeOrEqualTo(LocalDateTime.now());
        assertThat(dto.getData()).isEmpty();
    }

    @Test
    void taskTypeConstructor_ShouldSetTaskTypeAndGenerateId() {
        TaskDto dto = new TaskDto(TaskType.LOAD_TEST);

        assertThat(dto.getTaskId()).isNotNull();
        assertThat(dto.getTaskType()).isEqualTo(TaskType.LOAD_TEST);
        assertThat(dto.getCreatedAt()).isBeforeOrEqualTo(LocalDateTime.now());
    }

    @Test
    void setData_ShouldAddKeyValueToMap() {
        TaskDto dto = new TaskDto();
        dto.setData("threads", 10);
        dto.setData("duration", "5m");

        assertThat(dto.getData()).containsEntry("threads", 10)
                .containsEntry("duration", "5m");
    }

    @Test
    void jsonSerialization_ShouldSerializeDataMap() throws Exception {
        TaskDto dto = new TaskDto(TaskType.LOAD_TEST);
        dto.setData("key1", "value1");
        dto.setData("key2", 123);

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        String json = mapper.writeValueAsString(dto);

        assertThat(json).contains("key1", "value1", "key2", "123", "taskType", "LOAD_TEST");
    }

    @Test
    void jsonDeserialization_ShouldPopulateDataMap() throws Exception {
        String json = """
            {
              "taskType": "LOAD_TEST",
              "key1": "value1",
              "key2": 42
            }
            """;

        ObjectMapper mapper = new ObjectMapper();
        TaskDto dto = mapper.readValue(json, TaskDto.class);

        assertThat(dto.getTaskType()).isEqualTo(TaskType.LOAD_TEST);
        assertThat(dto.getData()).containsEntry("key1", "value1")
                .containsEntry("key2", 42);
    }

    @Test
    void lombokGeneratedMethods_ShouldWorkCorrectly() {
        TaskDto dto1 = new TaskDto(TaskType.LOAD_TEST);
        TaskDto dto2 = new TaskDto(TaskType.LOAD_TEST);
        dto2.setTaskId(dto1.getTaskId());

        assertThat(dto1).isNotEqualTo(dto2); // different object references
        assertThat(dto1.toString()).contains("taskId", "taskType");
    }
}