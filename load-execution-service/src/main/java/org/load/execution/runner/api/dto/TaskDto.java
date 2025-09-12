package org.load.execution.runner.api.dto;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import lombok.Data;
import org.load.execution.runner.core.model.TaskType;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Data
public class TaskDto {
    private String taskId;
    private TaskType taskType;
    private LocalDateTime createdAt;
    private Map<String, Object> data = new HashMap<>();

    public TaskDto() {
        this.taskId = UUID.randomUUID().toString();
        this.createdAt = LocalDateTime.now();
    }

    public TaskDto(TaskType taskType) {
        this();
        this.taskType = taskType;
    }

    @JsonAnyGetter
    public Map<String, Object> getData() { return data; }

    @JsonAnySetter
    public void setData(String key, Object value) { this.data.put(key, value); }

}
