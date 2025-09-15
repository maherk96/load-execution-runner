package org.load.execution.runner.api.dto;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import lombok.Data;
import org.load.execution.runner.core.model.TaskType;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Data Transfer Object (DTO) representing a task in the execution runner.
 * <p>
 * Each {@code TaskDto} contains:
 * <ul>
 *   <li>A unique task ID, generated as a UUID</li>
 *   <li>A {@link TaskType} specifying the kind of task</li>
 *   <li>The creation timestamp</li>
 *   <li>A map of dynamic key-value data that can store arbitrary attributes</li>
 * </ul>
 */
@Data
public class TaskDto {

    /**
     * Unique identifier for this task, generated automatically using {@link UUID}.
     */
    private String taskId;

    /**
     * The type/category of this task.
     */
    private TaskType taskType;

    /**
     * Timestamp indicating when this task was created.
     */
    private LocalDateTime createdAt;

    /**
     * Arbitrary key-value data associated with this task.
     * This allows storing extra metadata without modifying the DTO structure.
     */
    private Map<String, Object> data = new HashMap<>();

    /**
     * Default constructor.
     * <p>
     * Generates a random task ID and sets the creation timestamp.
     * </p>
     */
    public TaskDto() {
        this.taskId = UUID.randomUUID().toString();
        this.createdAt = LocalDateTime.now();
    }

    /**
     * Constructs a task with a given {@link TaskType}.
     *
     * @param taskType the type of task
     */
    public TaskDto(TaskType taskType) {
        this();
        this.taskType = taskType;
    }

    /**
     * Returns all additional data as a map for serialization.
     * <p>
     * Annotated with {@link JsonAnyGetter} so the contents of this map
     * are flattened into JSON at the root level.
     * </p>
     *
     * @return the data map
     */
    @JsonAnyGetter
    public Map<String, Object> getData() {
        return data;
    }

    /**
     * Adds a key-value pair to the data map.
     * <p>
     * Annotated with {@link JsonAnySetter} so unknown fields in JSON
     * will be added to this map automatically.
     * </p>
     *
     * @param key   the data key
     * @param value the data value
     */
    @JsonAnySetter
    public void setData(String key, Object value) {
        this.data.put(key, value);
    }
}
