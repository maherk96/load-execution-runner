package org.load.execution.runner.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.load.execution.runner.api.dto.TaskDto;
import org.load.execution.runner.api.dto.TaskExecution;
import org.load.execution.runner.api.dto.TaskResponseDto;
import org.load.execution.runner.core.model.ServiceState;
import org.load.execution.runner.core.model.TaskStatus;
import org.load.execution.runner.core.model.TaskType;
import org.load.execution.runner.core.queue.ImprovedTaskQueueService;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;


import org.springframework.http.MediaType;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.hamcrest.Matchers.*;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;


@WebMvcTest(controllers = EnhancedTaskController.class)
class EnhancedTaskControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private ImprovedTaskQueueService taskQueueService;

    private TaskDto taskDto;
    private TaskResponseDto successResponse;

    @BeforeEach
    void setUp() {
        taskDto = new TaskDto(TaskType.LOAD_TEST);
        taskDto.setTaskId("task-123");

        successResponse = new TaskResponseDto("task-123", "QUEUED", "null");
        successResponse.setTaskId("task-123");
        successResponse.setStatus("QUEUED");
    }

    // ========== POST /submit ==========

    @Test
    void submitTask_WhenQueued_ShouldReturn202() throws Exception {
        when(taskQueueService.submitTask(any(TaskDto.class))).thenReturn(successResponse);

        mockMvc.perform(post("/api/tasks/submit")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(taskDto)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.taskId").value("task-123"))
                .andExpect(jsonPath("$.status").value("QUEUED"));
    }

    @Test
    void submitTask_WhenError_ShouldReturn400() throws Exception {
        TaskResponseDto errorResponse = new TaskResponseDto("task-123", "ERROR", "Invalid task");
        errorResponse.setTaskId("task-123");
        errorResponse.setStatus("ERROR");
        errorResponse.setMessage("Invalid task");

        when(taskQueueService.submitTask(any(TaskDto.class))).thenReturn(errorResponse);

        mockMvc.perform(post("/api/tasks/submit")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(taskDto)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value("ERROR"))
                .andExpect(jsonPath("$.message").value("Invalid task"));
    }

    // ========== GET /{taskId}/status ==========

    @Test
    void getTaskStatus_WhenFound_ShouldReturn200() throws Exception {
        when(taskQueueService.getTaskStatus(eq("task-123"))).thenReturn(successResponse);

        mockMvc.perform(get("/api/tasks/task-123/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.taskId").value("task-123"));
    }

    @Test
    void getTaskStatus_WhenNotFound_ShouldReturn404() throws Exception {
        TaskResponseDto notFound = new TaskResponseDto("missing-task", "NOT_FOUND", "Task not found");
        notFound.setStatus("NOT_FOUND");
        when(taskQueueService.getTaskStatus(eq("missing-task"))).thenReturn(notFound);

        mockMvc.perform(get("/api/tasks/missing-task/status"))
                .andExpect(status().isNotFound());
    }

    // ========== DELETE /{taskId} ==========

    @Test
    void cancelTask_WhenCancelled_ShouldReturn200() throws Exception {
        TaskResponseDto cancelled = new TaskResponseDto("task-123", "CANCELLED", "Task cancelled");
        cancelled.setTaskId("task-123");
        cancelled.setStatus("CANCELLED");

        when(taskQueueService.cancelTask(eq("task-123"))).thenReturn(cancelled);

        mockMvc.perform(delete("/api/tasks/task-123"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CANCELLED"));
    }

    @Test
    void cancelTask_WhenNotFound_ShouldReturn404() throws Exception {
        TaskResponseDto notFound = new TaskResponseDto("missing-task", "NOT_FOUND", "Task not found");
        notFound.setStatus("NOT_FOUND");
        when(taskQueueService.cancelTask(eq("missing-task"))).thenReturn(notFound);

        mockMvc.perform(delete("/api/tasks/missing-task"))
                .andExpect(status().isNotFound());
    }

    // ========== GET /queue/status ==========

    @Test
    void getQueueStatus_ShouldReturnStatusMap() throws Exception {
        when(taskQueueService.getServiceStatus()).thenReturn(Map.of("queueSize", 5));

        mockMvc.perform(get("/api/tasks/queue/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.queueSize").value(5));
    }

    // ========== GET /history ==========

    @Test
    void getTaskHistory_ShouldReturnList() throws Exception {
        TaskExecution exec = new TaskExecution("task-123", TaskType.LOAD_TEST, TaskStatus.COMPLETED,
                LocalDateTime.now().minusMinutes(1), LocalDateTime.now().minusSeconds(30),
                LocalDateTime.now(), 500, null, 1);

        when(taskQueueService.getTaskHistory(50)).thenReturn(List.of(exec));

        mockMvc.perform(get("/api/tasks/history"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].taskId").value("task-123"))
                .andExpect(jsonPath("$[0].status").value("COMPLETED"));
    }

    // ========== GET /status/{status} ==========

    @Test
    void getTasksByStatus_ShouldReturnFilteredList() throws Exception {
        TaskExecution exec = new TaskExecution("task-123", TaskType.LOAD_TEST, TaskStatus.PROCESSING,
                LocalDateTime.now(), LocalDateTime.now(), null, 0, null, 1);

        when(taskQueueService.getTasksByStatus(eq(TaskStatus.PROCESSING), eq(20))).thenReturn(List.of(exec));

        mockMvc.perform(get("/api/tasks/status/PROCESSING"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].status").value("PROCESSING"));
    }

    // ========== GET /types ==========

    @Test
    void getAvailableTaskTypes_ShouldReturnSet() throws Exception {
        when(taskQueueService.getAvailableTaskTypes()).thenReturn(Set.of(TaskType.LOAD_TEST, TaskType.DATA_MIGRATION));

        mockMvc.perform(get("/api/tasks/types"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(2)))
                .andExpect(jsonPath("$", hasItem("LOAD_TEST")))
                .andExpect(jsonPath("$", hasItem("DATA_MIGRATION")));
    }

    // ========== GET /metrics ==========

    @Test
    void getTaskMetrics_ShouldReturnMetrics() throws Exception {
        when(taskQueueService.getServiceStatus()).thenReturn(Map.of(
                "queueSize", 2,
                "isProcessing", true,
                "activeTasks", 3,
                "statistics", Map.of("totalCompleted", 5, "totalFailed", 1, "successRate", 0.83, "averageProcessingTimeMs", 120)
        ));

        mockMvc.perform(get("/api/tasks/metrics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.queueSize").value(2))
                .andExpect(jsonPath("$.totalCompleted").value(5))
                .andExpect(jsonPath("$.successRate").value(0.83));
    }

    // ========== GET /health ==========

    @Test
    void healthCheck_WhenRunning_ShouldReturnUP() throws Exception {
        when(taskQueueService.getServiceState()).thenReturn(ServiceState.RUNNING);

        mockMvc.perform(get("/api/tasks/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }

    @Test
    void healthCheck_WhenNotRunning_ShouldReturnDOWN() throws Exception {
        when(taskQueueService.getServiceState()).thenReturn(ServiceState.SHUTDOWN);

        mockMvc.perform(get("/api/tasks/health"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.status").value("DOWN"));
    }
}