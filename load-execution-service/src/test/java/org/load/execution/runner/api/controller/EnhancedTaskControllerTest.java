package org.load.execution.runner.api.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.load.execution.runner.api.dto.TaskDto;
import org.load.execution.runner.api.dto.TaskExecution;
import org.load.execution.runner.api.dto.TaskResponseDto;
import org.load.execution.runner.core.model.ServiceState;
import org.load.execution.runner.core.model.TaskStatus;
import org.load.execution.runner.core.model.TaskType;
import org.load.execution.runner.core.queue.ImprovedTaskQueueService;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@Import(GlobalExceptionHandler.class)
@ExtendWith(MockitoExtension.class)
@DisplayName("EnhancedTaskController Tests")
class EnhancedTaskControllerTest {

    @Mock
    private ImprovedTaskQueueService taskQueueService;

    @InjectMocks
    private EnhancedTaskController taskController;

    private MockMvc mockMvc;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(taskController).build();
        objectMapper = new ObjectMapper();
        objectMapper .findAndRegisterModules(); // Register JavaTimeModule for LocalDateTime
    }

    @Nested
    @DisplayName("Task Lifecycle Tests")
    class TaskLifecycleTests {

        @Test
        @DisplayName("Submit task - Success (Queued)")
        void submitTask_Success_Queued() throws Exception {
            // Arrange
            TaskDto taskDto = createSampleTaskDto();
            TaskResponseDto responseDto = new TaskResponseDto("task-123", "QUEUED", 1, "Task queued successfully");

            when(taskQueueService.submitTask(any(TaskDto.class))).thenReturn(responseDto);

            // Act & Assert
            mockMvc.perform(post("/api/tasks/submit")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(taskDto)))
                    .andExpect(status().isAccepted())
                    .andExpect(jsonPath("$.taskId", is("task-123")))
                    .andExpect(jsonPath("$.status", is("QUEUED")))
                    .andExpect(jsonPath("$.queuePosition", is(1)))
                    .andExpect(jsonPath("$.message", is("Task queued successfully")));

            verify(taskQueueService).submitTask(any(TaskDto.class));
        }

        @Test
        @DisplayName("Submit task - Success (Processing)")
        void submitTask_Success_Processing() throws Exception {
            // Arrange
            TaskDto taskDto = createSampleTaskDto();
            TaskResponseDto responseDto = new TaskResponseDto("task-123", "PROCESSING", "Task is being processed");

            when(taskQueueService.submitTask(any(TaskDto.class))).thenReturn(responseDto);

            // Act & Assert
            mockMvc.perform(post("/api/tasks/submit")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(taskDto)))
                    .andExpect(status().isAccepted())
                    .andExpect(jsonPath("$.status", is("PROCESSING")));
        }

        @Test
        @DisplayName("Submit task - Error")
        void submitTask_Error() throws Exception {
            // Arrange
            TaskDto taskDto = createSampleTaskDto();
            TaskResponseDto responseDto = new TaskResponseDto("task-123", "ERROR", "Invalid task data");

            when(taskQueueService.submitTask(any(TaskDto.class))).thenReturn(responseDto);

            // Act & Assert
            mockMvc.perform(post("/api/tasks/submit")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(taskDto)))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.status", is("ERROR")))
                    .andExpect(jsonPath("$.message", is("Invalid task data")));
        }

        @Test
        @DisplayName("Submit task - Completed immediately")
        void submitTask_CompletedImmediately() throws Exception {
            // Arrange
            TaskDto taskDto = createSampleTaskDto();
            TaskResponseDto responseDto = new TaskResponseDto("task-123", "COMPLETED", "Task completed");

            when(taskQueueService.submitTask(any(TaskDto.class))).thenReturn(responseDto);

            // Act & Assert
            mockMvc.perform(post("/api/tasks/submit")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(taskDto)))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.status", is("COMPLETED")));
        }

        @Test
        @DisplayName("Get task status - Success")
        void getTaskStatus_Success() throws Exception {
            // Arrange
            String taskId = "task-123";
            TaskResponseDto responseDto = new TaskResponseDto(taskId, "PROCESSING", "Task is processing");

            when(taskQueueService.getTaskStatus(taskId)).thenReturn(responseDto);

            // Act & Assert
            mockMvc.perform(get("/api/tasks/{taskId}/status", taskId))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.taskId", is(taskId)))
                    .andExpect(jsonPath("$.status", is("PROCESSING")));

            verify(taskQueueService).getTaskStatus(taskId);
        }

        @Test
        @DisplayName("Get task status - Not Found")
        void getTaskStatus_NotFound() throws Exception {
            // Arrange
            String taskId = "non-existent-task";
            TaskResponseDto responseDto = new TaskResponseDto(taskId, "NOT_FOUND", "Task not found");

            when(taskQueueService.getTaskStatus(taskId)).thenReturn(responseDto);

            // Act & Assert
            mockMvc.perform(get("/api/tasks/{taskId}/status", taskId))
                    .andExpect(status().isNotFound());
        }

        @Test
        @DisplayName("Get task status - Error")
        void getTaskStatus_Error() throws Exception {
            // Arrange
            String taskId = "invalid-task";
            TaskResponseDto responseDto = new TaskResponseDto(taskId, "ERROR", "Invalid task ID");

            when(taskQueueService.getTaskStatus(taskId)).thenReturn(responseDto);

            // Act & Assert
            mockMvc.perform(get("/api/tasks/{taskId}/status", taskId))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.status", is("ERROR")));
        }

        @Test
        @DisplayName("Cancel task - Success")
        void cancelTask_Success() throws Exception {
            // Arrange
            String taskId = "task-123";
            TaskResponseDto responseDto = new TaskResponseDto(taskId, "CANCELLED", "Task cancelled successfully");

            when(taskQueueService.cancelTask(taskId)).thenReturn(responseDto);

            // Act & Assert
            mockMvc.perform(delete("/api/tasks/{taskId}", taskId))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.status", is("CANCELLED")))
                    .andExpect(jsonPath("$.message", is("Task cancelled successfully")));

            verify(taskQueueService).cancelTask(taskId);
        }

        @Test
        @DisplayName("Cancel task - Not Found")
        void cancelTask_NotFound() throws Exception {
            // Arrange
            String taskId = "non-existent-task";
            TaskResponseDto responseDto = new TaskResponseDto(taskId, "NOT_FOUND", "Task not found");

            when(taskQueueService.cancelTask(taskId)).thenReturn(responseDto);

            // Act & Assert
            mockMvc.perform(delete("/api/tasks/{taskId}", taskId))
                    .andExpect(status().isNotFound());
        }

        @Test
        @DisplayName("Cancel task - Error")
        void cancelTask_Error() throws Exception {
            // Arrange
            String taskId = "task-123";
            TaskResponseDto responseDto = new TaskResponseDto(taskId, "ERROR", "Cannot cancel completed task");

            when(taskQueueService.cancelTask(taskId)).thenReturn(responseDto);

            // Act & Assert
            mockMvc.perform(delete("/api/tasks/{taskId}", taskId))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.status", is("ERROR")));
        }
    }

    @Nested
    @DisplayName("Queue Management Tests")
    class QueueManagementTests {

        @Test
        @DisplayName("Get queue status - Success")
        void getQueueStatus_Success() throws Exception {
            // Arrange
            Map<String, Object> serviceStatus = createSampleServiceStatus();
            when(taskQueueService.getServiceStatus()).thenReturn(serviceStatus);

            // Act & Assert
            mockMvc.perform(get("/api/tasks/queue/status"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.serviceName", is("TaskQueueService")))
                    .andExpect(jsonPath("$.queueSize", is(5)))
                    .andExpect(jsonPath("$.isProcessing", is(true)))
                    .andExpect(jsonPath("$.statistics.totalCompleted", is(10)))
                    .andExpect(jsonPath("$.statistics.totalFailed", is(2)));

            verify(taskQueueService).getServiceStatus();
        }

        @Test
        @DisplayName("Get task history - Default limit")
        void getTaskHistory_DefaultLimit() throws Exception {
            // Arrange
            List<TaskExecution> history = createSampleTaskHistory();
            when(taskQueueService.getTaskHistory(50)).thenReturn(history);

            // Act & Assert
            mockMvc.perform(get("/api/tasks/history"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$", hasSize(2)))
                    .andExpect(jsonPath("$[0].taskId", is("task-1")))
                    .andExpect(jsonPath("$[1].taskId", is("task-2")));

            verify(taskQueueService).getTaskHistory(50);
        }

        @Test
        @DisplayName("Get task history - Custom limit")
        void getTaskHistory_CustomLimit() throws Exception {
            // Arrange
            List<TaskExecution> history = createSampleTaskHistory();
            when(taskQueueService.getTaskHistory(10)).thenReturn(history);

            // Act & Assert
            mockMvc.perform(get("/api/tasks/history?limit=10"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$", hasSize(2)));

            verify(taskQueueService).getTaskHistory(10);
        }

        @Test
        @DisplayName("Get tasks by status - Success")
        void getTasksByStatus_Success() throws Exception {
            // Arrange
            List<TaskExecution> tasks = createSampleTasksByStatus();
            when(taskQueueService.getTasksByStatus(TaskStatus.PROCESSING, 20)).thenReturn(tasks);

            // Act & Assert
            mockMvc.perform(get("/api/tasks/status/PROCESSING"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$", hasSize(1)))
                    .andExpect(jsonPath("$[0].status", is("PROCESSING")));

            verify(taskQueueService).getTasksByStatus(TaskStatus.PROCESSING, 20);
        }

        @Test
        @DisplayName("Get tasks by status - Custom limit")
        void getTasksByStatus_CustomLimit() throws Exception {
            // Arrange
            List<TaskExecution> tasks = createSampleTasksByStatus();
            when(taskQueueService.getTasksByStatus(TaskStatus.COMPLETED, 5)).thenReturn(tasks);

            // Act & Assert
            mockMvc.perform(get("/api/tasks/status/COMPLETED?limit=5"))
                    .andExpect(status().isOk());

            verify(taskQueueService).getTasksByStatus(TaskStatus.COMPLETED, 5);
        }

        @Test
        @DisplayName("Get available task types - Success")
        void getAvailableTaskTypes_Success() throws Exception {
            // Arrange
            Set<TaskType> taskTypes = Set.of(TaskType.REST_LOAD, TaskType.FIX_LOAD);
            when(taskQueueService.getAvailableTaskTypes()).thenReturn(taskTypes);

            // Act & Assert
            mockMvc.perform(get("/api/tasks/types"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$", hasSize(2)))
                    .andExpect(jsonPath("$", containsInAnyOrder("REST_LOAD", "FIX_LOAD")));

            verify(taskQueueService).getAvailableTaskTypes();
        }
    }

    @Nested
    @DisplayName("Monitoring Tests")
    class MonitoringTests {

        @Test
        @DisplayName("Get task metrics - Success")
        void getTaskMetrics_Success() throws Exception {
            // Arrange
            Map<String, Object> serviceStatus = createSampleServiceStatus();
            when(taskQueueService.getServiceStatus()).thenReturn(serviceStatus);

            // Act & Assert
            mockMvc.perform(get("/api/tasks/metrics"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.queueSize", is(5)))
                    .andExpect(jsonPath("$.isProcessing", is(true)))
                    .andExpect(jsonPath("$.activeTasks", is(3)))
                    .andExpect(jsonPath("$.totalCompleted", is(10)))
                    .andExpect(jsonPath("$.totalFailed", is(2)))
                    .andExpect(jsonPath("$.successRate", is("83.33%")))
                    .andExpect(jsonPath("$.averageProcessingTimeMs", is(1500)));

            verify(taskQueueService).getServiceStatus();
        }

        @Test
        @DisplayName("Get task metrics - Null statistics")
        void getTaskMetrics_NullStatistics() throws Exception {
            // Arrange
            Map<String, Object> serviceStatus = new HashMap<>();
            serviceStatus.put("queueSize", 0);
            serviceStatus.put("isProcessing", false);
            serviceStatus.put("activeTasks", 0);
            serviceStatus.put("statistics", null);

            when(taskQueueService.getServiceStatus()).thenReturn(serviceStatus);

            // Act & Assert
            mockMvc.perform(get("/api/tasks/metrics"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.queueSize", is(0)))
                    .andExpect(jsonPath("$.isProcessing", is(false)))
                    .andExpect(jsonPath("$.totalCompleted").doesNotExist())
                    .andExpect(jsonPath("$.totalFailed").doesNotExist());
        }

        @Test
        @DisplayName("Health check - Service running")
        void healthCheck_ServiceRunning() throws Exception {
            // Arrange
            when(taskQueueService.getServiceState()).thenReturn(ServiceState.RUNNING);

            // Act & Assert
            mockMvc.perform(get("/api/tasks/health"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.status", is("UP")))
                    .andExpect(jsonPath("$.service", is("TaskQueueService")));

            verify(taskQueueService).getServiceState();
        }

        @Test
        @DisplayName("Health check - Service shutting down")
        void healthCheck_ServiceShuttingDown() throws Exception {
            // Arrange
            when(taskQueueService.getServiceState()).thenReturn(ServiceState.SHUTTING_DOWN);

            // Act & Assert
            mockMvc.perform(get("/api/tasks/health"))
                    .andExpect(status().isServiceUnavailable())
                    .andExpect(jsonPath("$.status", is("DOWN")))
                    .andExpect(jsonPath("$.service", is("TaskQueueService")));
        }

        @Test
        @DisplayName("Health check - Service shutdown")
        void healthCheck_ServiceShutdown() throws Exception {
            // Arrange
            when(taskQueueService.getServiceState()).thenReturn(ServiceState.SHUTDOWN);

            // Act & Assert
            mockMvc.perform(get("/api/tasks/health"))
                    .andExpect(status().isServiceUnavailable())
                    .andExpect(jsonPath("$.status", is("DOWN")));
        }
    }

    @Nested
    @DisplayName("Edge Cases and Error Handling")
    class EdgeCaseTests {

        @Test
        @DisplayName("Submit task - Invalid JSON")
        void submitTask_InvalidJson() throws Exception {
            // Act & Assert
            mockMvc.perform(post("/api/tasks/submit")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("invalid json"))
                    .andExpect(status().isBadRequest());

            verify(taskQueueService, never()).submitTask(any());
        }

        @Test
        @DisplayName("Get task status - Empty task ID")
        void getTaskStatus_EmptyTaskId() throws Exception {
            // Arrange
            String emptyTaskId = " ";
            TaskResponseDto responseDto = new TaskResponseDto(emptyTaskId, "ERROR", "Invalid task ID");
            when(taskQueueService.getTaskStatus(emptyTaskId)).thenReturn(responseDto);

            // Act & Assert
            mockMvc.perform(get("/api/tasks/{taskId}/status", emptyTaskId))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.status", is("ERROR")));
        }

        @Test
        @DisplayName("Get tasks by status - Invalid status")
        void getTasksByStatus_InvalidStatus() throws Exception {
            // Act & Assert
            mockMvc.perform(get("/api/tasks/status/INVALID_STATUS"))
                    .andExpect(status().isBadRequest());
        }

    }

    // Helper methods for creating test data
    private TaskDto createSampleTaskDto() {
        TaskDto taskDto = new TaskDto();
        taskDto.setTaskId("task-123");
        taskDto.setTaskType(TaskType.REST_LOAD);
        taskDto.setData(Map.of("url", "http://example.com", "method", "GET"));
        return taskDto;
    }

    private Map<String, Object> createSampleServiceStatus() {
        Map<String, Object> statistics = Map.of(
                "totalCompleted", 10,
                "totalFailed", 2,
                "successRate", "83.33%",
                "averageProcessingTimeMs", 1500
        );

        return Map.of(
                "serviceName", "TaskQueueService",
                "queueSize", 5,
                "isProcessing", true,
                "activeTasks", 3,
                "statistics", statistics
        );
    }

    private List<TaskExecution> createSampleTaskHistory() {
        TaskExecution task1 = new TaskExecution("task-1", TaskType.REST_LOAD, TaskStatus.COMPLETED,
                LocalDateTime.now().minusHours(1), LocalDateTime.now().minusHours(1),
                LocalDateTime.now().minusMinutes(59), 1000, null, 1);

        TaskExecution task2 = new TaskExecution("task-2", TaskType.FIX_LOAD, TaskStatus.FAILED,
                LocalDateTime.now().minusMinutes(30), LocalDateTime.now().minusMinutes(30),
                LocalDateTime.now().minusMinutes(29), 500, "Connection timeout", 1);

        return List.of(task1, task2);
    }

    private List<TaskExecution> createSampleTasksByStatus() {
        TaskExecution task = new TaskExecution("task-processing", TaskType.REST_LOAD, TaskStatus.PROCESSING,
                LocalDateTime.now().minusMinutes(5), LocalDateTime.now().minusMinutes(5),
                null, 0, null, 1);

        return List.of(task);
    }
}