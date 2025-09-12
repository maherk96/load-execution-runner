package org.load.execution.runner.api.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.load.execution.runner.api.dto.TaskDto;
import org.load.execution.runner.api.dto.TaskExecution;
import org.load.execution.runner.api.dto.TaskResponseDto;
import org.load.execution.runner.core.model.ServiceState;
import org.load.execution.runner.core.model.TaskStatus;
import org.load.execution.runner.core.model.TaskType;
import org.load.execution.runner.core.queue.ImprovedTaskQueueService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api/tasks")
@Validated
public class EnhancedTaskController {

    private static final Logger logger = LoggerFactory.getLogger(EnhancedTaskController.class);
    private final ImprovedTaskQueueService taskQueueService;

    public EnhancedTaskController(ImprovedTaskQueueService taskQueueService) {
        this.taskQueueService = taskQueueService;
    }

    // ===============================
    // TASK LIFECYCLE ENDPOINTS
    // ===============================

    @Operation(summary = "Submit a new task", description = "Accepts a TaskDto payload and queues it for processing.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Task queued or processing"),
            @ApiResponse(responseCode = "200", description = "Task completed immediately"),
            @ApiResponse(responseCode = "400", description = "Task submission failed", content = @Content(schema = @Schema(implementation = TaskResponseDto.class)))
    })
    @PostMapping("/submit")
    public ResponseEntity<TaskResponseDto> submitTask(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    description = "Task to submit",
                    required = true,
                    content = @Content(schema = @Schema(implementation = TaskDto.class))
            )
            @RequestBody TaskDto task) {
        logger.info("Received task submission: {}", task.getTaskId());
        TaskResponseDto response = taskQueueService.submitTask(task);

        return switch (response.getStatus()) {
            case "ERROR" -> ResponseEntity.badRequest().body(response);
            case "QUEUED", "PROCESSING" -> ResponseEntity.accepted().body(response);
            default -> ResponseEntity.ok(response);
        };
    }

    @Operation(summary = "Get task status", description = "Returns the current status of a specific task.")
    @Parameter(name = "taskId", description = "ID of the task", required = true, example = "abc123", in = ParameterIn.PATH)
    @GetMapping("/{taskId}/status")
    public ResponseEntity<TaskResponseDto> getTaskStatus(@PathVariable("taskId") String taskId) {
        TaskResponseDto response = taskQueueService.getTaskStatus(taskId);
        return switch (response.getStatus()) {
            case "NOT_FOUND" -> ResponseEntity.notFound().build();
            case "ERROR" -> ResponseEntity.badRequest().body(response);
            default -> ResponseEntity.ok(response);
        };
    }

    @Operation(summary = "Cancel a task", description = "Cancels a running or queued task by ID.")
    @Parameter(name = "taskId", description = "ID of the task to cancel", required = true, example = "abc123", in = ParameterIn.PATH)
    @DeleteMapping("/{taskId}")
    public ResponseEntity<TaskResponseDto> cancelTask(@PathVariable String taskId) {
        logger.info("Received cancel request for task: {}", taskId);
        TaskResponseDto response = taskQueueService.cancelTask(taskId);

        return switch (response.getStatus()) {
            case "NOT_FOUND" -> ResponseEntity.notFound().build();
            case "ERROR" -> ResponseEntity.badRequest().body(response);
            case "CANCELLED" -> ResponseEntity.ok(response);
            default -> ResponseEntity.ok(response);
        };
    }

    // ===============================
    // QUEUE MANAGEMENT ENDPOINTS
    // ===============================

    @Operation(summary = "Get queue status", description = "Returns a map of queue statistics (size, active tasks, etc.).")
    @GetMapping("/queue/status")
    public ResponseEntity<Map<String, Object>> getQueueStatus() {
        Map<String, Object> status = taskQueueService.getServiceStatus();
        return ResponseEntity.ok(status);
    }

    @Operation(summary = "Get task history", description = "Returns a list of previously executed tasks.")
    @Parameter(name = "limit", description = "Maximum number of records to return", example = "50", in = ParameterIn.QUERY)
    @GetMapping("/history")
    public ResponseEntity<List<TaskExecution>> getTaskHistory(
            @RequestParam(defaultValue = "50") int limit) {
        List<TaskExecution> history = taskQueueService.getTaskHistory(limit);
        return ResponseEntity.ok(history);
    }

    @Operation(summary = "Get tasks by status", description = "Returns tasks filtered by their current status.")
    @Parameters({
            @Parameter(name = "status", description = "Task status filter", required = true, example = "PROCESSING", in = ParameterIn.PATH, schema = @Schema(implementation = TaskStatus.class)),
            @Parameter(name = "limit", description = "Maximum number of results to return", example = "20", in = ParameterIn.QUERY)
    })
    @GetMapping("/status/{status}")
    public ResponseEntity<List<TaskExecution>> getTasksByStatus(
            @PathVariable("status") TaskStatus status,
            @RequestParam(defaultValue = "20") int limit) {
        List<TaskExecution> tasks = taskQueueService.getTasksByStatus(status, limit);
        return ResponseEntity.ok(tasks);
    }

    @Operation(summary = "Get available task types", description = "Returns a set of supported task types.")
    @GetMapping("/types")
    public ResponseEntity<Set<TaskType>> getAvailableTaskTypes() {
        Set<TaskType> types = taskQueueService.getAvailableTaskTypes();
        return ResponseEntity.ok(types);
    }


    // ===============================
    // MONITORING ENDPOINTS
    // ===============================

    @Operation(summary = "Get service metrics", description = "Returns key metrics for monitoring systems.")
    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> getTaskMetrics() {
        Map<String, Object> serviceStatus = taskQueueService.getServiceStatus();
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("queueSize", serviceStatus.get("queueSize"));
        metrics.put("isProcessing", serviceStatus.get("isProcessing"));
        metrics.put("activeTasks", serviceStatus.get("activeTasks"));

        @SuppressWarnings("unchecked")
        Map<String, Object> statistics = (Map<String, Object>) serviceStatus.get("statistics");
        if (statistics != null) {
            metrics.put("totalCompleted", statistics.get("totalCompleted"));
            metrics.put("totalFailed", statistics.get("totalFailed"));
            metrics.put("successRate", statistics.get("successRate"));
            metrics.put("averageProcessingTimeMs", statistics.get("averageProcessingTimeMs"));
        }

        return ResponseEntity.ok(metrics);
    }

    @Operation(summary = "Health check", description = "Returns UP if the service is healthy, DOWN if shutting down.")
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> healthCheck() {
        ServiceState serviceState = taskQueueService.getServiceState();
        boolean isHealthy = serviceState == ServiceState.RUNNING;

        Map<String, String> health = Map.of(
                "status", isHealthy ? "UP" : "DOWN",
                "service", "TaskQueueService"
        );

        return isHealthy ? ResponseEntity.ok(health) :
                ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(health);
    }
}