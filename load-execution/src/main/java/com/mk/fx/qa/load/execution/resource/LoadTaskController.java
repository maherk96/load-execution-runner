package com.mk.fx.qa.load.execution.resource;

import com.mk.fx.qa.load.execution.cfg.ErrorResponse;
import com.mk.fx.qa.load.execution.dto.controllerresponse.HealthResponse;
import com.mk.fx.qa.load.execution.dto.controllerresponse.QueueStatusResponse;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskCancellationResponse;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskHistoryEntry;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskLiveMetricsResponse;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskMetricsResponse;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskStatusResponse;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionOutcome;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionRequest;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionResponse;
import com.mk.fx.qa.load.execution.metrics.LoadMetricsRegistry;
import com.mk.fx.qa.load.execution.metrics.LoadSnapshot;
import com.mk.fx.qa.load.execution.model.LoadModelType;
import com.mk.fx.qa.load.execution.model.LoadTask;
import com.mk.fx.qa.load.execution.model.TaskStatus;
import com.mk.fx.qa.load.execution.service.LoadTaskService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
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

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.mk.fx.qa.load.execution.model.TaskStatus.CANCELLED;
import static com.mk.fx.qa.load.execution.model.TaskStatus.COMPLETED;
import static com.mk.fx.qa.load.execution.model.TaskStatus.ERROR;
import static com.mk.fx.qa.load.execution.model.TaskStatus.PROCESSING;
import static com.mk.fx.qa.load.execution.model.TaskStatus.QUEUED;

@Tag(
    name = "Load Tasks",
    description = "Endpoints for submitting, monitoring, and managing load execution tasks")
@RestController
@RequestMapping("/api/tasks")
@Validated
public class LoadTaskController {

    private final LoadTaskService loadTaskService;
    private final LoadMetricsRegistry metricsRegistry;
    private final TaskMapper taskMapper;

    @Autowired
    public LoadTaskController(
            LoadTaskService loadTaskService, LoadMetricsRegistry metricsRegistry, TaskMapper taskMapper) {
        this.loadTaskService = loadTaskService;
        this.metricsRegistry = metricsRegistry;
        this.taskMapper = taskMapper;
    }

    @Operation(
        summary = "Submit a new load task",
        description = "Creates and queues a new load test task for execution.",
        responses = {
            @ApiResponse(
                responseCode = "202",
                description = "Task accepted and queued",
                content = @Content(schema = @Schema(implementation = TaskSubmissionResponse.class))),
            @ApiResponse(
                responseCode = "400",
                description = "Invalid request payload",
                content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(
                responseCode = "503",
                description = "Service not accepting new tasks",
                content = @Content(schema = @Schema(implementation = TaskSubmissionResponse.class)))
        })
    @PostMapping
    public ResponseEntity<TaskSubmissionResponse> submitTask(
            @Valid @RequestBody TaskSubmissionRequest request) {
        LoadTask loadTask = taskMapper.toDomain(request);
        Optional<TaskSubmissionOutcome> outcomeOpt = loadTaskService.submitTask(loadTask);

        if (outcomeOpt.isEmpty()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(new TaskSubmissionResponse(
                            null, TaskStatus.CANCELLED, "Service not accepting new tasks"));
        }

        var outcome = outcomeOpt.get();
        return ResponseEntity.status(determineSubmissionHttpStatus(outcome))
                .body(new TaskSubmissionResponse(outcome.taskId(), outcome.status(), outcome.message()));
    }

    @Operation(
            summary = "Get task status",
            description = "Returns the current status of a specific task by its ID.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "Task found",
                            content = @Content(schema = @Schema(implementation = TaskStatusResponse.class))),
                    @ApiResponse(responseCode = "404", description = "Task not found")
            })
    @GetMapping("/{taskId}")
    public ResponseEntity<TaskStatusResponse> getTaskStatus(
            @Parameter(description = "UUID of the task") @PathVariable UUID taskId) {
        return loadTaskService
                .getTaskStatus(taskId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @Operation(
            summary = "Cancel a task",
            description = "Attempts to cancel the specified task if it is still running or queued.",
            responses = {
                    @ApiResponse(
                            responseCode = "200",
                            description = "Task cancelled",
                            content = @Content(schema = @Schema(implementation = TaskCancellationResponse.class))),
                    @ApiResponse(
                            responseCode = "404",
                            description = "Task not found",
                            content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
                    @ApiResponse(
                            responseCode = "409",
                            description = "Task not cancellable",
                            content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
            })
    @DeleteMapping("/{taskId}")
    public ResponseEntity<?> cancelTask(
            @Parameter(description = "UUID of the task to cancel") @PathVariable UUID taskId) {
        var result = loadTaskService.cancelTask(taskId);
        return switch (result.getState()) {
            case NOT_FOUND ->
                    ResponseEntity.status(HttpStatus.NOT_FOUND)
                            .body(new ErrorResponse("Not Found", "Task not found"));
            case NOT_CANCELLABLE ->
                    ResponseEntity.status(HttpStatus.CONFLICT)
                            .body(new ErrorResponse("Conflict", "Task cannot be cancelled in its current state"));
            case CANCELLED ->
                    ResponseEntity.ok(
                            new TaskCancellationResponse(taskId, TaskStatus.CANCELLED, "Task cancelled"));
            case CANCELLATION_REQUESTED -> {
                TaskStatus currentStatus =
                        loadTaskService
                                .getTaskStatus(taskId)
                                .map(TaskStatusResponse::status)
                                .orElse(TaskStatus.PROCESSING);
                yield ResponseEntity.ok(
                        new TaskCancellationResponse(taskId, currentStatus, "Cancellation requested"));
            }
        };
    }

    @Operation(
            summary = "Get tasks",
            description = "Retrieves all tasks, optionally filtered by status.")
    @GetMapping
    public ResponseEntity<?> getTasks(
            @Parameter(description = "Optional status filter (e.g., QUEUED, PROCESSING)")
            @RequestParam(required = false)
            String status) {
        if (status == null) {
            return ResponseEntity.ok(loadTaskService.getAllTasks());
        }
        var taskStatus = TaskStatus.valueOf(status.toUpperCase());
        return ResponseEntity.ok(loadTaskService.getTasksByStatus(taskStatus));
    }

    @Operation(
            summary = "Get task history",
            description = "Returns a list of recently executed tasks.")
    @GetMapping("/history")
    public ResponseEntity<List<TaskHistoryEntry>> getTaskHistory() {
        return ResponseEntity.ok(loadTaskService.getTaskHistory());
    }

    @Operation(summary = "Get queue status", description = "Returns current queue size and capacity.")
    @GetMapping("/queue")
    public ResponseEntity<QueueStatusResponse> getQueueStatus() {
        return ResponseEntity.ok(loadTaskService.getQueueStatus());
    }

    @Operation(
            summary = "Get overall metrics",
            description = "Returns global execution metrics across all tasks.")
    @GetMapping("/metrics")
    public ResponseEntity<TaskMetricsResponse> getMetrics() {
        return ResponseEntity.ok(loadTaskService.getMetrics());
    }

    @Operation(
            summary = "Get task metrics",
            description = "Returns live metrics for a specific task.")
    @GetMapping("/{taskId}/metrics")
    public ResponseEntity<?> getTaskMetrics(
            @Parameter(description = "UUID of the task") @PathVariable UUID taskId) {
        return metricsRegistry
                .getSnapshot(taskId)
                .<ResponseEntity<?>>map(snapshot -> ResponseEntity.ok(mapToResponse(snapshot)))
                .orElseGet(
                        () ->
                                ResponseEntity.status(HttpStatus.NOT_FOUND)
                                        .body(new ErrorResponse("Not Found", "Metrics not found for task: " + taskId)));
    }

    @Operation(
            summary = "Get task report",
            description = "Returns final execution report for a completed task.")
    @GetMapping("/{taskId}/report")
    public ResponseEntity<?> getTaskReport(
            @Parameter(description = "UUID of the task") @PathVariable UUID taskId) {
        return metricsRegistry
                .getReport(taskId)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(
                        () ->
                                ResponseEntity.status(HttpStatus.NOT_FOUND)
                                        .body(new ErrorResponse("Not Found", "Report not found for task: " + taskId)));
    }

    @Operation(
            summary = "Get supported task types",
            description = "Returns a set of all supported task types.")
    @GetMapping("/types")
    public ResponseEntity<Set<String>> getSupportedTaskTypes() {
        return ResponseEntity.ok(loadTaskService.getSupportedTaskTypes());
    }

    @Operation(
            summary = "Health check",
            description = "Simple health endpoint to verify the service is running.")
    @GetMapping("/health")
    public ResponseEntity<HealthResponse> health() {
        return ResponseEntity.ok(new HealthResponse(loadTaskService.isHealthy() ? "UP" : "DOWN"));
    }

    private HttpStatus determineSubmissionHttpStatus(TaskSubmissionOutcome outcome) {
        return switch (outcome.status()) {
            case COMPLETED -> HttpStatus.OK;
            case PROCESSING, QUEUED -> HttpStatus.ACCEPTED;
            case ERROR -> HttpStatus.BAD_REQUEST;
            case CANCELLED -> HttpStatus.SERVICE_UNAVAILABLE;
            default -> throw new IllegalStateException("Unexpected value: " + outcome.status());
        };
    }


    private TaskLiveMetricsResponse mapToResponse(LoadSnapshot snapshot) {
        var resp = new TaskLiveMetricsResponse();
        var cfg = snapshot.config();
        resp.setTaskId(cfg.taskId());
        resp.setTaskType(cfg.taskType());
        resp.setBaseUrl(cfg.baseUrl());
        resp.setModel(LoadModelType.valueOf(cfg.model().name()));
        resp.setUsers(cfg.users());
        resp.setIterationsPerUser(cfg.iterationsPerUser());
        resp.setWarmup(cfg.warmup());
        resp.setRampUp(cfg.rampUp());
        resp.setHoldFor(cfg.holdFor());
        resp.setArrivalRatePerSec(cfg.arrivalRatePerSec());
        resp.setDuration(cfg.duration());
        resp.setRequestsPerIteration(cfg.requestsPerIteration());
        resp.setExpectedTotalRequests(cfg.expectedTotalRequests());
        resp.setExpectedRps(cfg.expectedRps());
        resp.setUsersStarted(snapshot.usersStarted());
        resp.setUsersCompleted(snapshot.usersCompleted());
        resp.setTotalRequests(snapshot.totalRequests());
        resp.setTotalErrors(snapshot.totalErrors());
        resp.setAchievedRps(snapshot.achievedRps());
        resp.setLatencyMinMs(snapshot.latencyMinMs());
        resp.setLatencyAvgMs(snapshot.latencyAvgMs());
        resp.setLatencyMaxMs(snapshot.latencyMaxMs());
        resp.setActiveUserIterations(snapshot.activeUserIterations());
        return resp;
    }



}