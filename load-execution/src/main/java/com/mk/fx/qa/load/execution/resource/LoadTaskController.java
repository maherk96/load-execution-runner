package com.mk.fx.qa.load.execution.resource;

import static com.mk.fx.qa.load.execution.model.TaskStatus.*;

import com.mk.fx.qa.load.execution.dto.controllerresponse.*;
import com.mk.fx.qa.load.execution.metrics.LoadMetricsRegistry;
import com.mk.fx.qa.load.execution.metrics.LoadSnapshot;
import com.mk.fx.qa.load.execution.model.*;
import com.mk.fx.qa.load.execution.service.LoadTaskService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.util.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

@Slf4j
@Tag(
    name = "Load Tasks",
    description = "Endpoints for submitting, monitoring, and managing load execution tasks")
@RestController
@RequestMapping("/api/tasks")
@Validated
@RequiredArgsConstructor
public class LoadTaskController {

  private final LoadTaskService loadTaskService;
  private final LoadMetricsRegistry metricsRegistry;
  private final TaskMapper taskMapper;
  private final ApiResponseFactory responseFactory;

  // -----------------------------------------------------
  // Task submission
  // -----------------------------------------------------
  @Operation(
      summary = "Submit a new load task",
      description = "Queues a new load test for execution.")
  @PostMapping
  public ResponseEntity<TaskSubmissionResponse> submitTask(
      @Valid @RequestBody TaskSubmissionRequest request) {
    log.info("Received load task submission type={}", request.getTaskType());
    LoadTask loadTask = taskMapper.toDomain(request);
    Optional<TaskSubmissionOutcome> outcomeOpt = loadTaskService.submitTask(loadTask);

    if (outcomeOpt.isEmpty()) {
      return responseFactory.unavailable(
          new TaskSubmissionResponse(null, CANCELLED, "Service not accepting new tasks"));
    }

    TaskSubmissionOutcome outcome = outcomeOpt.get();
    HttpStatus status = mapStatus(outcome.status());
    log.info("Task {} submitted with status {}", outcome.taskId(), outcome.status());
    return ResponseEntity.status(status)
        .body(new TaskSubmissionResponse(outcome.taskId(), outcome.status(), outcome.message()));
  }

  // -----------------------------------------------------
  // Task status and control
  // -----------------------------------------------------
  @Operation(summary = "Get task status", description = "Returns current status of a task.")
  @GetMapping("/{taskId}")
  public ResponseEntity<TaskStatusResponse> getTaskStatus(@PathVariable UUID taskId) {
    return loadTaskService
        .getTaskStatus(taskId)
        .map(ResponseEntity::ok)
        .orElseGet(
            () -> {
              log.warn("Task {} not found", taskId);
              return ResponseEntity.notFound().build();
            });
  }

  @Operation(summary = "Cancel task", description = "Attempts to cancel a running or queued task.")
  @DeleteMapping("/{taskId}")
  public ResponseEntity<?> cancelTask(@PathVariable UUID taskId) {
    var result = loadTaskService.cancelTask(taskId);
    log.info("Cancellation requested for {} -> {}", taskId, result.getState());
    return switch (result.getState()) {
      case NOT_FOUND -> responseFactory.error(HttpStatus.NOT_FOUND, "Not Found", "Task not found");
      case NOT_CANCELLABLE -> responseFactory.error(
          HttpStatus.CONFLICT, "Conflict", "Task cannot be cancelled in its current state");
      case CANCELLED -> ResponseEntity.ok(
          new TaskCancellationResponse(taskId, CANCELLED, "Task cancelled"));
      case CANCELLATION_REQUESTED -> {
        TaskStatus current =
            loadTaskService
                .getTaskStatus(taskId)
                .map(TaskStatusResponse::status)
                .orElse(PROCESSING);
        yield ResponseEntity.ok(
            new TaskCancellationResponse(taskId, current, "Cancellation requested"));
      }
    };
  }

  // -----------------------------------------------------
  // Task listings and history
  // -----------------------------------------------------
  @Operation(summary = "List tasks", description = "Lists all tasks or filters by status.")
  @GetMapping
  public ResponseEntity<?> getTasks(@RequestParam(required = false) String status) {
    if (status == null) {
      return ResponseEntity.ok(loadTaskService.getAllTasks());
    }
    try {
      TaskStatus taskStatus = TaskStatus.valueOf(status.toUpperCase());
      return ResponseEntity.ok(loadTaskService.getTasksByStatus(taskStatus));
    } catch (IllegalArgumentException ex) {
      log.warn("Invalid status filter: {}", status);
      String allowed = Arrays.toString(TaskStatus.values());
      return responseFactory.error(
          HttpStatus.BAD_REQUEST,
          "Invalid Status",
          "Unrecognized status: " + status + ". Allowed: " + allowed);
    }
  }

  @Operation(summary = "Task history", description = "Returns recently executed tasks.")
  @GetMapping("/history")
  public ResponseEntity<List<TaskHistoryEntry>> getTaskHistory() {
    return ResponseEntity.ok(loadTaskService.getTaskHistory());
  }

  // -----------------------------------------------------
  // Queue & metrics endpoints
  // -----------------------------------------------------
  @Operation(summary = "Queue status", description = "Returns current queue load.")
  @GetMapping("/queue")
  public ResponseEntity<QueueStatusResponse> getQueueStatus() {
    return ResponseEntity.ok(loadTaskService.getQueueStatus());
  }

  @Operation(
      summary = "Overall metrics",
      description = "Returns aggregate metrics across all tasks.")
  @GetMapping("/metrics")
  public ResponseEntity<TaskMetricsResponse> getMetrics() {
    return ResponseEntity.ok(loadTaskService.getMetrics());
  }

  @Operation(
      summary = "Task live metrics",
      description = "Returns metrics snapshot for a specific task.")
  @GetMapping("/{taskId}/metrics")
  public ResponseEntity<?> getTaskMetrics(@PathVariable UUID taskId) {
    return metricsRegistry
        .getSnapshot(taskId)
        .<ResponseEntity<?>>map(snapshot -> ResponseEntity.ok(mapToResponse(snapshot)))
        .orElseGet(
            () -> {
              log.warn("Metrics not found for task {}", taskId);
              return responseFactory.error(
                  HttpStatus.NOT_FOUND, "Not Found", "Metrics not found for task: " + taskId);
            });
  }

  @Operation(
      summary = "Task report",
      description = "Returns final execution report for completed task.")
  @GetMapping("/{taskId}/report")
  public ResponseEntity<?> getTaskReport(@PathVariable UUID taskId) {
    return metricsRegistry
        .getReport(taskId)
        .<ResponseEntity<?>>map(ResponseEntity::ok)
        .orElseGet(
            () -> {
              log.warn("Report not found for task {}", taskId);
              return responseFactory.error(
                  HttpStatus.NOT_FOUND, "Not Found", "Report not found for task: " + taskId);
            });
  }

  // -----------------------------------------------------
  // Misc endpoints
  // -----------------------------------------------------
  @Operation(summary = "Supported task types", description = "Lists all supported load task types.")
  @GetMapping("/types")
  public ResponseEntity<Set<String>> getSupportedTaskTypes() {
    return ResponseEntity.ok(loadTaskService.getSupportedTaskTypes());
  }

  @Operation(summary = "Health check", description = "Verifies service health.")
  @GetMapping("/healthy")
  public ResponseEntity<HealthResponse> health() {
    boolean healthy = loadTaskService.isHealthy();
    log.debug("Health check: {}", healthy ? "UP" : "DOWN");
    return ResponseEntity.ok(new HealthResponse(healthy ? "UP" : "DOWN"));
  }

  // -----------------------------------------------------
  // Helpers
  // -----------------------------------------------------
  private HttpStatus mapStatus(TaskStatus status) {
    return switch (status) {
      case COMPLETED -> HttpStatus.OK;
      case PROCESSING, QUEUED -> HttpStatus.ACCEPTED;
      case ERROR -> HttpStatus.BAD_REQUEST;
      case CANCELLED -> HttpStatus.SERVICE_UNAVAILABLE;
    };
  }

  private TaskLiveMetricsResponse mapToResponse(LoadSnapshot snapshot) {
    var cfg = snapshot.config();
    return TaskLiveMetricsResponse.builder()
        .taskId(cfg.taskId())
        .taskType(cfg.taskType())
        .baseUrl(cfg.baseUrl())
        .model(LoadModelType.valueOf(cfg.model().name()))
        .users(cfg.users())
        .iterationsPerUser(cfg.iterationsPerUser())
        .warmup(cfg.warmup())
        .rampUp(cfg.rampUp())
        .holdFor(cfg.holdFor())
        .arrivalRatePerSec(cfg.arrivalRatePerSec())
        .duration(cfg.duration())
        .requestsPerIteration(cfg.requestsPerIteration())
        .expectedTotalRequests(cfg.expectedTotalRequests())
        .expectedRps(cfg.expectedRps())
        .usersStarted(snapshot.usersStarted())
        .usersCompleted(snapshot.usersCompleted())
        .totalRequests(snapshot.totalRequests())
        .totalErrors(snapshot.totalErrors())
        .achievedRps(snapshot.achievedRps())
        .latencyMinMs(snapshot.latencyMinMs())
        .latencyAvgMs(snapshot.latencyAvgMs())
        .latencyMaxMs(snapshot.latencyMaxMs())
        .activeUserIterations(snapshot.activeUserIterations())
        .build();
  }
}
