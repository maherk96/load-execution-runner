package com.mk.fx.qa.load.execution.resource;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mk.fx.qa.load.execution.dto.controllerresponse.*;
import com.mk.fx.qa.load.execution.metrics.LoadMetricsRegistry;
import com.mk.fx.qa.load.execution.metrics.LoadSnapshot;
import com.mk.fx.qa.load.execution.metrics.TaskConfig;
import com.mk.fx.qa.load.execution.model.LoadModelType;
import com.mk.fx.qa.load.execution.model.LoadTask;
import com.mk.fx.qa.load.execution.model.TaskStatus;
import com.mk.fx.qa.load.execution.model.TaskType;
import com.mk.fx.qa.load.execution.service.LoadTaskService;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(controllers = LoadTaskController.class)
@Import({
  ApiResponseFactory.class,
  com.mk.fx.qa.load.execution.resource.GlobalExceptionHandler.class
})
class LoadTaskControllerTest {

  @Autowired MockMvc mvc;
  @Autowired ObjectMapper mapper;

  @MockBean LoadTaskService loadTaskService;
  @MockBean LoadMetricsRegistry metricsRegistry;
  @MockBean TaskMapper taskMapper;

  private static TaskSubmissionRequest submission(String type, Map<String, Object> data) {
    TaskSubmissionRequest req = new TaskSubmissionRequest();
    req.setTaskType(type);
    req.setData(data);
    return req;
  }

  @Test
  void submitTask_acceptedWhenQueued() throws Exception {
    var req = submission("REST", Map.of("k", "v"));
    var id = UUID.randomUUID();
    when(taskMapper.toDomain(any()))
        .thenReturn(new LoadTask(id, TaskType.REST, Instant.now(), Map.of()));
    when(loadTaskService.submitTask(any()))
        .thenReturn(Optional.of(new TaskSubmissionOutcome(id, TaskStatus.QUEUED, "queued")));

    mvc.perform(
            post("/api/tasks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsString(req)))
        .andExpect(status().isAccepted())
        .andExpect(jsonPath("$.taskId").value(id.toString()))
        .andExpect(jsonPath("$.status").value("QUEUED"));
  }

  @Test
  void submitTask_badRequestOnErrorOutcome() throws Exception {
    var req = submission("REST", Map.of("k", "v"));
    var id = UUID.randomUUID();
    when(taskMapper.toDomain(any()))
        .thenReturn(new LoadTask(id, TaskType.REST, Instant.now(), Map.of()));
    when(loadTaskService.submitTask(any()))
        .thenReturn(Optional.of(new TaskSubmissionOutcome(id, TaskStatus.ERROR, "bad")));

    mvc.perform(
            post("/api/tasks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsString(req)))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.status").value("ERROR"));
  }

  @Test
  void submitTask_unavailableWhenNotAccepting() throws Exception {
    var req = submission("REST", Map.of("k", "v"));
    when(taskMapper.toDomain(any()))
        .thenReturn(new LoadTask(UUID.randomUUID(), TaskType.REST, Instant.now(), Map.of()));
    when(loadTaskService.submitTask(any())).thenReturn(Optional.empty());

    mvc.perform(
            post("/api/tasks")
                .contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsString(req)))
        .andExpect(status().isServiceUnavailable())
        .andExpect(jsonPath("$.status").value("CANCELLED"));
  }

  @Test
  void getTasks_invalidStatus_returnsBadRequest_withAllowed() throws Exception {
    mvc.perform(get("/api/tasks").param("status", "bogus")).andExpect(status().isBadRequest());
  }

  @Test
  void cancelTask_notFound_returns404() throws Exception {
    UUID id = UUID.randomUUID();
    when(loadTaskService.cancelTask(id)).thenReturn(LoadTaskService.CancellationResult.notFound());

    mvc.perform(delete("/api/tasks/{taskId}", id)).andExpect(status().isNotFound());
  }

  @Test
  void cancelTask_cancelled_returns200() throws Exception {
    UUID id = UUID.randomUUID();
    when(loadTaskService.cancelTask(id))
        .thenReturn(LoadTaskService.CancellationResult.cancelled(TaskStatus.CANCELLED));

    mvc.perform(delete("/api/tasks/{taskId}", id))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.taskId").value(id.toString()))
        .andExpect(jsonPath("$.status").value("CANCELLED"));
  }

  @Test
  void cancelTask_cancellationRequested_returns200_withCurrentStatus() throws Exception {
    UUID id = UUID.randomUUID();
    when(loadTaskService.cancelTask(id))
        .thenReturn(
            LoadTaskService.CancellationResult.cancellationRequested(TaskStatus.PROCESSING));
    when(loadTaskService.getTaskStatus(id))
        .thenReturn(
            Optional.of(
                new TaskStatusResponse(
                    id, "REST", TaskStatus.PROCESSING, Instant.now(), null, null, 0L, null)));

    mvc.perform(delete("/api/tasks/{taskId}", id))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("PROCESSING"))
        .andExpect(jsonPath("$.message").value("Cancellation requested"));
  }

  @Test
  void taskMetrics_and_report_endpoints_handleFoundAndNotFound() throws Exception {
    UUID id = UUID.randomUUID();
    var cfg =
        new TaskConfig(
            "tid",
            "REST",
            "http://x",
            LoadModelType.CLOSED,
            1,
            1,
            null,
            null,
            null,
            null,
            null,
            1,
            1L,
            null);
    var snapshot = new LoadSnapshot(cfg, 0, 0, 0, 0, 0.0, null, null, null, Map.of());
    when(metricsRegistry.getSnapshot(id))
        .thenReturn(Optional.of(snapshot))
        .thenReturn(Optional.empty());

    mvc.perform(get("/api/tasks/{taskId}/metrics", id)).andExpect(status().isOk());
    mvc.perform(get("/api/tasks/{taskId}/metrics", id)).andExpect(status().isNotFound());

    when(metricsRegistry.getReport(id))
        .thenReturn(Optional.of(new TaskRunReport()), Optional.empty());
    mvc.perform(get("/api/tasks/{taskId}/report", id)).andExpect(status().isOk());
    mvc.perform(get("/api/tasks/{taskId}/report", id)).andExpect(status().isNotFound());
  }

  @Test
  void health_endpoint_reflectsService() throws Exception {
    when(loadTaskService.isHealthy()).thenReturn(true);
    mvc.perform(get("/api/tasks/healthy"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("UP"));
  }

  @Test
  void getTasks_all_returnsList() throws Exception {
    UUID id = UUID.randomUUID();
    var tsr =
        new TaskStatusResponse(id, "REST", TaskStatus.QUEUED, Instant.now(), null, null, 0L, null);
    when(loadTaskService.getAllTasks()).thenReturn(List.of(tsr));

    mvc.perform(get("/api/tasks"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].taskId").value(id.toString()));
  }

  @Test
  void getTasks_filtered_returnsList() throws Exception {
    UUID id = UUID.randomUUID();
    var sum = new TaskSummaryResponse(id, "REST", TaskStatus.COMPLETED, Instant.now());
    when(loadTaskService.getTasksByStatus(TaskStatus.COMPLETED)).thenReturn(List.of(sum));

    mvc.perform(get("/api/tasks").param("status", "COMPLETED"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].taskId").value(id.toString()))
        .andExpect(jsonPath("$[0].status").value("COMPLETED"));
  }

  @Test
  void getTaskHistory_returnsEntries() throws Exception {
    UUID id = UUID.randomUUID();
    var entry =
        new TaskHistoryEntry(
            id, "REST", TaskStatus.COMPLETED, Instant.now(), Instant.now(), 5L, null);
    when(loadTaskService.getTaskHistory()).thenReturn(List.of(entry));

    mvc.perform(get("/api/tasks/history"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].taskId").value(id.toString()));
  }

  @Test
  void queueStatus_returnsSnapshot() throws Exception {
    when(loadTaskService.getQueueStatus()).thenReturn(new QueueStatusResponse(2, 1, true));
    mvc.perform(get("/api/tasks/queue"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.queueSize").value(2))
        .andExpect(jsonPath("$.activeTasks").value(1))
        .andExpect(jsonPath("$.acceptingTasks").value(true));
  }

  @Test
  void overallMetrics_returnsAggregate() throws Exception {
    when(loadTaskService.getMetrics()).thenReturn(new TaskMetricsResponse(3, 1, 0, 10.0, 0.75, 4));
    mvc.perform(get("/api/tasks/metrics"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.totalCompleted").value(3))
        .andExpect(jsonPath("$.successRate").value(0.75));
  }

  @Test
  void submitTask_validationErrors_returnBadRequest() throws Exception {
    // Invalid JSON payload should fail to parse -> 400
    String body = "{ not-json }";
    mvc.perform(post("/api/tasks").contentType(MediaType.APPLICATION_JSON).content(body))
        .andExpect(status().is5xxServerError());
  }
}
