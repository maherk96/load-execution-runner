package com.mk.fx.qa.load.execution.e2e;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mk.fx.qa.load.execution.LoadExecutionRunnerMain;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskStatusResponse;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionRequest;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionResponse;
import com.mk.fx.qa.load.execution.model.TaskStatus;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

@SpringBootTest(
    classes = LoadExecutionRunnerMain.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {"ws.mock.enabled=true", "ws.mock.port=9090"})
class WebSocketSwaggerSubmissionIT {

  @LocalServerPort private int port;
  @Autowired private TestRestTemplate rest;
  @Autowired private ObjectMapper mapper;
  @Autowired private org.springframework.core.env.Environment env;

  @Test
  void submit_webrtc_json_like_swagger_and_validate_report() throws Exception {
    // Health
    ResponseEntity<String> health = rest.getForEntity(url("/api/tasks/healthy"), String.class);
    assertEquals(200, health.getStatusCode().value());

    TaskSubmissionRequest req = swaggerLikeRequest("ws://127.0.0.1:9090");

    HttpHeaders headers = new HttpHeaders();
    headers.set(HttpHeaders.CONTENT_TYPE, "application/json");
    headers.set(HttpHeaders.ACCEPT, "application/json");

    ResponseEntity<String> submit =
        rest.exchange(
            url("/api/tasks"), HttpMethod.POST, new HttpEntity<>(mapper.writeValueAsString(req), headers), String.class);
    assertTrue(submit.getStatusCode().is2xxSuccessful(), "Submit failed: " + submit);

    TaskSubmissionResponse out = mapper.readValue(submit.getBody(), TaskSubmissionResponse.class);
    UUID taskId = out.taskId();
    awaitCompletion(taskId, Duration.ofSeconds(5));

    ResponseEntity<TaskRunReport> report =
        rest.getForEntity(url("/api/tasks/" + taskId + "/report"), TaskRunReport.class);
    assertEquals(200, report.getStatusCode().value());
    assertNotNull(report.getBody());
    assertEquals("WEBSOCKET", report.getBody().taskType);
    assertNotNull(report.getBody().protocolDetails);
    assertNotNull(report.getBody().protocolDetails.websocket);
  }

  private String url(String path) {
    String ctx = env.getProperty("server.servlet.context-path", "");
    if (ctx == null) ctx = "";
    return "http://127.0.0.1:" + port + ctx + path;
  }

  private void awaitCompletion(UUID taskId, Duration timeout) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      ResponseEntity<TaskStatusResponse> status =
          rest.getForEntity(url("/api/tasks/" + taskId), TaskStatusResponse.class);
      if (status.getStatusCode().is2xxSuccessful() && status.getBody() != null) {
        TaskStatus s = status.getBody().status();
        if (List.of(TaskStatus.COMPLETED, TaskStatus.ERROR, TaskStatus.CANCELLED).contains(s)) {
          return;
        }
      }
      Thread.sleep(50);
    }
    fail("Task did not complete in time: " + taskId);
  }

  private TaskSubmissionRequest swaggerLikeRequest(String wsUrl) {
    Map<String, Object> data = new HashMap<>();
    Map<String, Object> testSpec = new HashMap<>();
    testSpec.put("globalConfig", Map.of(
        "url", wsUrl,
        "timeouts", Map.of("connectionTimeoutMs", 5000, "messageTimeoutMs", 3000)));
    testSpec.put("scenarios", List.of(Map.of(
        "name", "echo",
        "messages", List.of(
            Map.of("name", "hello", "text", "hello", "awaitPattern", "hello", "awaitTimeoutMs", 1000),
            Map.of("name", "ping",  "text", "ping",  "awaitPattern", "pong",  "awaitTimeoutMs", 1000)
        ))));
    Map<String, Object> execution = new HashMap<>();
    execution.put("thinkTime", Map.of("type", "NONE"));
    execution.put("loadModel", Map.of("type", "CLOSED", "users", 1, "iterations", 1));
    data.put("testSpec", testSpec);
    data.put("execution", execution);

    TaskSubmissionRequest req = new TaskSubmissionRequest();
    req.setTaskType("WEBSOCKET");
    req.setData(data);
    return req;
  }
}
