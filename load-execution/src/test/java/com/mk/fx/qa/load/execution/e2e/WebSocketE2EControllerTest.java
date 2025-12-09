package com.mk.fx.qa.load.execution.e2e;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mk.fx.qa.load.execution.LoadExecutionRunnerMain;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskStatusResponse;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionRequest;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionResponse;
import com.mk.fx.qa.load.execution.model.TaskStatus;
import com.mk.fx.qa.load.execution.processors.websocket.LocalEchoWsServer;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

@org.junit.jupiter.api.Disabled("Use MockMvc-based e2e test instead")
@SpringBootTest(
    classes = LoadExecutionRunnerMain.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WebSocketE2EControllerTest {

  @LocalServerPort private int port;
  @Autowired private TestRestTemplate rest;
  @Autowired private ObjectMapper mapper;

  private LocalEchoWsServer wsServer;
  private String wsUrl;

  @BeforeEach
  void startEcho() throws Exception {
    int wsPort = LocalEchoWsServer.findFreePort();
    wsServer = new LocalEchoWsServer(wsPort);
    wsServer.start();
    wsUrl = "ws://127.0.0.1:" + wsPort;
  }

  @AfterEach
  void stopEcho() throws Exception {
    if (wsServer != null) wsServer.stop();
  }

  @Test
  void e2e_closed_and_open_models_generate_reports_and_metrics() throws Exception {
    // Sanity check controller is up
    ResponseEntity<String> health = rest.getForEntity(url("/api/tasks/healthy"), String.class);
    assertEquals(200, health.getStatusCode().value());

    UUID closedId = submitTask(buildClosed(wsUrl));
    awaitCompletion(closedId, Duration.ofSeconds(5));

    // Verify metrics and report for CLOSED model
    ResponseEntity<Map> metricsClosed =
        rest.getForEntity(url("/api/tasks/" + closedId + "/metrics"), Map.class);
    assertEquals(200, metricsClosed.getStatusCode().value());
    ResponseEntity<TaskRunReport> reportClosed =
        rest.getForEntity(url("/api/tasks/" + closedId + "/report"), TaskRunReport.class);
    assertEquals(200, reportClosed.getStatusCode().value());
    assertNotNull(reportClosed.getBody());
    assertEquals("WEBSOCKET", reportClosed.getBody().taskType);
    assertNotNull(reportClosed.getBody().protocolDetails);
    assertNotNull(reportClosed.getBody().protocolDetails.websocket);
    assertFalse(reportClosed.getBody().protocolDetails.websocket.messages.isEmpty());

    // Submit OPEN model
    UUID openId = submitTask(buildOpen(wsUrl, 5.0, 2, "200ms"));
    awaitCompletion(openId, Duration.ofSeconds(5));

    ResponseEntity<TaskRunReport> reportOpen =
        rest.getForEntity(url("/api/tasks/" + openId + "/report"), TaskRunReport.class);
    assertEquals(200, reportOpen.getStatusCode().value());
    assertNotNull(reportOpen.getBody());
    assertEquals("WEBSOCKET", reportOpen.getBody().taskType);
    assertNotNull(reportOpen.getBody().metrics);
    // Expect exactly one iteration -> 2 messages
    assertEquals(2, reportOpen.getBody().metrics.totalRequests);

    // Verify controller auxiliary endpoints work
    ResponseEntity<Map> queue = rest.getForEntity(url("/api/tasks/queue"), Map.class);
    assertEquals(200, queue.getStatusCode().value());

    ResponseEntity<Set> types = rest.getForEntity(url("/api/tasks/types"), Set.class);
    assertEquals(200, types.getStatusCode().value());
    assertTrue(types.getBody().toString().contains("WEBSOCKET"));

    ResponseEntity<List> all = rest.getForEntity(url("/api/tasks"), List.class);
    assertEquals(200, all.getStatusCode().value());

    ResponseEntity<List> history = rest.getForEntity(url("/api/tasks/history"), List.class);
    assertEquals(200, history.getStatusCode().value());
    assertFalse(history.getBody().isEmpty());
  }

  private String url(String path) {
    return "http://127.0.0.1:" + port + path;
  }

  private UUID submitTask(TaskSubmissionRequest req) throws Exception {
    var headers = new org.springframework.http.HttpHeaders();
    headers.set(org.springframework.http.HttpHeaders.CONTENT_TYPE, "application/json");
    headers.set(org.springframework.http.HttpHeaders.ACCEPT, "application/json");
    String body = mapper.writeValueAsString(req);
    ResponseEntity<String> resp =
        rest.exchange(
            url("/api/tasks"), HttpMethod.POST, new HttpEntity<>(body, headers), String.class);
    assertTrue(resp.getStatusCode().is2xxSuccessful(), "Unexpected status: " + resp);
    TaskSubmissionResponse out = mapper.readValue(resp.getBody(), TaskSubmissionResponse.class);
    assertNotNull(out);
    return out.taskId();
  }

  private void awaitCompletion(UUID taskId, Duration timeout) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      ResponseEntity<TaskStatusResponse> status =
          rest.getForEntity(url("/api/tasks/" + taskId), TaskStatusResponse.class);
      if (status.getStatusCode().is2xxSuccessful() && status.getBody() != null) {
        TaskStatus s = status.getBody().status();
        if (s == TaskStatus.COMPLETED || s == TaskStatus.ERROR || s == TaskStatus.CANCELLED) {
          return;
        }
      }
      Thread.sleep(50);
    }
    fail("Task did not complete in time: " + taskId);
  }

  private TaskSubmissionRequest buildClosed(String wsUrl) {
    Map<String, Object> data = new HashMap<>();
    Map<String, Object> testSpec = new HashMap<>();
    testSpec.put("globalConfig", Map.of("url", wsUrl));

    List<Map<String, Object>> messages =
        List.of(
            Map.of(
                "name", "m1",
                "text", "ping",
                "awaitPattern", "pong",
                "awaitTimeoutMs", 1000),
            Map.of("name", "m2", "text", "hello"));

    testSpec.put("scenarios", List.of(Map.of("name", "s1", "messages", messages)));

    Map<String, Object> exec = new HashMap<>();
    exec.put("thinkTime", Map.of("type", "NONE"));
    exec.put("loadModel", Map.of("type", "CLOSED", "users", 1, "iterations", 1));
    data.put("testSpec", testSpec);
    data.put("execution", exec);

    TaskSubmissionRequest req = new TaskSubmissionRequest();
    req.setTaskType("WEBSOCKET");
    req.setData(data);
    return req;
  }

  private TaskSubmissionRequest buildOpen(String wsUrl, double rate, int maxConc, String duration) {
    Map<String, Object> data = new HashMap<>();
    Map<String, Object> testSpec = new HashMap<>();
    testSpec.put("globalConfig", Map.of("url", wsUrl));

    List<Map<String, Object>> messages =
        List.of(
            Map.of(
                "name", "m1",
                "text", "ping",
                "awaitPattern", "pong",
                "awaitTimeoutMs", 1000),
            Map.of("name", "m2", "text", "hello"));

    testSpec.put("scenarios", List.of(Map.of("name", "s1", "messages", messages)));

    Map<String, Object> exec = new HashMap<>();
    exec.put("thinkTime", Map.of("type", "NONE"));
    exec.put(
        "loadModel",
        Map.of(
            "type", "OPEN",
            "arrivalRatePerSec", rate,
            "maxConcurrent", maxConc,
            "duration", duration));
    data.put("testSpec", testSpec);
    data.put("execution", exec);

    TaskSubmissionRequest req = new TaskSubmissionRequest();
    req.setTaskType("WEBSOCKET");
    req.setData(data);
    return req;
  }
}
