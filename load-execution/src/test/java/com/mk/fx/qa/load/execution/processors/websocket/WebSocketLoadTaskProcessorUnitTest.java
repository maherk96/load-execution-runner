package com.mk.fx.qa.load.execution.processors.websocket;

import static org.junit.jupiter.api.Assertions.*;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionRequest;
import com.mk.fx.qa.load.execution.metrics.LoadMetricsRegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WebSocketLoadTaskProcessorUnitTest {

  private LocalEchoWsServer server;
  private String wsUrl;

  @BeforeEach
  void setUp() throws Exception {
    int port = LocalEchoWsServer.findFreePort();
    server = new LocalEchoWsServer(port);
    server.start();
    wsUrl = "ws://127.0.0.1:" + port;
  }

  @AfterEach
  void tearDown() throws Exception {
    if (server != null) server.stop();
  }

  @Test
  void closedModel_echoAndAck_success() throws Exception {
    var registry = new LoadMetricsRegistry();
    var processor = new WebSocketLoadTaskProcessor(registry);
    var taskId = UUID.randomUUID();

    TaskSubmissionRequest req = buildClosedRequest(taskId, wsUrl);
    processor.execute(req);

    Optional<TaskRunReport> reportOpt = registry.getReport(taskId);
    assertTrue(reportOpt.isPresent());
    TaskRunReport report = reportOpt.get();
    assertNotNull(report.metrics);
    assertEquals(2, report.metrics.totalRequests);
    assertEquals(0, report.metrics.failureCount);
    assertEquals(2, report.metrics.successCount);
    assertNotNull(report.protocolDetails);
    assertNotNull(report.protocolDetails.websocket);
  }

  private TaskSubmissionRequest buildClosedRequest(UUID taskId, String wsUrl) {
    Map<String, Object> data = new HashMap<>();
    Map<String, Object> testSpec = new HashMap<>();
    testSpec.put("globalConfig", Map.of("url", wsUrl));

    List<Map<String, Object>> messages =
        List.of(
            Map.of(
                "name", "m1",
                "text", "ping",
                "awaitPattern", "pong",
                "awaitTimeoutMs", 3000),
            Map.of("name", "m2", "text", "hello"));

    testSpec.put("scenarios", List.of(Map.of("name", "s1", "messages", messages)));

    Map<String, Object> exec = new HashMap<>();
    exec.put("thinkTime", Map.of("type", "NONE"));
    exec.put("loadModel", Map.of("type", "CLOSED", "users", 1, "iterations", 1));
    data.put("testSpec", testSpec);
    data.put("execution", exec);

    TaskSubmissionRequest req = new TaskSubmissionRequest();
    req.setTaskId(taskId.toString());
    req.setTaskType("WEBSOCKET");
    req.setData(data);
    return req;
  }
}
