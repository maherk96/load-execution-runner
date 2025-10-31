package com.mk.fx.qa.load.execution.processors.rest;

import static org.junit.jupiter.api.Assertions.*;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionRequest;
import com.mk.fx.qa.load.execution.metrics.LoadMetricsRegistry;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RestLoadTaskProcessorTest {

  private HttpServer server;
  private String baseUrl;

  @BeforeEach
  void setUp() throws Exception {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    // /ok returns 200
    server.createContext(
        "/ok",
        new HttpHandler() {
          @Override
          public void handle(HttpExchange exchange) throws IOException {
            byte[] body = "OK".getBytes();
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
              os.write(body);
            }
          }
        });
    // /err returns 500
    server.createContext(
        "/err",
        new HttpHandler() {
          @Override
          public void handle(HttpExchange exchange) throws IOException {
            byte[] body = "ERR".getBytes();
            exchange.sendResponseHeaders(500, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
              os.write(body);
            }
          }
        });
    server.start();
    baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
  }

  @AfterEach
  void tearDown() {
    if (server != null) server.stop(0);
  }

  @Test
  void closedModel_executes_andRecordsFailuresAndSuccesses() throws Exception {
    var registry = new LoadMetricsRegistry();
    var processor = new RestLoadTaskProcessor(registry);
    var taskId = UUID.randomUUID();

    TaskSubmissionRequest req = buildClosedRequest(taskId, baseUrl, List.of("/ok", "/err"));

    processor.execute(req);

    // Validate report and basic metrics
    Optional<TaskRunReport> reportOpt = registry.getReport(taskId);
    assertTrue(reportOpt.isPresent());
    TaskRunReport report = reportOpt.get();
    assertNotNull(report.metrics);
    assertEquals(2, report.metrics.totalRequests);
    assertEquals(1, report.metrics.failureCount);
    assertEquals(1, report.metrics.successCount);

    assertNotNull(report.protocolDetails);
    assertNotNull(report.protocolDetails.rest);
    assertEquals(2, report.protocolDetails.rest.endpoints.size());
  }

  @Test
  void openModel_clientFailure_cancelsAndProducesReport() throws Exception {
    var registry = new LoadMetricsRegistry();
    var processor = new RestLoadTaskProcessor(registry);
    var taskId = UUID.randomUUID();

    // Use an unreachable port to force connection failure
    String badBaseUrl = "http://127.0.0.1:1";
    TaskSubmissionRequest req =
        buildOpenRequest(taskId, badBaseUrl, List.of("/ok"), 5.0, 2, "200ms");

    processor.execute(req);

    var report = registry.getReport(taskId).orElseThrow();
    assertNotNull(report.metrics);
    // Expect cancellation due to iteration exception in open executor
    assertEquals("CANCELLED", report.testCompletion.reason);
  }

  @Test
  void validation_missingSections_throw() {
    var registry = new LoadMetricsRegistry();
    var processor = new RestLoadTaskProcessor(registry);
    var taskId = UUID.randomUUID();

    TaskSubmissionRequest req = new TaskSubmissionRequest();
    req.setTaskId(taskId.toString());
    req.setTaskType("REST");
    // No data/testSpec
    req.setData(new HashMap<>());

    assertThrows(IllegalArgumentException.class, () -> processor.execute(req));
  }

  @Test
  void missingBaseUrl_throwsIllegalArgument() {
    var registry = new LoadMetricsRegistry();
    var processor = new RestLoadTaskProcessor(registry);
    var taskId = UUID.randomUUID();

    Map<String, Object> data = new HashMap<>();
    Map<String, Object> testSpec = new HashMap<>();
    // globalConfig with no baseUrl
    testSpec.put("globalConfig", Map.of());
    testSpec.put(
        "scenarios",
        List.of(Map.of("name", "s1", "requests", List.of(Map.of("method", "GET", "path", "/ok")))));
    Map<String, Object> exec = new HashMap<>();
    exec.put("thinkTime", Map.of("type", "NONE"));
    exec.put("loadModel", Map.of("type", "CLOSED", "users", 1, "iterations", 1));
    data.put("testSpec", testSpec);
    data.put("execution", exec);

    TaskSubmissionRequest req = new TaskSubmissionRequest();
    req.setTaskId(taskId.toString());
    req.setTaskType("REST");
    req.setData(data);

    assertThrows(IllegalArgumentException.class, () -> processor.execute(req));
  }

  private TaskSubmissionRequest buildClosedRequest(
      UUID taskId, String baseUrl, List<String> paths) {
    Map<String, Object> data = new HashMap<>();
    Map<String, Object> testSpec = new HashMap<>();
    testSpec.put("globalConfig", Map.of("baseUrl", baseUrl));
    List<Map<String, Object>> requests =
        paths.stream()
            .map(
                p ->
                    (Map<String, Object>)
                        new HashMap<String, Object>() {
                          {
                            put("method", "GET");
                            put("path", p);
                          }
                        })
            .toList();
    testSpec.put("scenarios", List.of(Map.of("name", "s1", "requests", requests)));
    Map<String, Object> exec = new HashMap<>();
    exec.put("thinkTime", Map.of("type", "NONE"));
    exec.put("loadModel", Map.of("type", "CLOSED", "users", 1, "iterations", 1));
    data.put("testSpec", testSpec);
    data.put("execution", exec);

    TaskSubmissionRequest req = new TaskSubmissionRequest();
    req.setTaskId(taskId.toString());
    req.setTaskType("REST");
    req.setData(data);
    return req;
  }

  private TaskSubmissionRequest buildOpenRequest(
      UUID taskId,
      String baseUrl,
      List<String> paths,
      double rate,
      int maxConc,
      String durationIso) {
    Map<String, Object> data = new HashMap<>();
    Map<String, Object> testSpec = new HashMap<>();
    testSpec.put("globalConfig", Map.of("baseUrl", baseUrl));
    List<Map<String, Object>> requests =
        paths.stream()
            .map(
                p ->
                    (Map<String, Object>)
                        new HashMap<String, Object>() {
                          {
                            put("method", "GET");
                            put("path", p);
                          }
                        })
            .toList();
    testSpec.put("scenarios", List.of(Map.of("name", "s1", "requests", requests)));
    Map<String, Object> exec = new HashMap<>();
    exec.put("thinkTime", Map.of("type", "NONE"));
    exec.put(
        "loadModel",
        Map.of(
            "type", "OPEN",
            "arrivalRatePerSec", rate,
            "maxConcurrent", maxConc,
            "duration", durationIso));
    data.put("testSpec", testSpec);
    data.put("execution", exec);

    TaskSubmissionRequest req = new TaskSubmissionRequest();
    req.setTaskId(taskId.toString());
    req.setTaskType("REST");
    req.setData(data);
    return req;
  }
}
