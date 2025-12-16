package com.mk.fx.qa.load.execution.processors.rest;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionRequest;
import com.mk.fx.qa.load.execution.metrics.LoadMetricsRegistry;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;

class RestLoadProcessorLargeDatasetTest {

  private FakeLoadHttpServer fakeServer;
  private String baseUrl;

  @BeforeEach
  void setUp() throws Exception {

    fakeServer = new FakeLoadHttpServer(Map.of(
        "/api/users", new EndpointProfile(
            "users", 10, 50, 200, 0.95, false, 512
        ),
        "/api/posts", new EndpointProfile(
            "posts", 50, 150, 200, 0.90, false, 1024
        ),
        "/api/comments", new EndpointProfile(
            "comments", 100, 300, 200, 0.85, false, 2048
        ),
        "/api/analytics", new EndpointProfile(
            "analytics", 20, 80, 200, 0.80, false, 256
        ),
        "/api/orders", new EndpointProfile(
            "orders", 30, 100, 201, 0.92, false, 512
        ),
        "/api/reports", new EndpointProfile(
            "reports", 200, 500, 200, 0.75, true, 1024
        )
    ));

    fakeServer.start();
    baseUrl = fakeServer.baseUrl();

    System.out.println("Fake load server started at " + baseUrl);
  }

  @AfterEach
  void tearDown() {
    fakeServer.stop();
    System.out.println("Fake load server stopped");
  }

  @Test
  void closedModel_largeDataset_generatesComprehensiveReport() throws Exception {

    var registry = new LoadMetricsRegistry();
    var processor = new RestLoadTaskProcessor(registry);
    var taskId = UUID.randomUUID();

    TaskSubmissionRequest req = buildClosedLoadRequest(taskId, baseUrl);

    System.out.println("Starting CLOSED load test...");
    long start = System.currentTimeMillis();

    processor.execute(req);

    long duration = System.currentTimeMillis() - start;
    System.out.println("Test completed in " + duration + "ms");
    System.out.println("Total requests received: " + fakeServer.totalRequests());

    printEndpointBreakdown();

    TaskRunReport report = registry.getReport(taskId)
        .orElseThrow(() -> new AssertionError("Report not found"));

    printReport(report);

    assert report.metrics != null;
    assert report.metrics.totalRequests > 0;
    assert report.protocolDetails != null;
    assert report.protocolDetails.rest != null;
  }

  @Test
  void openModel_largeDataset_generatesComprehensiveReport() throws Exception {

    var registry = new LoadMetricsRegistry();
    var processor = new RestLoadTaskProcessor(registry);
    var taskId = UUID.randomUUID();

    TaskSubmissionRequest req = buildOpenLoadRequest(taskId, baseUrl);

    System.out.println("Starting OPEN load test...");
    long start = System.currentTimeMillis();

    processor.execute(req);

    long duration = System.currentTimeMillis() - start;
    System.out.println("Test completed in " + duration + "ms");
    System.out.println("Total requests received: " + fakeServer.totalRequests());

    printEndpointBreakdown();

    TaskRunReport report = registry.getReport(taskId)
        .orElseThrow(() -> new AssertionError("Report not found"));

    printReport(report);

    assert report.metrics.totalRequests > 0;
  }

  /* ------------------------------------------------------------------ */

  private void printEndpointBreakdown() {
    fakeServer.endpointCounters().forEach((name, count) ->
        System.out.println("  " + name + ": " + count.get() + " requests"));
  }

  private TaskSubmissionRequest buildClosedLoadRequest(UUID taskId, String baseUrl) {
    Map<String, Object> data = new HashMap<>();

    Map<String, Object> testSpec = new HashMap<>();
    testSpec.put("globalConfig", Map.of(
        "baseUrl", baseUrl,
        "headers", Map.of(
            "Content-Type", "application/json",
            "User-Agent", "LoadTestRunner/1.0"
        )
    ));

    testSpec.put("scenarios", List.of(
        Map.of(
            "name", "E-Commerce Flow",
            "requests", List.of(
                Map.of("method", "GET", "path", "/api/users", "query", Map.of("page", "1")),
                Map.of("method", "GET", "path", "/api/posts", "query", Map.of("limit", "10")),
                Map.of("method", "GET", "path", "/api/comments", "query", Map.of("postId", "1")),
                Map.of("method", "GET", "path", "/api/analytics", "query", Map.of("period", "daily")),
                Map.of("method", "POST", "path", "/api/orders",
                    "body", Map.of("product", "widget", "quantity", 5)),
                Map.of("method", "GET", "path", "/api/reports", "query", Map.of("type", "summary"))
            )
        )
    ));

    data.put("testSpec", testSpec);

    data.put("execution", Map.of(
        "thinkTime", Map.of("type", "RANDOM", "min", 1, "max", 5),
        "loadModel", Map.of(
            "type", "CLOSED",
            "users", 10,
            "iterations", 20,
            "rampUp", "10s",
            "warmup", "5s"
        )
    ));

    TaskSubmissionRequest req = new TaskSubmissionRequest();
    req.setTaskId(taskId.toString());
    req.setTaskType("REST");
    req.setData(data);
    return req;
  }

  private TaskSubmissionRequest buildOpenLoadRequest(UUID taskId, String baseUrl) {
    Map<String, Object> data = new HashMap<>();

    Map<String, Object> testSpec = new HashMap<>();
    testSpec.put("globalConfig", Map.of(
        "baseUrl", baseUrl,
        "headers", Map.of(
            "Content-Type", "application/json",
            "User-Agent", "LoadTestRunner/1.0"
        )
    ));

    testSpec.put("scenarios", List.of(
        Map.of(
            "name", "API Workflow",
            "requests", List.of(
                Map.of("method", "GET", "path", "/api/users"),
                Map.of("method", "GET", "path", "/api/posts"),
                Map.of("method", "GET", "path", "/api/comments"),
                Map.of("method", "GET", "path", "/api/analytics"),
                Map.of("method", "POST", "path", "/api/orders",
                    "body", Map.of("product", "gadget", "quantity", 3)),
                Map.of("method", "GET", "path", "/api/reports")
            )
        )
    ));

    data.put("testSpec", testSpec);

    data.put("execution", Map.of(
        "thinkTime", Map.of("type", "RANDOM", "min", 1, "max", 3),
        "loadModel", Map.of(
            "type", "OPEN",
            "arrivalRatePerSec", 100.0,
            "maxConcurrent", 50,
            "duration", "30s"
        )
    ));

    TaskSubmissionRequest req = new TaskSubmissionRequest();
    req.setTaskId(taskId.toString());
    req.setTaskType("REST");
    req.setData(data);
    return req;
  }

  private void printReport(TaskRunReport report) {

    System.out.println("\n" + "=".repeat(80));
    System.out.println("LOAD TEST REPORT");
    System.out.println("=".repeat(80));

    /* ===================== OVERALL METRICS ===================== */
    if (report.metrics != null) {
      System.out.println("\n📊 OVERALL METRICS:");
      System.out.println("  Total Requests:     " + report.metrics.totalRequests);
      System.out.println("  Successful:         " + report.metrics.successCount);
      System.out.println("  Failed:             " + report.metrics.failureCount);
      System.out.println("  Success Rate:       " +
              String.format("%.2f%%", report.metrics.successRate));
      System.out.println("  Achieved RPS:       " +
              String.format("%.2f", report.metrics.achievedRps));

      /* ---------------- Latency ---------------- */
      if (report.metrics.latency != null) {
        System.out.println("\n📈 LATENCY (ms):");
        System.out.println("  Min:  " + report.metrics.latency.min);
        System.out.println("  Avg:  " + report.metrics.latency.avg);
        System.out.println("  Max:  " + report.metrics.latency.max);
        System.out.println("  p95:  " + report.metrics.latency.p95);
        System.out.println("  p99:  " + report.metrics.latency.p99);
      }

      /* ---------------- User metrics ---------------- */
      if (report.metrics.usersStarted > 0 || report.metrics.usersCompleted > 0) {
        System.out.println("\n👥 USER METRICS:");
        System.out.println("  Users Started:   " + report.metrics.usersStarted);
        System.out.println("  Users Completed: " + report.metrics.usersCompleted);
      }

      /* ---------------- Error breakdown ---------------- */
      if (report.metrics.errorBreakdown != null &&
              !report.metrics.errorBreakdown.isEmpty()) {

        System.out.println("\n❌ ERROR BREAKDOWN:");
        report.metrics.errorBreakdown.forEach(error ->
                System.out.println("  " + error.type + ": " + error.count));
      }
    }

    /* ===================== COMPLETION ===================== */
    if (report.completion != null) {
      System.out.println("\n✅ TEST COMPLETION:");
      System.out.println("  Reason:   " + report.completion.reason);
      System.out.println("  Duration: " +
              String.format("%.2f sec", report.completion.actualDurationSec));
      System.out.println("  Progress: " + report.completion.percentComplete + "%");

      if (report.completion.message != null) {
        System.out.println("  Message:  " + report.completion.message);
      }
    }

    /* ===================== REST PROTOCOL DETAILS ===================== */
    if (report.protocolDetails != null && report.protocolDetails.rest != null) {
      var rest = report.protocolDetails.rest;

      if (rest.endpoints != null && !rest.endpoints.isEmpty()) {
        System.out.println("\n🌐 ENDPOINT BREAKDOWN:");

        rest.endpoints.forEach(endpoint -> {
          System.out.println("\n  " + endpoint.method + " " + endpoint.path + ":");
          System.out.println("    Total:     " + endpoint.total);
          System.out.println("    Success:   " + endpoint.success);
          System.out.println("    Failures:  " + endpoint.failure);

          if (endpoint.latency != null) {
            System.out.println("    Avg Latency: " + endpoint.latency.avg + " ms");
            System.out.println("    p95 Latency: " + endpoint.latency.p95 + " ms");
          }

          if (endpoint.statusBreakdown != null &&
                  !endpoint.statusBreakdown.isEmpty()) {
            System.out.println("    Status codes: " + endpoint.statusBreakdown);
          }
        });
      }
    }

    /* ===================== EXECUTIVE SUMMARY ===================== */
    if (report.executiveSummary != null) {
      System.out.println("\n📋 EXECUTIVE SUMMARY:");
      System.out.println("  " + report.executiveSummary);
    }

    System.out.println("\n" + "=".repeat(80));
  }
  /* ---------------- Your existing printReport(report) stays unchanged ---------------- */
}