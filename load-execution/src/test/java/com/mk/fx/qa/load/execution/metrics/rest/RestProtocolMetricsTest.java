package com.mk.fx.qa.load.execution.metrics.rest;

import static org.junit.jupiter.api.Assertions.*;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import java.util.List;
import org.junit.jupiter.api.Test;

class RestProtocolMetricsTest {

  @Test
  void emptyMetrics_doesNotPopulateReport() {
    RestProtocolMetrics m = new RestProtocolMetrics();
    TaskRunReport r = new TaskRunReport();
    m.applyTo(r);
    assertNull(r.protocolDetails);
  }

  @Test
  void successAndFailure_areAggregatedWithBreakdownAndLatency() {
    RestProtocolMetrics m = new RestProtocolMetrics();
    // Two successes with latencies 10 and 20
    m.recordSuccess("GET", "/items", 10, 200, 1);
    m.recordSuccess("GET", "/items", 20, 201, 2);
    // One 404 and one 503 failure
    m.recordHttpFailure(404, "GET", "/items", 3);
    m.recordHttpFailure(503, "GET", "/items", 4);

    TaskRunReport r = new TaskRunReport();
    m.applyTo(r);

    assertNotNull(r.protocolDetails);
    assertNotNull(r.protocolDetails.rest);
    assertNotNull(r.protocolDetails.rest.endpoints);
    assertEquals(1, r.protocolDetails.rest.endpoints.size());
    TaskRunReport.RestEndpoint ep = r.protocolDetails.rest.endpoints.get(0);

    assertEquals("GET", ep.method);
    assertEquals("/items", ep.path);
    assertEquals(4, ep.total);
    assertEquals(2, ep.success);
    assertEquals(2, ep.failure);

    assertNotNull(ep.latency);
    assertEquals(10L, ep.latency.min);
    assertEquals(20L, ep.latency.max);
    assertEquals(15L, ep.latency.avg);
    assertNotNull(ep.latency.p95);
    assertNotNull(ep.latency.p99);
    assertTrue(ep.latency.p95 >= 10 && ep.latency.p95 <= 20);
    assertTrue(ep.latency.p99 >= 10 && ep.latency.p99 <= 20);

    assertNotNull(ep.statusBreakdown);
    assertEquals(2L, ep.statusBreakdown.get("2xx"));
    assertEquals(1L, ep.statusBreakdown.get("HTTP_4xx"));
    assertEquals(1L, ep.statusBreakdown.get("HTTP_5xx"));
    assertFalse(Boolean.TRUE.equals(ep.outlierDetected));
  }

  @Test
  void outlierDetection_setsFieldsBasedOnMaxVsP95() {
    RestProtocolMetrics m = new RestProtocolMetrics();
    // Many fast successes
    for (int i = 0; i < 20; i++) m.recordSuccess("GET", "/slow", 10, 200, i);
    // One very slow request; ensure userId propagated
    m.recordSuccess("GET", "/slow", 1000, 200, 42);

    TaskRunReport r = new TaskRunReport();
    m.applyTo(r);

    TaskRunReport.RestEndpoint ep = r.protocolDetails.rest.endpoints.get(0);
    assertTrue(Boolean.TRUE.equals(ep.outlierDetected));
    assertNotNull(ep.outlierInfo);
    assertNotNull(ep.outlierTimestamp);
    assertEquals(42, ep.likelyAffectedUser);
    assertTrue(ep.latency.max > 5 * ep.latency.p95);
  }

  @Test
  void onlyFailures_producesNullAvgAndPercentiles() {
    RestProtocolMetrics m = new RestProtocolMetrics();
    m.recordHttpFailure(418, "POST", "/teapot", 7);
    m.recordHttpFailure(404, "POST", "/teapot", 7);

    TaskRunReport r = new TaskRunReport();
    m.applyTo(r);

    TaskRunReport.RestEndpoint ep = r.protocolDetails.rest.endpoints.get(0);
    assertEquals(2, ep.total);
    assertEquals(0, ep.success);
    assertEquals(2, ep.failure);
    assertNull(ep.latency.avg);
    assertNull(ep.latency.p95);
    assertNull(ep.latency.p99);
    // min/max default to 0 when no successes were recorded (no latency samples)
    assertEquals(0L, ep.latency.min);
    assertEquals(0L, ep.latency.max);
    assertEquals(2L, ep.statusBreakdown.get("HTTP_4xx"));
  }

  @Test
  void multipleEndpoints_areTrackedSeparately() {
    RestProtocolMetrics m = new RestProtocolMetrics();
    m.recordSuccess("GET", "/a", 5, 200, 1);
    m.recordSuccess("POST", "/b", 15, 201, 2);
    m.recordHttpFailure(500, "GET", "/a", 3);

    TaskRunReport r = new TaskRunReport();
    m.applyTo(r);

    List<TaskRunReport.RestEndpoint> eps = r.protocolDetails.rest.endpoints;
    assertEquals(2, eps.size());
    TaskRunReport.RestEndpoint a =
        eps.stream().filter(e -> "/a".equals(e.path)).findFirst().orElseThrow();
    TaskRunReport.RestEndpoint b =
        eps.stream().filter(e -> "/b".equals(e.path)).findFirst().orElseThrow();

    assertEquals(2, a.total);
    assertEquals(1, a.success);
    assertEquals(1, a.failure);
    assertEquals(1, b.total);
    assertEquals(1, b.success);
    assertEquals(0, b.failure);
  }
}
