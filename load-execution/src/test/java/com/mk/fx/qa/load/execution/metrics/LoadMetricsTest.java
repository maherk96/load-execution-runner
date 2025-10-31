package com.mk.fx.qa.load.execution.metrics;

import static org.junit.jupiter.api.Assertions.*;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import com.mk.fx.qa.load.execution.model.LoadModelType;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LoadMetricsTest {

  private static TaskConfig closedConfig() {
    return new TaskConfig(
        "task-1",
        "REST",
        "http://localhost",
        LoadModelType.CLOSED,
        2,
        3,
        Duration.ZERO,
        Duration.ZERO,
        Duration.ofSeconds(1),
        null,
        null,
        2,
        12,
        null);
  }

  private static TaskConfig openConfig() {
    return new TaskConfig(
        "task-open",
        "REST",
        "http://localhost",
        LoadModelType.OPEN,
        null,
        null,
        null,
        null,
        null,
        10.0,
        Duration.ZERO,
        1,
        5,
        25.0);
  }

  @Test
  void zeroState_hasNoLatenciesAndNoRequests() {
    LoadMetrics m = new LoadMetrics(closedConfig());

    assertEquals(0, m.totalRequests());
    assertEquals(0, m.totalErrors());
    assertEquals(0, m.totalUsersStarted());
    assertEquals(0, m.totalUsersCompleted());
    assertTrue(m.latencyAvgMs().isEmpty());
    assertTrue(m.latencyMinMs().isEmpty());
    assertTrue(m.latencyMaxMs().isEmpty());
    assertTrue(m.latencyP95Ms().isEmpty());
    assertTrue(m.latencyP99Ms().isEmpty());
    assertTrue(m.getTimeSeries().isEmpty());

    var snap = m.snapshotNow();
    assertEquals(0, snap.totalRequests());
    assertEquals(0, snap.totalErrors());
    assertNull(snap.latencyAvgMs());
    assertNull(snap.latencyMinMs());
    assertNull(snap.latencyMaxMs());
    assertNotNull(snap.achievedRps());
    assertTrue(snap.achievedRps() >= 0.0);
    assertTrue(snap.activeUserIterations().isEmpty());
  }

  @Test
  void recordSuccessAndFailure_updatesCountsAndLatencies() {
    LoadMetrics m = new LoadMetrics(closedConfig());

    m.recordRequestSuccess(10);
    m.recordRequestSuccess(20);
    m.recordRequestFailure(new RuntimeException("boom"));

    assertEquals(3, m.totalRequests());
    assertEquals(1, m.totalErrors());
    assertTrue(m.latencyAvgMs().isPresent());
    assertEquals(10L, m.latencyMinMs().orElseThrow());
    assertEquals(20L, m.latencyMaxMs().orElseThrow());
    assertTrue(m.latencyP95Ms().isPresent());
    assertTrue(m.latencyP99Ms().isPresent());

    // error breakdown present when failures recorded (may be classified generically)
    var breakdown = m.errorBreakdown();
    assertFalse(breakdown.isEmpty());
  }

  @Test
  void usersFlow_updatesCounts_andSnapshotShowsActiveIterations() {
    LoadMetrics m = new LoadMetrics(closedConfig());

    m.recordUserStarted(0);
    m.recordUserProgress(0, 2);
    m.recordUserStarted(1);

    assertEquals(2, m.totalUsersStarted());
    var snap = m.snapshotNow();
    // current iterations include user 0 -> 2, user 1 -> 0 (implicitly added on start)
    assertEquals(2, snap.activeUserIterations().size());
    assertEquals(2, snap.activeUserIterations().get(0));

    m.recordUserCompleted(0, 3);
    m.recordUserCompleted(1, 2);
    assertEquals(2, m.totalUsersCompleted());
  }

  @Test
  void timeSeries_snapshotReflectsWindowAndResets() {
    LoadMetrics m = new LoadMetrics(closedConfig());

    // Record two successes and one categorized failure (with latency) in this window
    m.recordRequestSuccess(10);
    m.recordRequestSuccess(20);
    m.recordFailure("HTTP_500", 30);

    m.forceSnapshotForTest();
    List<TimeSeriesPoint> ts = m.getTimeSeries();
    assertEquals(1, ts.size());
    TimeSeriesPoint p = ts.get(0);
    assertEquals(3, p.totalRequests());
    assertEquals(1, p.totalErrors());
    assertEquals(10, p.latMinMs());
    assertEquals(30, p.latMaxMs());
    assertEquals(20, p.latAvgMs());

    // Next window without activity should show zeros
    m.forceSnapshotForTest();
    ts = m.getTimeSeries();
    assertEquals(2, ts.size());
    TimeSeriesPoint p2 = ts.get(1);
    assertEquals(0, p2.latMinMs());
    assertEquals(0, p2.latAvgMs());
    assertEquals(0, p2.latMaxMs());
  }

  @Test
  void buildReport_includesProtocolProviders_andCompletionContext() {
    LoadMetrics m = new LoadMetrics(closedConfig());
    m.recordRequestSuccess(5);
    m.recordRequestFailure(new IllegalArgumentException("bad"));
    m.recordUserStarted(0);
    m.recordUserCompleted(0, 3);
    m.forceSnapshotForTest();

    // Add a protocol metrics provider that populates REST details
    m.registerProtocolMetrics(
        new ProtocolMetricsProvider() {
          @Override
          public void applyTo(TaskRunReport report) {
            if (report.protocolDetails == null) {
              report.protocolDetails = new TaskRunReport.ProtocolDetails();
            }
            TaskRunReport.RestDetails rd = new TaskRunReport.RestDetails();
            TaskRunReport.RestEndpoint ep = new TaskRunReport.RestEndpoint();
            ep.method = "GET";
            ep.path = "/health";
            ep.total = 1;
            rd.endpoints = List.of(ep);
            report.protocolDetails.rest = rd;
          }
        });

    // Mark cancelled to check completion reason
    m.setCompletionContext(true, null, 1, 1);

    TaskRunReport report = m.buildReport();
    assertNotNull(report.metrics);
    assertTrue(report.metrics.totalRequests >= 1);
    assertNotNull(report.protocolDetails);
    assertNotNull(report.protocolDetails.rest);
    assertNotNull(report.protocolDetails.rest.endpoints);
    assertEquals("CANCELLED", report.testCompletion.reason);
  }

  @Test
  void buildReport_closedModel_computesExpectedRps_andHoldExpiredReason() {
    LoadMetrics m = new LoadMetrics(closedConfig());
    // Minimal activity
    m.recordRequestSuccess(10);

    TaskRunReport report = m.buildReport();
    assertNotNull(report.config);
    // expectedRps derived from expectedTotalRequests / hold seconds (12 / 1s)
    assertNotNull(report.config.expectedRps);
    assertEquals(12.0, report.config.expectedRps, 0.5);
    assertNotNull(report.testCompletion);
    assertEquals("HOLD_EXPIRED", report.testCompletion.reason);
    assertEquals(Duration.ofSeconds(1), report.config.holdFor);
  }

  @Test
  void buildReport_openModel_usesProvidedExpectedRps_andHoldExpiredReason() {
    LoadMetrics m = new LoadMetrics(openConfig());
    // Minimal activity
    m.recordRequestSuccess(5);

    TaskRunReport report = m.buildReport();
    assertNotNull(report.config);
    // expectedRps passed through from TaskConfig
    assertEquals(25.0, report.config.expectedRps, 0.01);
    assertEquals(Duration.ZERO, report.config.openDuration);
    // With duration=0 and immediate build, treated as hold expired for OPEN
    assertEquals("HOLD_EXPIRED", report.testCompletion.reason);
  }
}
