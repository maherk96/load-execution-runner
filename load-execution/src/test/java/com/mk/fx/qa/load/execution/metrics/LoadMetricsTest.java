package com.mk.fx.qa.load.execution.metrics;

import static org.junit.jupiter.api.Assertions.*;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import com.mk.fx.qa.load.execution.model.LoadModelType;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;

/**
 * LoadMetrics behaviour and concurrency tests.
 *
 * <p>This suite is intentionally behaviour-focused and includes a concurrency stress test to
 * validate atomicity and thread-safety under load.
 */
@DisplayName("LoadMetrics")
class LoadMetricsTest {

  // Keep tests fast & deterministic in CI
  private static final int CI_THREADS = Math.max(2, Runtime.getRuntime().availableProcessors());
  private static final Duration TEST_TIMEOUT = Duration.ofSeconds(10);

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

  private static LoadMetrics newClosedMetrics() {
    return new LoadMetrics(closedConfig());
  }

  private static LoadMetrics newOpenMetrics() {
    return new LoadMetrics(openConfig());
  }

  @Nested
  @DisplayName("Zero state")
  class ZeroState {

    @Test
    @DisplayName("Has no requests, no errors, no latencies, and empty time series")
    void zeroState_hasNoLatenciesAndNoRequests() {
      LoadMetrics m = newClosedMetrics();

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
  }

  @Nested
  @DisplayName("Request recording")
  class RequestRecording {

    @Test
    @DisplayName("Successes update counts and latency trackers; failures increment errors")
    void recordSuccessAndFailure_updatesCountsAndLatencies() {
      LoadMetrics m = newClosedMetrics();

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

      var breakdown = m.errorBreakdown();
      assertFalse(breakdown.isEmpty());
    }

    @Test
    @DisplayName("Failure without latency does not create latency samples")
    void recordFailure_withoutLatency_doesNotCreateLatencySamples() {
      LoadMetrics m = newClosedMetrics();

      m.recordRequestFailure(new RuntimeException("boom"));

      assertEquals(1, m.totalRequests());
      assertEquals(1, m.totalErrors());
      assertTrue(m.latencyMinMs().isEmpty());
      assertTrue(m.latencyAvgMs().isEmpty());
      assertTrue(m.latencyMaxMs().isEmpty());
      // p95/p99 should also be empty
      assertTrue(m.latencyP95Ms().isEmpty());
      assertTrue(m.latencyP99Ms().isEmpty());
    }

    @Test
    @DisplayName("Categorised failure records latency and increments category counts")
    void recordFailure_category_recordsLatencyAndCategory() {
      LoadMetrics m = newClosedMetrics();

      m.recordFailure("HTTP_5xx", 50);

      assertEquals(1, m.totalRequests());
      assertEquals(1, m.totalErrors());
      assertEquals(50L, m.latencyMinMs().orElseThrow());
      assertEquals(50L, m.latencyMaxMs().orElseThrow());
      assertTrue(m.errorBreakdown().containsKey("HTTP_5xx"));
    }
  }

  @Nested
  @DisplayName("User lifecycle")
  class UserLifecycle {

    @Test
    @DisplayName("User start/progress/completion updates counts and snapshot active iterations")
    void usersFlow_updatesCounts_andSnapshotShowsActiveIterations() {
      LoadMetrics m = newClosedMetrics();

      m.recordUserStarted(0);
      m.recordUserProgress(0, 2);
      m.recordUserStarted(1);

      assertEquals(2, m.totalUsersStarted());

      var snap = m.snapshotNow();
      assertEquals(2, snap.activeUserIterations().size());
      assertEquals(2, snap.activeUserIterations().get(0));

      m.recordUserCompleted(0, 3);
      m.recordUserCompleted(1, 2);
      assertEquals(2, m.totalUsersCompleted());
    }
  }

  @Nested
  @DisplayName("Time series snapshots")
  class TimeSeriesSnapshots {

    @Test
    @DisplayName("Snapshot captures current window and resets for next interval")
    void timeSeries_snapshotReflectsWindowAndResets() {
      LoadMetrics m = newClosedMetrics();

      // Window 1
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

      // Window 2 (no activity)
      m.forceSnapshotForTest();
      TimeSeriesPoint p2 = m.getTimeSeries().get(1);

      assertEquals(0, p2.totalRequests());
      assertEquals(0, p2.totalErrors());
      assertEquals(0, p2.latMinMs());
      assertEquals(0, p2.latAvgMs());
      assertEquals(0, p2.latMaxMs());
    }
  }

  @Nested
  @DisplayName("Report building")
  class ReportBuilding {

    @Test
    @DisplayName("Report includes protocol providers and cancellation completion reason")
    void buildReport_includesProtocolProviders_andCompletionContext() {
      LoadMetrics m = newClosedMetrics();

      m.recordRequestSuccess(5);
      m.recordRequestFailure(new IllegalArgumentException("bad"));
      m.recordUserStarted(0);
      m.recordUserCompleted(0, 3);
      m.forceSnapshotForTest();

      m.registerProtocolMetrics(
              report -> {
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
              });

      m.setCompletionContext(true, null, 1, 1);

      TaskRunReport report = m.buildReport();
      assertNotNull(report.metrics);
      assertTrue(report.metrics.totalRequests >= 1);
      assertNotNull(report.protocolDetails);
      assertNotNull(report.protocolDetails.rest);
      assertNotNull(report.protocolDetails.rest.endpoints);
      assertEquals("CANCELLED", report.completion.reason.toString());
    }

    @Test
    @DisplayName("Completion precedence: CANCELLED overrides HOLD_EXPIRED")
    void completionContext_cancelledOverridesHoldExpired() {
      LoadMetrics m = newClosedMetrics();
      m.setCompletionContext(true, true, 2, 0);

      TaskRunReport report = m.buildReport();
      assertEquals("CANCELLED", report.completion.reason.toString());
    }

    @Test
    @DisplayName("Closed model derives expected RPS from expectedTotalRequests / hold duration")
    void buildReport_closedModel_computesExpectedRps_andHoldExpiredReason() {
      LoadMetrics m = newClosedMetrics();

      m.recordRequestSuccess(10);
      m.setCompletionContext(false, true, 2, 0);

      TaskRunReport report = m.buildReport();
      assertNotNull(report.execution);
      assertNotNull(report.execution.expectedRps);
      assertEquals(12.0, report.execution.expectedRps, 0.5);
      assertNotNull(report.completion);
      assertEquals("HOLD_EXPIRED", report.completion.reason.toString());
      assertNotNull(report.execution.closed);
      assertEquals(Duration.ofSeconds(1), report.execution.closed.holdFor);
    }

    @Test
    @DisplayName("Open model uses configured expected RPS and uses open duration in report config")
    void buildReport_openModel_usesProvidedExpectedRps_andHoldExpiredReason() {
      LoadMetrics m = newOpenMetrics();

      m.recordRequestSuccess(5);

      TaskRunReport report = m.buildReport();
      assertNotNull(report.execution);
      assertEquals(25.0, report.execution.expectedRps, 0.01);
      assertNotNull(report.execution.open);
      assertEquals(Duration.ZERO, report.execution.open.duration);
      assertEquals("HOLD_EXPIRED", report.completion.reason.toString());
    }
  }

  @Nested
  @DisplayName("Concurrency & heavy load safety")
  class ConcurrencyAndHeavyLoad {

    @Test
    @Timeout(10)
    @DisplayName("Concurrent request recording is atomic and does not throw under load")
    void concurrentRecording_isThreadSafe_andCountsMatch() throws Exception {
      LoadMetrics m = newClosedMetrics();

      int threads = CI_THREADS;
      int opsPerThread = 25_000; // ~200k operations on an 8-core box
      int totalOps = threads * opsPerThread;

      ExecutorService pool = Executors.newFixedThreadPool(threads);
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threads);

      AtomicInteger expectedErrors = new AtomicInteger();

      for (int t = 0; t < threads; t++) {
        pool.submit(
                () -> {
                  try {
                    start.await();
                    for (int i = 0; i < opsPerThread; i++) {
                      // 90% success, 10% failure-with-latency to stress both paths
                      if ((i % 10) == 0) {
                        expectedErrors.incrementAndGet();
                        m.recordRequestFailure(new RuntimeException("boom"), 7);
                      } else {
                        m.recordRequestSuccess(5);
                      }
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } finally {
                    done.countDown();
                  }
                });
      }

      start.countDown();
      assertTrue(done.await(8, TimeUnit.SECONDS), "Workers did not finish in time");
      pool.shutdownNow();

      assertEquals(totalOps, m.totalRequests(), "Total request count mismatch under concurrency");
      assertEquals(expectedErrors.get(), m.totalErrors(), "Error count mismatch under concurrency");

      // Latency should exist because we recorded both successes and failure-with-latency
      assertTrue(m.latencyMinMs().isPresent());
      assertTrue(m.latencyMaxMs().isPresent());

      // Building report should be safe after concurrent writes
      TaskRunReport report = m.buildReport();
      assertNotNull(report);
      assertNotNull(report.metrics);
      assertTrue(report.metrics.totalRequests >= totalOps);
    }

    @Test
    @Timeout(10)
    @DisplayName("Protocol providers can be applied after heavy concurrent updates")
    void protocolProviders_applyAfterConcurrentUpdates() throws Exception {
      LoadMetrics m = newClosedMetrics();

      // Simulate a protocol provider that would be updated concurrently elsewhere
      // (we only verify LoadMetrics can still build report correctly with providers present).
      m.registerProtocolMetrics(
              report -> {
                if (report.protocolDetails == null) {
                  report.protocolDetails = new TaskRunReport.ProtocolDetails();
                }
                // minimal marker
                TaskRunReport.RestDetails rd = new TaskRunReport.RestDetails();
                report.protocolDetails.rest = rd;
              });

      int threads = CI_THREADS;
      int opsPerThread = 15_000;

      ExecutorService pool = Executors.newFixedThreadPool(threads);
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threads);

      for (int t = 0; t < threads; t++) {
        pool.submit(
                () -> {
                  try {
                    start.await();
                    for (int i = 0; i < opsPerThread; i++) {
                      m.recordRequestSuccess(3);
                      if ((i % 25) == 0) {
                        m.recordRequestFailure(new RuntimeException("x"), 4);
                      }
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } finally {
                    done.countDown();
                  }
                });
      }

      start.countDown();
      assertTrue(done.await(8, TimeUnit.SECONDS), "Workers did not finish in time");
      pool.shutdownNow();

      TaskRunReport report = m.buildReport();
      assertNotNull(report.protocolDetails);
      assertNotNull(report.protocolDetails.rest);
    }

    /**
     * Optional heavier stress test for local runs:
     * Run with: -Dloadmetrics.stress=true
     */
    @Test
    @Timeout(25)
    @Tag("stress")
    @DisplayName("HEAVY: 1M+ concurrent operations (opt-in)")
    void heavyStress_1MOperations_optIn() throws Exception {
      Assumptions.assumeTrue(Boolean.getBoolean("loadmetrics.stress"), "stress test disabled");

      LoadMetrics m = newClosedMetrics();

      int threads = Math.max(4, CI_THREADS);
      int opsPerThread = 300_000; // 4 threads => 1.2M ops
      ExecutorService pool = Executors.newFixedThreadPool(threads);
      CountDownLatch start = new CountDownLatch(1);
      CountDownLatch done = new CountDownLatch(threads);

      for (int t = 0; t < threads; t++) {
        pool.submit(
                () -> {
                  try {
                    start.await();
                    for (int i = 0; i < opsPerThread; i++) {
                      if ((i & 127) == 0) {
                        m.recordFailure("HTTP_5xx", 9);
                      } else {
                        m.recordRequestSuccess(8);
                      }
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } finally {
                    done.countDown();
                  }
                });
      }

      start.countDown();
      assertTrue(done.await(20, TimeUnit.SECONDS), "Heavy stress workers did not finish in time");
      pool.shutdownNow();

      long expected = (long) threads * opsPerThread;
      assertEquals(expected, m.totalRequests());
      assertTrue(m.totalErrors() > 0);
      assertTrue(m.latencyP95Ms().isPresent());
      assertTrue(m.latencyP99Ms().isPresent());

      // Ensure report build still works under large history
      TaskRunReport report = m.buildReport();
      assertNotNull(report.metrics);
      assertEquals(expected, report.metrics.totalRequests);
    }
  }
}

