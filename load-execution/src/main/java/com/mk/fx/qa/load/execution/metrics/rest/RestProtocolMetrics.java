package com.mk.fx.qa.load.execution.metrics.rest;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import com.mk.fx.qa.load.execution.metrics.ProtocolMetricsProvider;
import com.mk.fx.qa.load.execution.metrics.Reservoir;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class RestProtocolMetrics implements ProtocolMetricsProvider {

  private final Map<String, EndpointStats> endpointStats = new ConcurrentHashMap<>();

  public void recordSuccess(
          String method, String path, long latencyMs, int statusCode, Integer userId) {
    EndpointStats s =
            endpointStats.computeIfAbsent(key(method, path), k -> new EndpointStats(method, path));
    s.onSuccess(latencyMs, statusCode, userId);
  }

  public String recordHttpFailure(int statusCode, String method, String path, Integer userId) {
    EndpointStats s =
            endpointStats.computeIfAbsent(key(method, path), k -> new EndpointStats(method, path));
    String category = httpCategory(statusCode);
    s.onFailure(category);
    return category;
  }

  public void recordException(String method, String path, String category, Integer userId) {
    EndpointStats s =
            endpointStats.computeIfAbsent(key(method, path), k -> new EndpointStats(method, path));
    String cat = (category == null || category.isBlank()) ? "EXCEPTION" : category.toUpperCase();
    s.onFailure(cat);
  }

  private String key(String method, String path) {
    return (method == null ? "" : method) + " " + (path == null ? "" : path);
  }

  private String httpCategory(int statusCode) {
    if (statusCode >= 500) return "HTTP_5xx";
    if (statusCode >= 400) return "HTTP_4xx";
    if (statusCode >= 300) return "HTTP_3xx";
    return "HTTP_" + statusCode;
  }

  @Override
  public void applyTo(TaskRunReport report) {
    if (report == null || endpointStats.isEmpty()) return;

    // Ensure protocolDetails exists
    TaskRunReport.ProtocolDetails pd =
            report.protocolDetails != null ? report.protocolDetails : new TaskRunReport.ProtocolDetails();
    report.protocolDetails = pd;

    // Build REST details
    TaskRunReport.RestDetails rd = new TaskRunReport.RestDetails();
    List<TaskRunReport.RestEndpoint> list = new ArrayList<>(endpointStats.size());

    for (EndpointStats s : endpointStats.values()) {
      TaskRunReport.RestEndpoint es = new TaskRunReport.RestEndpoint();
      es.method = s.method;
      es.path = s.path;

      es.total = s.total.get();
      es.success = s.success.get();
      es.failure = s.failure.get();

      // Latency (only meaningful for successes)
      TaskRunReport.Latency l = new TaskRunReport.Latency();
      long min = s.minLatency.get();
      l.min = (min == Long.MAX_VALUE) ? 0L : min;
      l.max = s.maxLatency.get();
      l.avg = safeLong(s.avgMs());
      l.p95 = safeLong(s.p95Ms());
      l.p99 = safeLong(s.p99Ms());
      es.latency = l;

      es.statusBreakdown = s.statusBreakdownSnapshot();

      // Outlier detection: primitive boolean + renamed affected user field
      boolean hasP95 = s.p95Ms() != null && s.p95Ms() > 0;
      if (hasP95 && l.max > s.p95Ms() * 5) {
        es.outlierDetected = true;
        es.outlierInfo = "Detected slow requests: max " + l.max + "ms vs P95 " + s.p95Ms() + "ms";
        es.outlierTimestamp = s.maxLatencyAt;
        es.likelyAffectedUserId = s.maxLatencyUser;
      } else {
        es.outlierDetected = false;
        es.outlierInfo = null;
        es.outlierTimestamp = null;
        es.likelyAffectedUserId = null;
      }

      list.add(es);
    }

    rd.endpoints = List.copyOf(list);
    pd.rest = rd;
  }

  private static long safeLong(Long v) {
    return v == null ? 0L : v;
  }

  /* ====================================================================== */

  private static final class EndpointStats {
    final String method;
    final String path;

    final AtomicLong total = new AtomicLong();
    final AtomicLong success = new AtomicLong();
    final AtomicLong failure = new AtomicLong();

    final AtomicLong sumLatency = new AtomicLong();
    final AtomicLong minLatency = new AtomicLong(Long.MAX_VALUE);
    final AtomicLong maxLatency = new AtomicLong(0);

    volatile java.time.Instant maxLatencyAt;
    volatile Integer maxLatencyUser;

    final Reservoir reservoir = new Reservoir(100_000);
    final Map<String, AtomicLong> statusBreakdown = new ConcurrentHashMap<>();

    EndpointStats(String method, String path) {
      this.method = method;
      this.path = path;
    }

    void onSuccess(long latencyMs, int statusCode, Integer userId) {
      total.incrementAndGet();
      success.incrementAndGet();

      long v = Math.max(0, latencyMs);
      sumLatency.addAndGet(v);
      minLatency.accumulateAndGet(v, Math::min);

      long prev;
      do {
        prev = maxLatency.get();
        if (v <= prev) break;
      } while (!maxLatency.compareAndSet(prev, v));

      if (v > prev) {
        maxLatencyAt = java.time.Instant.now();
        maxLatencyUser = userId; // should already be 1-based if you want UI-aligned ids
      }

      reservoir.add(v);

      String codeGroup =
              statusCode >= 200 && statusCode < 600
                      ? (statusCode / 100) + "xx"
                      : String.valueOf(statusCode);

      statusBreakdown.computeIfAbsent(codeGroup, k -> new AtomicLong()).incrementAndGet();
    }

    void onFailure(String category) {
      total.incrementAndGet();
      failure.incrementAndGet();
      statusBreakdown.computeIfAbsent(category, k -> new AtomicLong()).incrementAndGet();
    }

    Long avgMs() {
      long s = success.get();
      return s == 0 ? null : sumLatency.get() / s;
    }

    Long p95Ms() {
      return reservoir.percentile(95).orElse(null);
    }

    Long p99Ms() {
      return reservoir.percentile(99).orElse(null);
    }

    Map<String, Long> statusBreakdownSnapshot() {
      Map<String, Long> map = new HashMap<>();
      for (var e : statusBreakdown.entrySet()) {
        map.put(e.getKey(), e.getValue().get());
      }
      return Map.copyOf(map);
    }
  } 
}