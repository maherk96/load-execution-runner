package com.mk.fx.qa.load.execution.metrics.rest;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import com.mk.fx.qa.load.execution.metrics.ProtocolMetricsProvider;
import com.mk.fx.qa.load.execution.metrics.Reservoir;
import java.util.*;
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
    if (endpointStats.isEmpty()) {
      return;
    }

    TaskRunReport.ProtocolDetails pd =
        report.protocolDetails == null
            ? new TaskRunReport.ProtocolDetails()
            : report.protocolDetails;
    report.protocolDetails = pd;

    TaskRunReport.RestDetails rd = new TaskRunReport.RestDetails();
    List<TaskRunReport.RestEndpoint> list = new ArrayList<>();

    for (EndpointStats s : endpointStats.values()) {
      TaskRunReport.RestEndpoint es = new TaskRunReport.RestEndpoint();
      es.method = s.method;
      es.path = s.path;
      es.total = s.total.get();
      es.success = s.success.get();
      es.failure = s.failure.get();

      TaskRunReport.Latency l = new TaskRunReport.Latency();
      l.min = s.minLatency.get() == Long.MAX_VALUE ? null : s.minLatency.get();
      l.max = s.maxLatency.get();
      l.avg = s.avgMs();
      l.p99 = s.p99Ms();
      l.p95 = s.p95Ms();
      es.latency = l;
      es.statusBreakdown = s.statusBreakdownSnapshot();

      if (l.p95 != null && l.max > l.p95 * 5) {
        es.outlierDetected = true;
        es.outlierInfo = "Detected slow requests: max " + l.max + "ms vs P95 " + l.p95 + "ms";
        es.outlierTimestamp = s.maxLatencyAt;
        es.likelyAffectedUser = s.maxLatencyUser;
      } else {
        es.outlierDetected = false;
      }
      list.add(es);
    }

    rd.endpoints = List.copyOf(list);
    pd.rest = rd;
    report.protocolDetails = pd;
  }

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
    final Reservoir reservoir = new Reservoir(100000);
    final Map<String, AtomicLong> statusBreakdown = new ConcurrentHashMap<>();

    public EndpointStats(String method, String path) {
      this.method = method;
      this.path = path;
    }

    private void onSuccess(long latencyMs, int statusCode, Integer userId) {
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
        maxLatencyUser = userId;
      }
      reservoir.add(v);
      String codeGroup =
          statusCode >= 200 && statusCode < 600
              ? (statusCode / 100) + "xx"
              : String.valueOf(statusCode);
      statusBreakdown.computeIfAbsent(codeGroup, k -> new AtomicLong()).incrementAndGet();
    }

    private void onFailure(String category) {
      total.incrementAndGet();
      failure.incrementAndGet();
      statusBreakdown.computeIfAbsent(category, k -> new AtomicLong()).incrementAndGet();
    }

    private Long avgMs() {
      long s = success.get();
      return s == 0 ? null : sumLatency.get() / s;
    }

    private Long p95Ms() {
      return reservoir.percentile(95).orElse(null);
    }

    private Long p99Ms() {
      return reservoir.percentile(99).orElse(null);
    }

    private Map<String, Long> statusBreakdownSnapshot() {
      Map<String, Long> map = new HashMap<>();
      for (var e : statusBreakdown.entrySet()) map.put(e.getKey(), e.getValue().get());
      return Map.copyOf(map);
    }
  }
}
