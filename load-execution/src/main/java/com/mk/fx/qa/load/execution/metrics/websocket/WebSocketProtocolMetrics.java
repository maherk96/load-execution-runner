package com.mk.fx.qa.load.execution.metrics.websocket;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import com.mk.fx.qa.load.execution.metrics.ProtocolMetricsProvider;
import com.mk.fx.qa.load.execution.metrics.Reservoir;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** Protocol metrics for WebSocket message operations. */
public class WebSocketProtocolMetrics implements ProtocolMetricsProvider {

  private final Map<String, MsgStats> messages = new ConcurrentHashMap<>();

  public void recordSuccess(String name, long latencyMs, Integer userId) {
    messages.computeIfAbsent(key(name), k -> new MsgStats(name)).onSuccess(latencyMs, userId);
  }

  public void recordTimeout(String name, long latencyMs) {
    messages.computeIfAbsent(key(name), k -> new MsgStats(name)).onFailure("TIMEOUT");
  }

  public void recordError(String name, String category) {
    messages.computeIfAbsent(key(name), k -> new MsgStats(name)).onFailure(category);
  }

  private String key(String name) {
    return name == null ? "message" : name;
  }

  @Override
  public void applyTo(TaskRunReport report) {
    if (messages.isEmpty()) return;

    TaskRunReport.ProtocolDetails pd =
        report.protocolDetails == null
            ? new TaskRunReport.ProtocolDetails()
            : report.protocolDetails;
    report.protocolDetails = pd;

    TaskRunReport.WebSocketDetails wd = new TaskRunReport.WebSocketDetails();
    List<TaskRunReport.WebSocketMessage> list = new ArrayList<>();
    for (MsgStats s : messages.values()) {
      TaskRunReport.WebSocketMessage m = new TaskRunReport.WebSocketMessage();
      m.name = s.name;
      m.total = s.total.get();
      m.success = s.success.get();
      m.failure = s.failure.get();
      TaskRunReport.Latency l = new TaskRunReport.Latency();
      l.min = s.minLatency.get() == Long.MAX_VALUE ? 0 : s.minLatency.get();
      l.max = s.maxLatency.get();
      l.avg = s.avgMs();
      l.p95 = s.p95Ms();
      l.p99 = s.p99Ms();
      m.latency = l;
      list.add(m);
    }
    wd.messages = List.copyOf(list);
    pd.websocket = wd;
    report.protocolDetails = pd;
  }

  private static final class MsgStats {
    final String name;
    final AtomicLong total = new AtomicLong();
    final AtomicLong success = new AtomicLong();
    final AtomicLong failure = new AtomicLong();
    final AtomicLong sumLatency = new AtomicLong();
    final AtomicLong minLatency = new AtomicLong(Long.MAX_VALUE);
    final AtomicLong maxLatency = new AtomicLong(0);
    final Reservoir reservoir = new Reservoir(100000);

    MsgStats(String name) {
      this.name = name;
    }

    void onSuccess(long latencyMs, Integer userId) {
      total.incrementAndGet();
      success.incrementAndGet();
      long v = Math.max(0, latencyMs);
      sumLatency.addAndGet(v);
      minLatency.accumulateAndGet(v, Math::min);
      maxLatency.accumulateAndGet(v, Math::max);
      reservoir.add(v);
    }

    void onFailure(String category) {
      total.incrementAndGet();
      failure.incrementAndGet();
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
  }
}
