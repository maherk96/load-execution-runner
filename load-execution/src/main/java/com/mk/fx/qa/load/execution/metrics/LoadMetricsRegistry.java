package com.mk.fx.qa.load.execution.metrics;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

@Component
public class LoadMetricsRegistry {

  private final Map<UUID, LoadMetrics> active = new ConcurrentHashMap<>();
  private final Map<UUID, LoadSnapshot> completed = new ConcurrentHashMap<>();
  private final Map<UUID, TaskRunReport> reports = new ConcurrentHashMap<>();

  public void register(UUID taskId, LoadMetrics metrics) {
    active.put(taskId, metrics);
  }

  public void unregister(UUID taskId) {
    active.remove(taskId);
  }

  public void complete(UUID taskId, LoadSnapshot finalSnapshot) {
    completed.put(taskId, finalSnapshot);
    active.remove(taskId);
  }

  public Optional<LoadSnapshot> getSnapshot(UUID taskId) {
    LoadMetrics m = active.get(taskId);
    if (m != null) {
      return Optional.of(m.snapshotNow());
    }
    return Optional.ofNullable(completed.get(taskId));
  }

  public void saveReport(UUID taskId, TaskRunReport report) {
    reports.put(taskId, report);
  }

  public Optional<TaskRunReport> getReport(UUID taskId) {
    return Optional.ofNullable(reports.get(taskId));
  }
}
