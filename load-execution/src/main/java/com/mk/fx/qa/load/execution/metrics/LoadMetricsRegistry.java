package com.mk.fx.qa.load.execution.metrics;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * Thread-safe registry for task metrics and reports.
 *
 * <p>Tracks currently active {@link LoadMetrics}, exposes snapshots for active or completed tasks,
 * and stores final {@link TaskRunReport} instances.
 */
@Component
public class LoadMetricsRegistry {

  private final Map<UUID, LoadMetrics> active = new ConcurrentHashMap<>();
  private final Map<UUID, LoadSnapshot> completed = new ConcurrentHashMap<>();
  private final Map<UUID, TaskRunReport> reports = new ConcurrentHashMap<>();

  /** Registers a metrics instance as active for the given task. */
  public void register(UUID taskId, LoadMetrics metrics) {
    active.put(taskId, metrics);
  }

  /** Removes an active metrics instance without marking completion. */
  public void unregister(UUID taskId) {
    active.remove(taskId);
  }

  /** Marks the task as completed with a final snapshot and removes it from active. */
  public void complete(UUID taskId, LoadSnapshot finalSnapshot) {
    completed.put(taskId, finalSnapshot);
    active.remove(taskId);
  }

  /** Returns a live snapshot for active tasks or the final one for completed tasks. */
  public Optional<LoadSnapshot> getSnapshot(UUID taskId) {
    LoadMetrics m = active.get(taskId);
    if (m != null) {
      return Optional.of(m.snapshotNow());
    }
    return Optional.ofNullable(completed.get(taskId));
  }

  /** Saves the final run report for the given task id. */
  public void saveReport(UUID taskId, TaskRunReport report) {
    reports.put(taskId, report);
  }

  /** Retrieves a previously saved run report, if any. */
  public Optional<TaskRunReport> getReport(UUID taskId) {
    return Optional.ofNullable(reports.get(taskId));
  }
}
