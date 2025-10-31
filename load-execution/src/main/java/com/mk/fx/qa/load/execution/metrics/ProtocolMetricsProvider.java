package com.mk.fx.qa.load.execution.metrics;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;

/**
 * Pluggable protocol-specific metrics provider. Implementations collect protocol data during a run
 * and contribute to the final TaskRunReport.
 */
public interface ProtocolMetricsProvider {
  void applyTo(TaskRunReport report);
}
