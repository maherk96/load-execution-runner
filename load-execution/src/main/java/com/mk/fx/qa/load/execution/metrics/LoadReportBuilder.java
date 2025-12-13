package com.mk.fx.qa.load.execution.metrics;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import com.mk.fx.qa.load.execution.model.LoadModelType;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class LoadReportBuilder {

  TaskRunReport build(
          TaskConfig config,
          Instant startedAt,
          long totalRequests,
          long totalErrors,
          double achievedRps,
          LatencyTracker latency,
          UserTracker users,
          ErrorTracker errors,
          List<TimeSeriesPoint> timeSeries,
          List<ProtocolMetricsProvider> protocolProviders,
          CompletionInfo completionInfo) {

    final TaskRunReport r = new TaskRunReport();

    // identifiers & timing
    r.taskId = config.taskId();
    r.taskType = config.taskType();
    r.model = LoadModelType.valueOf(config.model().name());
    r.startTime = startedAt;

    final Instant end = Instant.now();
    r.endTime = end;
    r.durationSec = Math.max(0.0, Duration.between(startedAt, end).toMillis() / 1000.0);

    // environment
    TaskRunReport.EnvInfo env = new TaskRunReport.EnvInfo();
    env.host = EnvironmentInfo.host();
    env.triggeredBy = EnvironmentInfo.triggeredBy();
    r.environment = env;

    // config
    final TaskRunReport.Config cfg = buildConfig(config);
    r.config = cfg;

    // metrics aggregate
    final TaskRunReport.Metrics m =
            buildMetrics(totalRequests, totalErrors, achievedRps, latency, users, errors, cfg);
    r.metrics = m;

    // completion status
    r.testCompletion =
            computeCompletion(
                    config,
                    r.durationSec,
                    totalRequests,
                    m.usersCompleted,
                    cfg.expectedTotalRequests,
                    completionInfo);

    // capacity analysis
    r.capacityAnalysis = computeCapacity(cfg.expectedRps, achievedRps);

    // timeseries
    r.timeSeriesEntries = buildTimeSeriesEntries(timeSeries, startedAt, cfg.expectedRps);

    // apply protocol providers last (they decorate report)
    if (protocolProviders != null && !protocolProviders.isEmpty()) {
      for (ProtocolMetricsProvider provider : protocolProviders) {
        if (provider != null) {
          provider.applyTo(r);
        }
      }
    }

    // user completion analysis
    r.userCompletionAnalysis = computeUserAnalysis(m.userCompletionHistogram);

    // summary and visualization hints (depends on protocol details possibly added above)
    r.summary = computeSummary(m, r.testCompletion, r.capacityAnalysis, r.protocolDetails);

    return r;
  }

  private TaskRunReport.Config buildConfig(TaskConfig config) {
    TaskRunReport.Config cfg = new TaskRunReport.Config();
    cfg.users = config.users();
    cfg.iterationsPerUser = config.iterationsPerUser();
    cfg.requestsPerIteration = config.requestsPerIteration();
    cfg.warmup = config.warmup();
    cfg.rampUp = config.rampUp();
    cfg.holdFor = config.holdFor();
    cfg.arrivalRatePerSec = config.arrivalRatePerSec();
    cfg.openDuration = config.duration();
    cfg.expectedTotalRequests = config.expectedTotalRequests();

    Double expectedRps = config.expectedRps();
    if (expectedRps == null
            && config.model() == LoadModelType.CLOSED
            && config.holdFor() != null
            && !config.holdFor().isZero()) {
      double holdSec = Math.max(0.001, config.holdFor().toMillis() / 1000.0);
      expectedRps = cfg.expectedTotalRequests / holdSec;
    }
    cfg.expectedRps = expectedRps;
    return cfg;
  }

  private TaskRunReport.Metrics buildMetrics(
          long totalRequests,
          long totalErrors,
          double achievedRps,
          LatencyTracker latency,
          UserTracker users,
          ErrorTracker errors,
          TaskRunReport.Config cfg) {

    TaskRunReport.Metrics m = new TaskRunReport.Metrics();
    m.totalRequests = totalRequests;
    m.failureCount = totalErrors;
    m.successCount = Math.max(0, m.totalRequests - m.failureCount);
    m.successRate = m.totalRequests == 0 ? 0.0 : (double) m.successCount / m.totalRequests;
    m.achievedRps = achievedRps;

    // IMPORTANT: average latency must use latency sample count (not totalRequests),
    // because some failures are recorded without latency samples.
    long latencySamples = latency.count(); // <-- requires LatencyTracker.count()
    TaskRunReport.Latency lat = new TaskRunReport.Latency();
    lat.avg = latencySamples == 0 ? 0L : (latency.sumMs() / Math.max(1, latencySamples));
    lat.min = latency.minMs().orElse(0L);
    lat.max = latency.maxMs().orElse(0L);
    lat.p95 = latency.p95Ms().orElse(0L);
    lat.p99 = latency.p99Ms().orElse(0L);
    m.latency = lat;

    // error breakdown array
    Map<String, Long> breakdown = errors.breakdownSnapshot();
    if (breakdown.isEmpty()) {
      m.errorBreakdown = List.of();
    } else {
      List<TaskRunReport.ErrorItem> errs = new ArrayList<>(breakdown.size());
      for (Map.Entry<String, Long> e : breakdown.entrySet()) {
        TaskRunReport.ErrorItem item = new TaskRunReport.ErrorItem();
        item.type = e.getKey();
        item.count = e.getValue();
        errs.add(item);
      }
      m.errorBreakdown = List.copyOf(errs);
    }

    // error samples
    m.errorSamples = errors.samplesSnapshot();

    // user histogram
    m.userCompletionHistogram = users.buildHistogram();
    m.usersStarted = users.totalUsersStarted();
    m.usersCompleted = users.totalUsersCompleted();
    m.expectedRps = cfg.expectedRps;

    return m;
  }

  private List<TaskRunReport.TimeSeriesEntry> buildTimeSeriesEntries(
          List<TimeSeriesPoint> timeSeries, Instant startedAt, Double expectedRps) {

    if (timeSeries == null || timeSeries.isEmpty()) {
      return List.of();
    }

    List<TaskRunReport.TimeSeriesEntry> windows = new ArrayList<>(timeSeries.size());

    long prevReq = 0;
    long prevErr = 0;
    Instant prevTs = startedAt;

    for (TimeSeriesPoint p : timeSeries) {
      TaskRunReport.TimeSeriesEntry w = new TaskRunReport.TimeSeriesEntry();
      w.timestamp = p.timestamp();

      w.usersCompleted = p.usersCompleted();
      w.usersActive = Math.max(0, p.usersStarted() - p.usersCompleted());

      long deltaReq = p.totalRequests() - prevReq;
      long deltaErr = p.totalErrors() - prevErr;

      // defensive: never allow negative deltas
      if (deltaReq < 0) deltaReq = 0;
      if (deltaErr < 0) deltaErr = 0;

      double secs = Math.max(0.001, Duration.between(prevTs, p.timestamp()).toMillis() / 1000.0);
      w.rpsInWindow = deltaReq / secs;
      w.expectedRpsInWindow = expectedRps;
      w.totalRequestsSoFar = p.totalRequests();
      w.errorsInWindow = deltaErr;

      TaskRunReport.LatencyWindow lw = new TaskRunReport.LatencyWindow();
      lw.min = p.latMinMs();
      lw.avg = p.latAvgMs();
      lw.max = p.latMaxMs();
      w.latency = lw;

      windows.add(w);

      prevReq = p.totalRequests();
      prevErr = p.totalErrors();
      prevTs = p.timestamp();
    }

    return List.copyOf(windows);
  }

  private TaskRunReport.TestCompletion computeCompletion(
          TaskConfig cfg,
          double actualDurationSec,
          long actualRequests,
          int usersCompleted,
          long expectedTotalRequests,
          CompletionInfo info) {

    TaskRunReport.TestCompletion tc = new TaskRunReport.TestCompletion();

    Double expectedDurationSec = null;
    if (cfg.model() == LoadModelType.CLOSED) {
      double warm = cfg.warmup() != null ? cfg.warmup().toMillis() / 1000.0 : 0.0;
      double ramp = cfg.rampUp() != null ? cfg.rampUp().toMillis() / 1000.0 : 0.0;
      double hold = cfg.holdFor() != null ? cfg.holdFor().toMillis() / 1000.0 : 0.0;
      expectedDurationSec = warm + ramp + hold;
    } else if (cfg.model() == LoadModelType.OPEN) {
      expectedDurationSec = cfg.duration() != null ? cfg.duration().toMillis() / 1000.0 : null;
    }

    tc.expectedDurationSec = expectedDurationSec;
    tc.actualDurationSec = actualDurationSec;
    tc.percentComplete =
            (int)
                    Math.max(
                            0,
                            Math.min(
                                    100,
                                    Math.round((actualRequests * 100.0) / Math.max(1, expectedTotalRequests))));

    // Precedence: CANCELLED always wins
    if (info != null && info.cancelled) {
      tc.reason = "CANCELLED";
      tc.message = "Execution cancelled before completion.";
      return tc;
    }

    String reason;

    if (cfg.model() == LoadModelType.CLOSED) {
      boolean holdExpired = info != null && Boolean.TRUE.equals(info.holdExpired);
      Integer totalUsers = info != null ? info.totalUsers : cfg.users();

      if (holdExpired) {
        reason = "HOLD_EXPIRED";
      } else if (totalUsers != null && usersCompleted >= totalUsers) {
        reason = "ALL_USERS_FINISHED";
      } else if (totalUsers != null && usersCompleted < totalUsers) {
        reason = "ERROR";
      } else {
        // fallback: duration-based
        if (expectedDurationSec != null && actualDurationSec + 1 < expectedDurationSec) {
          reason = "ERROR";
        } else {
          reason = "HOLD_EXPIRED";
        }
      }
    } else { // OPEN
      if (expectedDurationSec != null) {
        if (actualDurationSec >= expectedDurationSec - 1.0) {
          reason = "HOLD_EXPIRED";
        } else {
          reason = "ERROR";
        }
      } else {
        reason = "HOLD_EXPIRED";
      }
    }

    tc.reason = reason;
    tc.message =
            switch (reason) {
              case "ALL_USERS_FINISHED" -> "All users completed iterations before hold expired.";
              case "HOLD_EXPIRED" -> "Execution ran for the configured hold/duration.";
              case "ERROR" -> "Execution stopped before completion due to errors or early termination.";
              default -> "Execution finished.";
            };

    return tc;
  }

  private TaskRunReport.CapacityAnalysis computeCapacity(Double targetRps, double achievedRps) {
    TaskRunReport.CapacityAnalysis ca = new TaskRunReport.CapacityAnalysis();
    ca.targetRps = targetRps;
    ca.achievedRps = achievedRps;

    if (targetRps == null || targetRps <= 0) {
      ca.utilizationPercent = null;
      ca.assessment = "UNKNOWN";
      ca.recommendation = "Define a target RPS to assess utilization.";
      return ca;
    }

    double util = (achievedRps / targetRps) * 100.0;
    ca.utilizationPercent = util;

    if (util < 80.0) {
      ca.assessment = "UNDER_UTILIZED";
      ca.recommendation = "Increase load to better utilize system capacity.";
    } else if (util <= 120.0) {
      ca.assessment = "OPTIMAL";
      ca.recommendation = "Maintain current load; system operating near target.";
    } else {
      ca.assessment = "OVER_UTILIZED";
      ca.recommendation = "System has headroom; consider raising targets or adding scenarios.";
    }
    return ca;
  }

  private TaskRunReport.UserCompletionAnalysis computeUserAnalysis(
          List<TaskRunReport.UserCompletion> histogram) {

    if (histogram == null || histogram.isEmpty()) return null;

    TaskRunReport.UserCompletionAnalysis uca = new TaskRunReport.UserCompletionAnalysis();

    TaskRunReport.UserCompletion fastest = null, slowest = null;
    long sum = 0;

    for (TaskRunReport.UserCompletion uc : histogram) {
      sum += uc.completionTimeMs;
      if (fastest == null || uc.completionTimeMs < fastest.completionTimeMs) fastest = uc;
      if (slowest == null || uc.completionTimeMs > slowest.completionTimeMs) slowest = uc;
    }

    double avg = sum / (double) histogram.size();
    double varianceSum = 0.0;
    for (TaskRunReport.UserCompletion uc : histogram) {
      double d = uc.completionTimeMs - avg;
      varianceSum += d * d;
    }
    double std = Math.sqrt(varianceSum / histogram.size());
    double variancePct = avg > 0 ? (std / avg) * 100.0 : 0.0;

    String assessment = variancePct < 20.0 ? "LOW" : (variancePct <= 50.0 ? "MODERATE" : "HIGH");

    TaskRunReport.UserCompletionSummary f = new TaskRunReport.UserCompletionSummary();
    f.userId = fastest.userId;
    f.completionTimeMs = fastest.completionTimeMs;
    f.iterationsCompleted = fastest.iterationsCompleted;

    TaskRunReport.UserCompletionSummary s = new TaskRunReport.UserCompletionSummary();
    s.userId = slowest.userId;
    s.completionTimeMs = slowest.completionTimeMs;
    s.iterationsCompleted = slowest.iterationsCompleted;

    uca.fastest = f;
    uca.slowest = s;
    uca.avgCompletionTimeMs = avg;
    uca.stdDevMs = std;
    uca.varianceAssessment = assessment;
    uca.insight =
            switch (assessment) {
              case "LOW" -> "User completion times are consistent across the run.";
              case "MODERATE" ->
                      "Moderate variance observed; investigate hotspots or uneven work distribution.";
              default ->
                      "High variance in completion times; likely contention or endpoint variability.";
            };

    return uca;
  }

  private TaskRunReport.Summary computeSummary(
          TaskRunReport.Metrics m,
          TaskRunReport.TestCompletion tc,
          TaskRunReport.CapacityAnalysis ca,
          TaskRunReport.ProtocolDetails pd) {

    TaskRunReport.Summary s = new TaskRunReport.Summary();

    String status =
            m.successRate >= 1.0 ? "SUCCESS" : (m.successRate >= 0.95 ? "PARTIAL_SUCCESS" : "FAILED");
    s.status = status;
    s.message =
            switch (status) {
              case "SUCCESS" -> "All requests succeeded with no failures.";
              case "PARTIAL_SUCCESS" -> "Minor failures observed; overall run largely successful.";
              default -> "Failures observed; review errors and outliers.";
            };

    s.highlights = new ArrayList<>();
    s.concerns = new ArrayList<>();

    s.highlights.add(String.format("successRate=%.2f%%", m.successRate * 100));
    if (m.latency != null) {
      s.highlights.add("latency.avg=" + (m.latency.avg != null ? m.latency.avg : 0) + "ms");
      s.highlights.add("latency.p95=" + (m.latency.p95 != null ? m.latency.p95 : 0) + "ms");
    }
    if (ca != null && ca.targetRps != null) {
      s.highlights.add(
              String.format("achievedRps=%.2f vs target=%.2f", m.achievedRps, ca.targetRps));
    }
    if (tc != null && tc.reason != null) {
      s.highlights.add("completion=" + tc.reason);
    }

    if (m.failureCount > 0) s.concerns.add("failures=" + m.failureCount);
    if (m.usersStarted > 0 && m.usersCompleted < m.usersStarted) {
      s.concerns.add("incompleteUsers=" + (m.usersStarted - m.usersCompleted));
    }
    if (pd != null && pd.rest != null && pd.rest.endpoints != null) {
      boolean anyOutlier =
              pd.rest.endpoints.stream().anyMatch(e -> Boolean.TRUE.equals(e.outlierDetected));
      if (anyOutlier) s.concerns.add("latencyOutliersDetected");
    }
    if (ca != null && ca.utilizationPercent != null && ca.utilizationPercent < 50.0) {
      s.concerns.add("lowUtilization=" + String.format("%.1f%%", ca.utilizationPercent));
    }

    return s;
  }
}