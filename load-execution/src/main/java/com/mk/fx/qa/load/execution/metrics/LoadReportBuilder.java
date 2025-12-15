package com.mk.fx.qa.load.execution.metrics;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import com.mk.fx.qa.load.execution.model.LoadModelType;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Builds a {@link TaskRunReport} by aggregating runtime metrics, configuration,
 * completion status, capacity analysis, and protocol-specific details.
 *
 * <p>This class performs no execution logic and does not mutate underlying trackers.</p>
 */
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

    Objects.requireNonNull(config, "config must not be null");
    Objects.requireNonNull(startedAt, "startedAt must not be null");
    Objects.requireNonNull(latency, "latency tracker must not be null");
    Objects.requireNonNull(users, "user tracker must not be null");
    Objects.requireNonNull(errors, "error tracker must not be null");

    final TaskRunReport report = new TaskRunReport();

    // ---------------- identity & timing ----------------
    report.taskId = config.taskId();
    report.taskType = config.taskType();
    report.model = config.model();

    report.startTime = startedAt;
    final Instant endTime = now();
    report.endTime = endTime;
    report.durationSec = Math.max(0.0, secondsBetween(startedAt, endTime));

    // ---------------- environment ----------------
    final TaskRunReport.Environment env = new TaskRunReport.Environment();
    env.host = EnvironmentInfo.host();
    env.triggeredBy = EnvironmentInfo.triggeredBy();
    report.environment = env;

    // ---------------- execution config (MODEL-AWARE) ----------------
    report.execution = buildExecutionConfig(config);

    // ---------------- model info ----------------
    report.modelInfo = buildModelInfo(config, report.execution);

    // ---------------- metrics ----------------
    report.metrics = buildMetrics(totalRequests, totalErrors, achievedRps, latency, users, errors);

    // ---------------- completion ----------------
    report.completion =
            computeCompletion(
                    config,
                    report.durationSec,
                    totalRequests,
                    report.metrics.usersCompleted,
                    report.execution.expectedTotalRequests,
                    completionInfo);

    // ---------------- capacity ----------------
    report.capacity = computeCapacity(config.model(), report.execution.expectedRps, achievedRps);

    // ---------------- time series ----------------
    report.timeSeries = buildTimeSeries(timeSeries, startedAt, report.execution.expectedRps);

    // ---------------- protocol decorations ----------------
    // Protocol providers decorate report.protocolDetails (and may add outlier flags etc.)
    if (protocolProviders != null) {
      for (ProtocolMetricsProvider provider : protocolProviders) {
        if (provider != null) provider.applyTo(report);
      }
    }

    // ---------------- user completion analysis ----------------
    report.userCompletionAnalysis = computeUserAnalysis(report.metrics.userCompletionHistogram);

    // ---------------- summary ----------------
    report.summary =
            computeSummary(
                    report.metrics,
                    report.completion,
                    report.capacity,
                    report.protocolDetails);

    // ---------------- executive summary ----------------
    report.executiveSummary = buildExecutiveSummary(report);

    return report;
  }

  /* ====================================================================== */
  /* Execution config                                                       */
  /* ====================================================================== */

  private TaskRunReport.ExecutionConfig buildExecutionConfig(TaskConfig config) {
    final TaskRunReport.ExecutionConfig exec = new TaskRunReport.ExecutionConfig();

    // expectations (shared)
    exec.expectedTotalRequests = config.expectedTotalRequests();
    exec.expectedRps = computeExpectedRps(config);

    if (config.model() == LoadModelType.CLOSED) {
      final TaskRunReport.ClosedConfig c = new TaskRunReport.ClosedConfig();
      c.users = safeMinOne(config.users());
      c.iterationsPerUser = safeMinOne(config.iterationsPerUser());
      c.requestsPerIteration = Math.max(1, config.requestsPerIteration());

      // IMPORTANT UX: zero durations are shown as null to avoid "PT0S" confusion
      c.warmup = nullIfZero(config.warmup());
      c.rampUp = nullIfZero(config.rampUp());
      c.holdFor = nullIfZero(config.holdFor());

      exec.closed = c;
      exec.open = null;
    } else {
      final TaskRunReport.OpenConfig o = new TaskRunReport.OpenConfig();
      o.arrivalRatePerSec = config.arrivalRatePerSec();
      o.duration = nullIfZero(config.duration());

      // If you have maxConcurrent in request somewhere else, populate it.
      // It's not in TaskConfig, so keep null unless you later add it.
      o.maxConcurrent = null;

      exec.open = o;
      exec.closed = null;
    }

    return exec;
  }

  private Double computeExpectedRps(TaskConfig config) {
    // Preserve your existing intent:
    // - use expectedRps if provided
    // - else derive for CLOSED if holdFor > 0
    // - else null
    Double expected = config.expectedRps();
    if (expected != null) return expected;

    if (config.model() == LoadModelType.CLOSED
            && config.holdFor() != null
            && !config.holdFor().isZero()) {

      double holdSec = Math.max(0.001, seconds(config.holdFor()));
      return config.expectedTotalRequests() / holdSec;
    }

    return null;
  }

  private TaskRunReport.ModelInfo buildModelInfo(TaskConfig config, TaskRunReport.ExecutionConfig exec) {
    final TaskRunReport.ModelInfo mi = new TaskRunReport.ModelInfo();

    if (config.model() == LoadModelType.CLOSED) {
      mi.description = "Closed model: fixed number of virtual users executing iterations.";
      mi.throughputExplanation =
              "Throughput is latency-bound: RPS increases with faster responses and/or more users.";
      mi.configNotes =
              phaseNote(exec.closed != null ? exec.closed.warmup : null,
                      exec.closed != null ? exec.closed.rampUp : null,
                      exec.closed != null ? exec.closed.holdFor : null);
    } else {
      mi.description = "Open model: arrivals launched at a target rate up to a max concurrency.";
      mi.throughputExplanation =
              "Throughput is rate-driven: aim to meet target arrival rate; saturation policy controls overflow.";
      mi.configNotes =
              (exec.open != null && exec.open.duration != null)
                      ? "Execution duration: " + exec.open.duration
                      : "Execution duration: not configured (runs until stopped).";
    }

    return mi;
  }

  private String phaseNote(Duration warmup, Duration rampUp, Duration holdFor) {
    boolean any = (warmup != null && !warmup.isZero())
            || (rampUp != null && !rampUp.isZero())
            || (holdFor != null && !holdFor.isZero());

    if (!any) return "No warmup/ramp-up/hold phases configured.";

    List<String> parts = new ArrayList<>();
    if (warmup != null && !warmup.isZero()) parts.add("warmup=" + warmup);
    if (rampUp != null && !rampUp.isZero()) parts.add("rampUp=" + rampUp);
    if (holdFor != null && !holdFor.isZero()) parts.add("holdFor=" + holdFor);
    return String.join(", ", parts);
  }

  /* ====================================================================== */
  /* Metrics                                                                */
  /* ====================================================================== */

  private TaskRunReport.Metrics buildMetrics(
          long totalRequests,
          long totalErrors,
          double achievedRps,
          LatencyTracker latency,
          UserTracker users,
          ErrorTracker errors) {

    final TaskRunReport.Metrics m = new TaskRunReport.Metrics();

    m.totalRequests = totalRequests;
    m.failureCount = totalErrors;
    m.successCount = Math.max(0, totalRequests - totalErrors);
    m.successRate = totalRequests == 0 ? 0.0 : (double) m.successCount / totalRequests;
    m.achievedRps = achievedRps;

    // latency: average must use sample count, not totalRequests
    final long samples = latency.sampleCount();
    final TaskRunReport.Latency lat = new TaskRunReport.Latency();
    lat.min = latency.minMs().orElse(0L);
    lat.max = latency.maxMs().orElse(0L);
    lat.avg = samples == 0 ? 0L : (latency.sumMs() / Math.max(1, samples));
    lat.p95 = latency.p95Ms().orElse(0L);
    lat.p99 = latency.p99Ms().orElse(0L);
    m.latency = lat;

    // errors: breakdown + samples
    Map<String, Long> breakdown = errors.breakdownSnapshot();
    if (breakdown == null || breakdown.isEmpty()) {
      m.errorBreakdown = List.of();
    } else {
      List<TaskRunReport.ErrorBreakdownItem> items = new ArrayList<>(breakdown.size());
      for (Map.Entry<String, Long> e : breakdown.entrySet()) {
        TaskRunReport.ErrorBreakdownItem item = new TaskRunReport.ErrorBreakdownItem();
        item.type = e.getKey();
        item.count = e.getValue();
        items.add(item);
      }
      m.errorBreakdown = List.copyOf(items);
    }

    // Map ErrorSample to new DTO shape if necessary
    // Assuming your tracker returns TaskRunReport.ErrorSample-compatible objects? If not, adapt here.
    List<TaskRunReport.ErrorSample> samplesList = new ArrayList<>();
    var trackerSamples = errors.samplesSnapshot();
    if (trackerSamples != null) {
      for (var s : trackerSamples) {
        TaskRunReport.ErrorSample es = new TaskRunReport.ErrorSample();
        es.type = s.type;
        es.message = s.message;
        es.stackTrace = s.stackTrace;
        samplesList.add(es);
      }
    }
    m.errorSamples = List.copyOf(samplesList);

    // users
    m.userCompletionHistogram = users.buildHistogram(); // must already be 1-based ids (recommended)
    m.usersStarted = users.totalUsersStarted();
    m.usersCompleted = users.totalUsersCompleted();

    return m;
  }

  /* ====================================================================== */
  /* Time series                                                            */
  /* ====================================================================== */

  private List<TaskRunReport.TimeSeriesSnapshot> buildTimeSeries(
          List<TimeSeriesPoint> timeSeries,
          Instant startedAt,
          Double expectedRps) {

    if (timeSeries == null || timeSeries.isEmpty()) return List.of();

    List<TaskRunReport.TimeSeriesSnapshot> snapshots = new ArrayList<>(timeSeries.size());

    Instant prev = startedAt;
    long totalSoFar = 0;

    for (TimeSeriesPoint p : timeSeries) {
      TaskRunReport.TimeSeriesSnapshot s = new TaskRunReport.TimeSeriesSnapshot();
      s.timestamp = p.timestamp();

      // Snapshot semantics: completed "so far"
      s.usersCompletedSoFar = p.usersCompleted();
      s.usersActive = Math.max(0, p.usersStarted() - p.usersCompleted());

      long windowReq = p.totalRequests();
      long windowErr = p.totalErrors();

      double secs = Math.max(0.001, secondsBetween(prev, p.timestamp()));
      s.rpsInWindow = windowReq / secs;
      s.expectedRpsInWindow = expectedRps;

      totalSoFar += windowReq;
      s.totalRequestsSoFar = totalSoFar;
      s.errorsInWindow = windowErr;

      TaskRunReport.LatencyWindow lw = new TaskRunReport.LatencyWindow();
      lw.min = p.latMinMs();
      lw.avg = p.latAvgMs();
      lw.max = p.latMaxMs();
      s.latency = lw;

      snapshots.add(s);
      prev = p.timestamp();
    }

    return List.copyOf(snapshots);
  }

  /* ====================================================================== */
  /* Completion                                                             */
  /* ====================================================================== */

  private TaskRunReport.TestCompletion computeCompletion(
          TaskConfig cfg,
          double actualDurationSec,
          long actualRequests,
          int usersCompleted,
          long expectedTotalRequests,
          CompletionInfo info) {

    TaskRunReport.TestCompletion tc = new TaskRunReport.TestCompletion();

    tc.expectedDurationSec = expectedDuration(cfg);
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
      tc.reason = TaskRunReport.CompletionReason.CANCELLED;
      tc.message = "Execution cancelled before completion.";
      return tc;
    }

    TaskRunReport.CompletionReason reason =
            determineCompletionReason(cfg, actualDurationSec, usersCompleted, info, tc.expectedDurationSec);

    tc.reason = reason;
    tc.message =
            switch (reason) {
              case ALL_USERS_FINISHED -> "All users completed iterations.";
              case HOLD_EXPIRED -> "Execution ran for the configured duration/hold.";
              case ERROR -> "Execution stopped before completion due to errors or early termination.";
              case CANCELLED -> "Execution cancelled.";
            };

    return tc;
  }

  private Double expectedDuration(TaskConfig cfg) {
    if (cfg.model() == LoadModelType.CLOSED) {
      return seconds(cfg.warmup()) + seconds(cfg.rampUp()) + seconds(cfg.holdFor());
    }
    if (cfg.model() == LoadModelType.OPEN && cfg.duration() != null) {
      return seconds(cfg.duration());
    }
    return null;
  }

  private TaskRunReport.CompletionReason determineCompletionReason(
          TaskConfig cfg,
          double actualDurationSec,
          int usersCompleted,
          CompletionInfo info,
          Double expectedDurationSec) {

    if (cfg.model() == LoadModelType.CLOSED) {
      boolean holdExpired = info != null && Boolean.TRUE.equals(info.holdExpired);
      Integer totalUsers = info != null ? info.totalUsers : cfg.users();

      if (holdExpired) return TaskRunReport.CompletionReason.HOLD_EXPIRED;
      if (totalUsers != null && usersCompleted >= totalUsers) return TaskRunReport.CompletionReason.ALL_USERS_FINISHED;
      if (totalUsers != null && usersCompleted < totalUsers) return TaskRunReport.CompletionReason.ERROR;

      // Fallback: duration-based
      if (expectedDurationSec != null && actualDurationSec + 1 < expectedDurationSec) {
        return TaskRunReport.CompletionReason.ERROR;
      }
      return TaskRunReport.CompletionReason.HOLD_EXPIRED;
    }

    // OPEN
    if (expectedDurationSec != null) {
      return actualDurationSec >= expectedDurationSec - 1.0
              ? TaskRunReport.CompletionReason.HOLD_EXPIRED
              : TaskRunReport.CompletionReason.ERROR;
    }
    return TaskRunReport.CompletionReason.HOLD_EXPIRED;
  }

  /* ====================================================================== */
  /* Capacity                                                               */
  /* ====================================================================== */

  private TaskRunReport.CapacityAnalysis computeCapacity(
          LoadModelType model,
          Double targetRps,
          double achievedRps) {

    TaskRunReport.CapacityAnalysis ca = new TaskRunReport.CapacityAnalysis();
    ca.targetRps = targetRps;
    ca.achievedRps = achievedRps;

    // Key UX fix: capacity utilization is primarily meaningful for OPEN runs.
    if (model == LoadModelType.CLOSED) {
      ca.utilizationPercent = null;
      ca.assessment = TaskRunReport.CapacityAssessment.NOT_APPLICABLE;
      ca.recommendation = "For CLOSED model, interpret achieved RPS as latency-bound throughput.";
      ca.note = "Capacity utilization is rate-target-based and applies primarily to OPEN model executions.";
      return ca;
    }

    if (targetRps == null || targetRps <= 0) {
      ca.utilizationPercent = null;
      ca.assessment = TaskRunReport.CapacityAssessment.NOT_APPLICABLE;
      ca.recommendation = "Define expectedRps to assess utilization against a target.";
      ca.note = "No target RPS was configured for this OPEN run.";
      return ca;
    }

    double util = (achievedRps / targetRps) * 100.0;
    ca.utilizationPercent = util;

    if (util < 80.0) {
      ca.assessment = TaskRunReport.CapacityAssessment.UNDER_UTILIZED;
      ca.recommendation = "Increase load (arrival rate / concurrency) to better utilize system capacity.";
    } else if (util <= 120.0) {
      ca.assessment = TaskRunReport.CapacityAssessment.OPTIMAL;
      ca.recommendation = "Maintain current load; system operating near target.";
    } else {
      ca.assessment = TaskRunReport.CapacityAssessment.OVER_UTILIZED;
      ca.recommendation = "System exceeded target throughput; consider raising targets or adding scenarios.";
    }
    ca.note = null;
    return ca;
  }

  /* ====================================================================== */
  /* User completion analysis                                                */
  /* ====================================================================== */

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

    uca.avgCompletionTimeMs = avg;
    uca.stdDevMs = std;

    double variancePct = avg > 0 ? (std / avg) * 100.0 : 0.0;
    uca.variance =
            variancePct < 20.0
                    ? TaskRunReport.VarianceAssessment.LOW
                    : (variancePct <= 50.0
                    ? TaskRunReport.VarianceAssessment.MODERATE
                    : TaskRunReport.VarianceAssessment.HIGH);

    uca.fastest = toSummary(fastest);
    uca.slowest = toSummary(slowest);

    uca.insight =
            switch (uca.variance) {
              case LOW -> "User completion times are consistent across the run.";
              case MODERATE -> "Moderate variance observed; investigate hotspots or uneven work distribution.";
              case HIGH -> "High variance in completion times; likely contention or endpoint variability.";
            };

    return uca;
  }

  private TaskRunReport.UserCompletionSummary toSummary(TaskRunReport.UserCompletion uc) {
    TaskRunReport.UserCompletionSummary s = new TaskRunReport.UserCompletionSummary();
    s.userId = uc.userId;
    s.completionTimeMs = uc.completionTimeMs;
    s.iterationsCompleted = uc.iterationsCompleted;
    return s;
  }

  /* ====================================================================== */
  /* Summary                                                                 */
  /* ====================================================================== */

  private TaskRunReport.Summary computeSummary(
          TaskRunReport.Metrics m,
          TaskRunReport.TestCompletion completion,
          TaskRunReport.CapacityAnalysis capacity,
          TaskRunReport.ProtocolDetails protocolDetails) {

    TaskRunReport.Summary s = new TaskRunReport.Summary();

    // Status
    if (m.successRate >= 1.0) {
      s.status = TaskRunReport.ExecutionStatus.SUCCESS;
      s.severity = TaskRunReport.Severity.INFO;
      s.message = "All requests succeeded with no failures.";
    } else if (m.successRate >= 0.95) {
      s.status = TaskRunReport.ExecutionStatus.PARTIAL_SUCCESS;
      s.severity = TaskRunReport.Severity.WARNING;
      s.message = "Minor failures observed; overall run largely successful.";
    } else {
      s.status = TaskRunReport.ExecutionStatus.FAILED;
      s.severity = TaskRunReport.Severity.CRITICAL;
      s.message = "Failures observed; review errors and outliers.";
    }

    s.highlights = new ArrayList<>();
    s.concerns = new ArrayList<>();

    s.highlights.add(String.format("successRate=%.2f%%", m.successRate * 100));
    s.highlights.add("latency.avg=" + m.latency.avg + "ms");
    s.highlights.add("latency.p95=" + m.latency.p95 + "ms");

    if (completion != null) {
      s.highlights.add("completion=" + completion.reason);
    }

    if (capacity != null && capacity.targetRps != null) {
      s.highlights.add(
              String.format("achievedRps=%.2f vs target=%.2f", m.achievedRps, capacity.targetRps));
    } else {
      s.highlights.add(String.format("achievedRps=%.2f", m.achievedRps));
    }

    // Concerns
    if (m.failureCount > 0) {
      s.concerns.add("failures=" + m.failureCount);
    }

    if (m.usersStarted > 0 && m.usersCompleted < m.usersStarted) {
      s.concerns.add("incompleteUsers=" + (m.usersStarted - m.usersCompleted));
      if (s.severity == TaskRunReport.Severity.INFO) {
        s.severity = TaskRunReport.Severity.WARNING;
      }
    }

    boolean outliers =
            protocolDetails != null
                    && protocolDetails.rest != null
                    && protocolDetails.rest.endpoints != null
                    && protocolDetails.rest.endpoints.stream().anyMatch(e -> e != null && e.outlierDetected);

    if (outliers) {
      s.concerns.add("latencyOutliersDetected");
      if (s.severity == TaskRunReport.Severity.INFO) {
        s.severity = TaskRunReport.Severity.WARNING;
      }
    }

    if (capacity != null
            && capacity.utilizationPercent != null
            && capacity.utilizationPercent < 50.0
            && capacity.assessment != TaskRunReport.CapacityAssessment.NOT_APPLICABLE) {
      s.concerns.add("lowUtilization=" + String.format("%.1f%%", capacity.utilizationPercent));
    }

    return s;
  }

  /* ====================================================================== */
  /* Executive summary                                                       */
  /* ====================================================================== */

  private String buildExecutiveSummary(TaskRunReport report) {
    if (report == null || report.metrics == null) return null;

    TaskRunReport.Metrics m = report.metrics;

    String base =
            String.format(
                    "%d/%d users completed; %d requests executed in %.2fs; successRate=%.2f%%; avg=%dms p95=%dms.",
                    m.usersCompleted,
                    m.usersStarted,
                    m.totalRequests,
                    report.durationSec,
                    m.successRate * 100.0,
                    m.latency.avg,
                    m.latency.p95);

    boolean outliers =
            report.protocolDetails != null
                    && report.protocolDetails.rest != null
                    && report.protocolDetails.rest.endpoints != null
                    && report.protocolDetails.rest.endpoints.stream().anyMatch(e -> e != null && e.outlierDetected);

    if (outliers) {
      return base + " Latency outliers were detected on at least one endpoint.";
    }
    return base;
  }

  /* ====================================================================== */
  /* Helpers                                                                 */
  /* ====================================================================== */

  private static Duration nullIfZero(Duration d) {
    if (d == null || d.isZero()) return null;
    return d;
  }

  private static int safeMinOne(Integer v) {
    if (v == null) return 1;
    return Math.max(1, v);
  }

  private static double seconds(Duration d) {
    return d == null ? 0.0 : d.toMillis() / 1000.0;
  }

  private static double secondsBetween(Instant a, Instant b) {
    return Duration.between(a, b).toMillis() / 1000.0;
  }

  protected Instant now() {
    return Instant.now();
  }
}