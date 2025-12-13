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

  /* ----------------------------- constants ----------------------------- */

  private static final String STATUS_SUCCESS = "SUCCESS";
  private static final String STATUS_PARTIAL_SUCCESS = "PARTIAL_SUCCESS";
  private static final String STATUS_FAILED = "FAILED";

  private static final String COMPLETION_CANCELLED = "CANCELLED";
  private static final String COMPLETION_HOLD_EXPIRED = "HOLD_EXPIRED";
  private static final String COMPLETION_ALL_USERS_FINISHED = "ALL_USERS_FINISHED";
  private static final String COMPLETION_ERROR = "ERROR";

  private static final String VARIANCE_LOW = "LOW";
  private static final String VARIANCE_MODERATE = "MODERATE";
  private static final String VARIANCE_HIGH = "HIGH";

  /* ----------------------------- public API ----------------------------- */

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

    /* identifiers & timing */
    report.taskId = config.taskId();
    report.taskType = config.taskType();
    report.model = LoadModelType.valueOf(config.model().name());
    report.startTime = startedAt;

    final Instant endTime = now();
    report.endTime = endTime;
    report.durationSec = Math.max(0.0, secondsBetween(startedAt, endTime));

    /* environment */
    final TaskRunReport.EnvInfo env = new TaskRunReport.EnvInfo();
    env.host = EnvironmentInfo.host();
    env.triggeredBy = EnvironmentInfo.triggeredBy();
    report.environment = env;

    /* config */
    final TaskRunReport.Config cfg = buildConfig(config);
    report.config = cfg;

    /* metrics */
    final TaskRunReport.Metrics metrics =
            buildMetrics(totalRequests, totalErrors, achievedRps, latency, users, errors, cfg);
    report.metrics = metrics;

    /* completion */
    report.testCompletion =
            computeCompletion(
                    config,
                    report.durationSec,
                    totalRequests,
                    metrics.usersCompleted,
                    cfg.expectedTotalRequests,
                    completionInfo);

    /* capacity */
    report.capacityAnalysis = computeCapacity(cfg.expectedRps, achievedRps);

    /* time series */
    report.timeSeriesEntries =
            buildTimeSeriesEntries(timeSeries, startedAt, cfg.expectedRps);

    /* protocol decorations */
    if (protocolProviders != null) {
      for (ProtocolMetricsProvider provider : protocolProviders) {
        if (provider != null) {
          provider.applyTo(report);
        }
      }
    }

    /* user completion */
    report.userCompletionAnalysis =
            computeUserAnalysis(metrics.userCompletionHistogram);

    /* summary */
    report.summary =
            computeSummary(
                    metrics,
                    report.testCompletion,
                    report.capacityAnalysis,
                    report.protocolDetails);

    return report;
  }

  /* ----------------------------- config ----------------------------- */

  private TaskRunReport.Config buildConfig(TaskConfig config) {
    final TaskRunReport.Config cfg = new TaskRunReport.Config();

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

      final double holdSec =
              Math.max(0.001, seconds(config.holdFor()));
      expectedRps = cfg.expectedTotalRequests / holdSec;
    }

    cfg.expectedRps = expectedRps;
    return cfg;
  }

  /* ----------------------------- metrics ----------------------------- */

  private TaskRunReport.Metrics buildMetrics(
          long totalRequests,
          long totalErrors,
          double achievedRps,
          LatencyTracker latency,
          UserTracker users,
          ErrorTracker errors,
          TaskRunReport.Config cfg) {

    final TaskRunReport.Metrics metrics = new TaskRunReport.Metrics();

    metrics.totalRequests = totalRequests;
    metrics.failureCount = totalErrors;
    metrics.successCount = Math.max(0, totalRequests - totalErrors);
    metrics.successRate =
            totalRequests == 0 ? 0.0 : (double) metrics.successCount / totalRequests;
    metrics.achievedRps = achievedRps;

    /* latency */
    final long latencySamples = latency.sampleCount();
    final long safeSamples = Math.max(1, latencySamples);

    final TaskRunReport.Latency lat = new TaskRunReport.Latency();
    lat.avg = latencySamples == 0 ? 0L : latency.sumMs() / safeSamples;
    lat.min = latency.minMs().orElse(0L);
    lat.max = latency.maxMs().orElse(0L);
    lat.p95 = latency.p95Ms().orElse(0L);
    lat.p99 = latency.p99Ms().orElse(0L);
    metrics.latency = lat;

    /* errors */
    final Map<String, Long> breakdown = errors.breakdownSnapshot();
    if (breakdown.isEmpty()) {
      metrics.errorBreakdown = List.of();
    } else {
      final List<TaskRunReport.ErrorItem> items =
              new ArrayList<>(breakdown.size());
      for (Map.Entry<String, Long> e : breakdown.entrySet()) {
        final TaskRunReport.ErrorItem item = new TaskRunReport.ErrorItem();
        item.type = e.getKey();
        item.count = e.getValue();
        items.add(item);
      }
      metrics.errorBreakdown = List.copyOf(items);
    }

    metrics.errorSamples = errors.samplesSnapshot();

    /* users */
    metrics.userCompletionHistogram = users.buildHistogram();
    metrics.usersStarted = users.totalUsersStarted();
    metrics.usersCompleted = users.totalUsersCompleted();
    metrics.expectedRps = cfg.expectedRps;

    return metrics;
  }

  /* ----------------------------- time series ----------------------------- */

  private List<TaskRunReport.TimeSeriesEntry> buildTimeSeriesEntries(
          List<TimeSeriesPoint> timeSeries,
          Instant startedAt,
          Double expectedRps) {

    if (timeSeries == null || timeSeries.isEmpty()) {
      return List.of();
    }

    final List<TaskRunReport.TimeSeriesEntry> entries =
            new ArrayList<>(timeSeries.size());

    Instant prevTs = startedAt;
    long totalSoFar = 0;

    for (TimeSeriesPoint point : timeSeries) {
      final TaskRunReport.TimeSeriesEntry entry =
              new TaskRunReport.TimeSeriesEntry();

      entry.timestamp = point.timestamp();
      entry.usersCompleted = point.usersCompleted();
      entry.usersActive =
              Math.max(0, point.usersStarted() - point.usersCompleted());

      final long windowReq = point.totalRequests();
      final long windowErr = point.totalErrors();

      final double secs =
              Math.max(0.001, secondsBetween(prevTs, point.timestamp()));

      entry.rpsInWindow = windowReq / secs;
      entry.expectedRpsInWindow = expectedRps;

      totalSoFar += windowReq;
      entry.totalRequestsSoFar = totalSoFar;
      entry.errorsInWindow = windowErr;

      final TaskRunReport.LatencyWindow lw =
              new TaskRunReport.LatencyWindow();
      lw.min = point.latMinMs();
      lw.avg = point.latAvgMs();
      lw.max = point.latMaxMs();
      entry.latency = lw;

      entries.add(entry);
      prevTs = point.timestamp();
    }

    return List.copyOf(entries);
  }

  /* ----------------------------- completion ----------------------------- */

  private TaskRunReport.TestCompletion computeCompletion(
          TaskConfig cfg,
          double actualDurationSec,
          long actualRequests,
          int usersCompleted,
          long expectedTotalRequests,
          CompletionInfo info) {

    final TaskRunReport.TestCompletion tc =
            new TaskRunReport.TestCompletion();

    tc.expectedDurationSec = expectedDuration(cfg);
    tc.actualDurationSec = actualDurationSec;
    tc.percentComplete =
            (int)
                    Math.max(
                            0,
                            Math.min(
                                    100,
                                    Math.round(
                                            (actualRequests * 100.0)
                                                    / Math.max(1, expectedTotalRequests))));

    if (info != null && info.cancelled) {
      tc.reason = COMPLETION_CANCELLED;
      tc.message = "Execution cancelled before completion.";
      return tc;
    }

    final String reason = determineCompletionReason(
            cfg, actualDurationSec, usersCompleted, info, tc.expectedDurationSec);

    tc.reason = reason;
    tc.message =
            switch (reason) {
              case COMPLETION_ALL_USERS_FINISHED ->
                      "All users completed iterations before hold expired.";
              case COMPLETION_HOLD_EXPIRED ->
                      "Execution ran for the configured hold/duration.";
              case COMPLETION_ERROR ->
                      "Execution stopped before completion due to errors or early termination.";
              default -> "Execution finished.";
            };

    return tc;
  }

  /* ----------------------------- capacity ----------------------------- */

  private TaskRunReport.CapacityAnalysis computeCapacity(
          Double targetRps,
          double achievedRps) {

    final TaskRunReport.CapacityAnalysis ca =
            new TaskRunReport.CapacityAnalysis();

    ca.targetRps = targetRps;
    ca.achievedRps = achievedRps;

    if (targetRps == null || targetRps <= 0) {
      ca.utilizationPercent = null;
      ca.assessment = "UNKNOWN";
      ca.recommendation =
              "Define a target RPS to assess utilization.";
      return ca;
    }

    final double utilization = (achievedRps / targetRps) * 100.0;
    ca.utilizationPercent = utilization;

    if (utilization < 80.0) {
      ca.assessment = "UNDER_UTILIZED";
      ca.recommendation =
              "Increase load to better utilize system capacity.";
    } else if (utilization <= 120.0) {
      ca.assessment = "OPTIMAL";
      ca.recommendation =
              "Maintain current load; system operating near target.";
    } else {
      ca.assessment = "OVER_UTILIZED";
      ca.recommendation =
              "System has headroom; consider raising targets or adding scenarios.";
    }

    return ca;
  }

  /* ----------------------------- user analysis ----------------------------- */

  private TaskRunReport.UserCompletionAnalysis computeUserAnalysis(
          List<TaskRunReport.UserCompletion> histogram) {

    if (histogram == null || histogram.isEmpty()) {
      return null;
    }

    final TaskRunReport.UserCompletionAnalysis uca =
            new TaskRunReport.UserCompletionAnalysis();

    TaskRunReport.UserCompletion fastest = null;
    TaskRunReport.UserCompletion slowest = null;
    long sum = 0;

    for (TaskRunReport.UserCompletion uc : histogram) {
      sum += uc.completionTimeMs;
      if (fastest == null || uc.completionTimeMs < fastest.completionTimeMs) {
        fastest = uc;
      }
      if (slowest == null || uc.completionTimeMs > slowest.completionTimeMs) {
        slowest = uc;
      }
    }

    final double avg = sum / (double) histogram.size();
    double varianceSum = 0.0;

    for (TaskRunReport.UserCompletion uc : histogram) {
      final double d = uc.completionTimeMs - avg;
      varianceSum += d * d;
    }

    final double stdDev =
            Math.sqrt(varianceSum / histogram.size());
    final double variancePct =
            avg > 0 ? (stdDev / avg) * 100.0 : 0.0;

    final String assessment =
            variancePct < 20.0
                    ? VARIANCE_LOW
                    : (variancePct <= 50.0 ? VARIANCE_MODERATE : VARIANCE_HIGH);

    uca.fastest = toSummary(fastest);
    uca.slowest = toSummary(slowest);
    uca.avgCompletionTimeMs = avg;
    uca.stdDevMs = stdDev;
    uca.varianceAssessment = assessment;

    uca.insight =
            switch (assessment) {
              case VARIANCE_LOW ->
                      "User completion times are consistent across the run.";
              case VARIANCE_MODERATE ->
                      "Moderate variance observed; investigate hotspots or uneven work distribution.";
              default ->
                      "High variance in completion times; likely contention or endpoint variability.";
            };

    return uca;
  }

  /* ----------------------------- summary ----------------------------- */

  private TaskRunReport.Summary computeSummary(
          TaskRunReport.Metrics metrics,
          TaskRunReport.TestCompletion completion,
          TaskRunReport.CapacityAnalysis capacity,
          TaskRunReport.ProtocolDetails protocolDetails) {

    final TaskRunReport.Summary summary =
            new TaskRunReport.Summary();

    final String status =
            metrics.successRate >= 1.0
                    ? STATUS_SUCCESS
                    : (metrics.successRate >= 0.95
                    ? STATUS_PARTIAL_SUCCESS
                    : STATUS_FAILED);

    summary.status = status;
    summary.message =
            switch (status) {
              case STATUS_SUCCESS ->
                      "All requests succeeded with no failures.";
              case STATUS_PARTIAL_SUCCESS ->
                      "Minor failures observed; overall run largely successful.";
              default ->
                      "Failures observed; review errors and outliers.";
            };

    summary.highlights = new ArrayList<>(6);
    summary.concerns = new ArrayList<>(4);

    summary.highlights.add(
            String.format("successRate=%.2f%%", metrics.successRate * 100));
    summary.highlights.add(
            "latency.avg=" + metrics.latency.avg + "ms");
    summary.highlights.add(
            "latency.p95=" + metrics.latency.p95 + "ms");

    if (capacity != null && capacity.targetRps != null) {
      summary.highlights.add(
              String.format(
                      "achievedRps=%.2f vs target=%.2f",
                      metrics.achievedRps, capacity.targetRps));
    }

    if (completion != null && completion.reason != null) {
      summary.highlights.add("completion=" + completion.reason);
    }

    if (metrics.failureCount > 0) {
      summary.concerns.add("failures=" + metrics.failureCount);
    }

    if (metrics.usersStarted > 0
            && metrics.usersCompleted < metrics.usersStarted) {
      summary.concerns.add(
              "incompleteUsers="
                      + (metrics.usersStarted - metrics.usersCompleted));
    }

    if (protocolDetails != null
            && protocolDetails.rest != null
            && protocolDetails.rest.endpoints != null
            && protocolDetails.rest.endpoints.stream()
            .anyMatch(e -> Boolean.TRUE.equals(e.outlierDetected))) {
      summary.concerns.add("latencyOutliersDetected");
    }

    if (capacity != null
            && capacity.utilizationPercent != null
            && capacity.utilizationPercent < 50.0) {
      summary.concerns.add(
              "lowUtilization="
                      + String.format("%.1f%%", capacity.utilizationPercent));
    }

    return summary;
  }

  /* ----------------------------- helpers ----------------------------- */

  private TaskRunReport.UserCompletionSummary toSummary(
          TaskRunReport.UserCompletion uc) {

    final TaskRunReport.UserCompletionSummary s =
            new TaskRunReport.UserCompletionSummary();
    s.userId = uc.userId;
    s.completionTimeMs = uc.completionTimeMs;
    s.iterationsCompleted = uc.iterationsCompleted;
    return s;
  }

  private Double expectedDuration(TaskConfig cfg) {
    if (cfg.model() == LoadModelType.CLOSED) {
      return seconds(cfg.warmup())
              + seconds(cfg.rampUp())
              + seconds(cfg.holdFor());
    }
    if (cfg.model() == LoadModelType.OPEN && cfg.duration() != null) {
      return seconds(cfg.duration());
    }
    return null;
  }

  private String determineCompletionReason(
          TaskConfig cfg,
          double actualDurationSec,
          int usersCompleted,
          CompletionInfo info,
          Double expectedDurationSec) {

    if (cfg.model() == LoadModelType.CLOSED) {
      final boolean holdExpired =
              info != null && Boolean.TRUE.equals(info.holdExpired);
      final Integer totalUsers =
              info != null ? info.totalUsers : cfg.users();

      if (holdExpired) return COMPLETION_HOLD_EXPIRED;
      if (totalUsers != null && usersCompleted >= totalUsers)
        return COMPLETION_ALL_USERS_FINISHED;
      if (totalUsers != null) return COMPLETION_ERROR;

      return (expectedDurationSec != null
              && actualDurationSec + 1 < expectedDurationSec)
              ? COMPLETION_ERROR
              : COMPLETION_HOLD_EXPIRED;
    }

    if (expectedDurationSec != null) {
      return actualDurationSec >= expectedDurationSec - 1.0
              ? COMPLETION_HOLD_EXPIRED
              : COMPLETION_ERROR;
    }

    return COMPLETION_HOLD_EXPIRED;
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