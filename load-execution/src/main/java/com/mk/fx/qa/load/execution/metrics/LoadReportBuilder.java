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
      List<ProtocolMetricsProvider> protocolProviders) {

    var r = new TaskRunReport();

    // identifiers & timing
    r.taskId = config.taskId();
    r.taskType = config.taskType();
    r.model = LoadModelType.valueOf(config.model().name());
    r.startTime = startedAt;
    var end = Instant.now();
    r.endTime = end;
    r.durationSec = Math.max(0.0, Duration.between(startedAt, end).toMillis() / 1000.0);

    // environment
    TaskRunReport.EnvInfo env = new TaskRunReport.EnvInfo();
    env.host = EnvironmentInfo.host();
    env.triggeredBy = EnvironmentInfo.triggeredBy();
    r.environment = env;

    // config
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
    var expectedRps = config.expectedRps();
    if (expectedRps == null
        && config.model() == LoadModelType.CLOSED
        && config.holdFor() != null
        && !config.holdFor().isZero()) {
      var holdSec = Math.max(0.001, config.holdFor().toMillis() / 1000.0);
      expectedRps = cfg.expectedTotalRequests / holdSec;
    }
    cfg.expectedRps = expectedRps;
    r.config = cfg;

    // metrics aggregate
    TaskRunReport.Metrics m = new TaskRunReport.Metrics();
    m.totalRequests = totalRequests;
    m.failureCount = totalErrors;
    m.successCount = Math.max(0, m.totalRequests - m.failureCount);
    m.successRate = m.totalRequests == 0 ? 0.0 : (double) m.successCount / m.totalRequests;
    m.achievedRps = achievedRps;
    TaskRunReport.Latency lat = new TaskRunReport.Latency();
    lat.avg = totalRequests == 0 ? 0L : latency.sumMs() / Math.max(1, totalRequests);
    lat.min = latency.minMs().orElse(0L);
    lat.max = latency.maxMs().orElse(0L);
    lat.p95 = latency.p95Ms().orElse(0L);
    lat.p99 = latency.p99Ms().orElse(0L);
    m.latency = lat;

    // error breakdown array
    List<TaskRunReport.ErrorItem> errs = new ArrayList<>();
    for (Map.Entry<String, Long> e : errors.breakdownSnapshot().entrySet()) {
      TaskRunReport.ErrorItem item = new TaskRunReport.ErrorItem();
      item.type = e.getKey();
      item.count = e.getValue();
      errs.add(item);
    }
    m.errorBreakdown = List.copyOf(errs);

    // error samples
    m.errorSamples = errors.samplesSnapshot();

    // user histogram
    m.userCompletionHistogram = users.buildHistogram();
    m.usersStarted = users.totalUsersStarted();
    m.usersCompleted = users.totalUsersCompleted();
    m.expectedRps = cfg.expectedRps;
    r.metrics = m;

    // timeseries
    List<TaskRunReport.TimeSeriesEntry> windows = new java.util.ArrayList<>();
    long prevReq = 0;
    long prevErr = 0;
    Instant prevTs = startedAt;
    for (TimeSeriesPoint p : timeSeries) {
      TaskRunReport.TimeSeriesEntry w = new TaskRunReport.TimeSeriesEntry();
      w.timestamp = p.timestamp();
      w.usersCompleted = p.usersCompleted();
      int usersActive = Math.max(0, p.usersStarted() - p.usersCompleted());
      w.usersActive = usersActive;
      long deltaReq = p.totalRequests() - prevReq;
      long deltaErr = p.totalErrors() - prevErr;
      double secs = Math.max(0.001, Duration.between(prevTs, p.timestamp()).toMillis() / 1000.0);
      w.rpsInWindow = deltaReq / secs;
      w.expectedRpsInWindow = cfg.expectedRps; // constant target for now
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
    r.timeSeriesEntries = List.copyOf(windows);

    for (ProtocolMetricsProvider provider : protocolProviders) {
      provider.applyTo(r);
    }
    return r;
  }
}
