package com.mk.fx.qa.load.execution.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import com.mk.fx.qa.load.execution.model.LoadModelType;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Collects and tracks load test metrics such as requests, latency, users, and protocol statistics.
 */
@Slf4j
public class LoadMetrics {

  @Getter private final TaskConfig config;

  private final Instant startedAt = Instant.now();
  private final AtomicInteger usersStarted = new AtomicInteger();
  private final AtomicInteger usersCompleted = new AtomicInteger();
  private final AtomicLong requests = new AtomicLong();
  private final AtomicLong errors = new AtomicLong();
  private final AtomicLong latencyMin = new AtomicLong(Long.MAX_VALUE);
  private final AtomicLong latencyMax = new AtomicLong(0);
  private final AtomicLong latencySum = new AtomicLong();
  private final Reservoir latencyReservoir = new Reservoir(5000);
  private final AtomicLong windowLatencyMin = new AtomicLong(Long.MAX_VALUE);
  private final AtomicLong windowLatencyMax = new AtomicLong(0);
  private final AtomicLong windowLatencySum = new AtomicLong(0);
  private final AtomicLong windowRequests = new AtomicLong(0);

  private final Map<String, AtomicLong> errorBreakdown = new ConcurrentHashMap<>();
  private final List<TaskRunReport.ErrorSample> errorSamples = new CopyOnWriteArrayList<>();
  private final List<ProtocolMetricsProvider> protocolProviders = new CopyOnWriteArrayList<>();

  private final Map<Integer, Integer> userIterations = new ConcurrentHashMap<>();
  private final Map<Integer, Instant> userStartTimes = new ConcurrentHashMap<>();
  private final Map<Integer, Instant> userEndTimes = new ConcurrentHashMap<>();
  private final Map<Integer, AtomicInteger> userIterationsCompleted = new ConcurrentHashMap<>();

  private ScheduledExecutorService snapshots;
  private final List<TimeSeriesPoint> timeSeries = new CopyOnWriteArrayList<>();
  private volatile Instant lastSnapshotAt = startedAt;

  private static final int MAX_ERROR_SAMPLES = 5;

  public Map<Integer, Integer> currentUserIterations() {
    return Map.copyOf(userIterations);
  }

  public LoadMetrics(TaskConfig config) {
    this.config = Objects.requireNonNull(config, "config");
  }

  public void start() {
    logStart();
    snapshots =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r);
              t.setName("metrics-snapshots-" + config.taskId());
              t.setDaemon(true);
              return t;
            });
    snapshots.scheduleAtFixedRate(this::snapshot, 5, 5, TimeUnit.SECONDS);
  }

  public void stopAndSummarise() {
    forceSnapshotForTest();
    if (snapshots != null) {
      snapshots.shutdownNow();
      try {
        snapshots.awaitTermination(2, TimeUnit.SECONDS);
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    }
    logFinalSummary();
  }

  public void recordUserStarted(int userIndex) {
    userIterations.putIfAbsent(userIndex, 0);
    usersStarted.incrementAndGet();
    userStartTimes.put(userIndex, Instant.now());
    userIterationsCompleted.putIfAbsent(userIndex, new AtomicInteger());
    log.info(
        "Task {} user {} started (usersStarted={})",
        config.taskId(),
        userIndex + 1,
        usersStarted.get());
  }

  public void recordUserProgress(int userIndex, int iteration) {
    userIterations.put(userIndex, iteration);
    userIterationsCompleted.computeIfAbsent(userIndex, k -> new AtomicInteger()).set(iteration + 1);
  }

  public void recordUserCompleted(int userIndex, int totalIterationsCompleted) {
    usersCompleted.incrementAndGet();
    userIterations.remove(userIndex);
    userEndTimes.put(userIndex, Instant.now());
    userIterationsCompleted
        .computeIfAbsent(userIndex, k -> new AtomicInteger())
        .set(totalIterationsCompleted);
    log.info(
        "Task {} user {} completed {} iterations (usersCompleted={})",
        config.taskId(),
        userIndex + 1,
        totalIterationsCompleted,
        usersCompleted.get());
  }

  public void recordRequestSuccess(long latencyMs) {
    requests.incrementAndGet();
    latencySum.addAndGet(Math.max(0, latencyMs));
    updateMinMax(latencyMs);
    latencyReservoir.add(latencyMs);
    updateWindow(latencyMs);
  }

  public void recordRequestFailure(Throwable t) {
    errors.incrementAndGet();
    requests.incrementAndGet();
    String key = classifyError(t);
    errorBreakdown.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
    if (t != null && errorSamples.size() < MAX_ERROR_SAMPLES) {
      errorSamples.add(buildErrorSample(key, t));
    }
  }

  public void registerProtocolMetrics(ProtocolMetricsProvider provider) {
    protocolProviders.add(provider);
  }

  public void recordFailure(String category, long latencyMs) {
    errors.incrementAndGet();
    requests.incrementAndGet();
    latencySum.addAndGet(Math.max(0, latencyMs));
    updateMinMax(latencyMs);
    latencyReservoir.add(latencyMs);
    String key = category == null || category.isBlank() ? "UNKNOWN" : category.toUpperCase();
    errorBreakdown.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
    updateWindow(latencyMs);
  }

  public long totalRequests() {
    return requests.get();
  }

  public long totalErrors() {
    return errors.get();
  }

  public int totalUsersStarted() {
    return usersStarted.get();
  }

  public int totalUsersCompleted() {
    return usersCompleted.get();
  }

  public Optional<Long> latencyAvgMs() {
    var count = requests.get();
    if (count == 0) return Optional.empty();
    return Optional.of(latencySum.get() / Math.max(1, count));
  }

  public Optional<Long> latencyP95Ms() {
    return latencyReservoir.percentile(95);
  }

  public Optional<Long> latencyP99Ms() {
    return latencyReservoir.percentile(99);
  }

  public Optional<Long> latencyMinMs() {
    var min = latencyMin.get();
    return min == Long.MAX_VALUE ? Optional.empty() : Optional.of(min);
  }

  public Optional<Long> latencyMaxMs() {
    var max = latencyMax.get();
    return max == 0 ? Optional.empty() : Optional.of(max);
  }

  private void updateWindow(long latencyMs) {
    var v = Math.max(0, latencyMs);
    windowLatencyMax.accumulateAndGet(v, Math::max);
    windowLatencyMin.accumulateAndGet(v, Math::min);
    windowLatencySum.addAndGet(v);
    windowRequests.incrementAndGet();
  }

  public LoadSnapshot snapshotNow() {
    return new LoadSnapshot(
        config,
        usersStarted.get(),
        usersCompleted.get(),
        requests.get(),
        errors.get(),
        achievedRps(),
        latencyMinMs().orElse(null),
        latencyAvgMs().orElse(null),
        latencyMaxMs().orElse(null),
        Map.copyOf(userIterations));
  }

  private void updateMinMax(long latencyMs) {
    var v = Math.max(0, latencyMs);
    latencyMax.accumulateAndGet(v, Math::max);
    latencyMin.accumulateAndGet(v, Math::min);
  }

  private double achievedRps() {
    double elapsedSec =
        Math.max(0.001, Duration.between(startedAt, Instant.now()).toMillis() / 1000.0);
    return requests.get() / elapsedSec;
  }

  private void logStart() {
    var sb = new StringBuilder();
    sb.append("Task ")
        .append(config.taskId())
        .append(" started: ")
        .append("type=")
        .append(config.taskType())
        .append(", model=")
        .append(config.model())
        .append(", baseUrl=")
        .append(config.baseUrl());
    if (config.model() == LoadModelType.OPEN) {
      sb.append(", rate=")
          .append(config.arrivalRatePerSec())
          .append("/s, duration=")
          .append(config.duration());
    } else {
      sb.append(", users=")
          .append(config.users())
          .append(", iterationsPerUser=")
          .append(config.iterationsPerUser())
          .append(", warmup=")
          .append(config.warmup())
          .append(", rampUp=")
          .append(config.rampUp())
          .append(", holdFor=")
          .append(config.holdFor());
    }
    sb.append(", requestsPerIteration=")
        .append(config.requestsPerIteration())
        .append(", expectedTotalRequests=")
        .append(config.expectedTotalRequests());
    if (config.expectedRps() != null) {
      sb.append(", expectedRps=").append(String.format("%.2f", config.expectedRps()));
    }
    log.info(sb.toString());
  }

  private void snapshot() {
    var rps = achievedRps();
    var sb = new StringBuilder();
    sb.append("Task ")
        .append(config.taskId())
        .append(" snapshot: ")
        .append("usersStarted=")
        .append(totalUsersStarted())
        .append(", usersCompleted=")
        .append(totalUsersCompleted())
        .append(", requests=")
        .append(totalRequests())
        .append("/")
        .append(config.expectedTotalRequests());
    if (config.expectedRps() != null) {
      sb.append(", rps actual/expected=")
          .append(String.format("%.2f", rps))
          .append("/")
          .append(String.format("%.2f", config.expectedRps()));
    } else {
      sb.append(", rps actual=").append(String.format("%.2f", rps));
    }
    latencyMinMs().ifPresent(min -> sb.append(", lat(ms) min=").append(min));
    latencyAvgMs().ifPresent(avg -> sb.append(", avg=").append(avg));
    latencyMaxMs().ifPresent(max -> sb.append(", max=").append(max));
    latencyP95Ms().ifPresent(p95 -> sb.append(", p95=").append(p95));
    latencyP99Ms().ifPresent(p99 -> sb.append(", p99=").append(p99));

    if (!userIterations.isEmpty()) {
      sb.append(", activeUsers=");
      var shown = 0;
      for (Map.Entry<Integer, Integer> e : userIterations.entrySet()) {
        if (shown++ >= 10) {
          break;
        }
        sb.append("[#").append(e.getKey() + 1).append(":iter=").append(e.getValue()).append("]");
      }
    }
    log.info(sb.toString());

    // store time-series point
    var now = Instant.now();
    var req = requests.get();
    var err = errors.get();

    var minLocal = windowLatencyMin.get();
    var maxLocal = windowLatencyMax.get();
    var countLocal = windowRequests.get();
    var sumLocal = windowLatencySum.get();
    var avgLocal = countLocal == 0 ? 0 : sumLocal / countLocal;

    timeSeries.add(
        new TimeSeriesPoint(
            now,
            req,
            err,
            minLocal == Long.MAX_VALUE ? 0 : minLocal,
            maxLocal,
            avgLocal,
            usersStarted.get(),
            usersCompleted.get()));
    lastSnapshotAt = now;
    windowLatencyMin.set(Long.MAX_VALUE);
    windowLatencyMax.set(0);
    windowLatencySum.set(0);
    windowRequests.set(0);
  }

  @VisibleForTesting
  public void forceSnapshotForTest() {
    snapshot();
  }

  private void logFinalSummary() {
    double rps = achievedRps();
    StringBuilder sb = new StringBuilder();
    sb.append("Task ")
        .append(config.taskId())
        .append(" summary ")
        .append("type=")
        .append(config.taskType())
        .append(", model=")
        .append(config.model())
        .append(", usersStarted=")
        .append(totalUsersStarted())
        .append(", usersCompleted=")
        .append(totalUsersCompleted())
        .append(", totalRequests=")
        .append(totalRequests())
        .append(", expectedTotalRequests=")
        .append(config.expectedTotalRequests());
    if (config.expectedRps() != null) {
      sb.append(", rps actual/expected=")
          .append(String.format("%.2f", rps))
          .append("/")
          .append(String.format("%.2f", config.expectedRps()));
    } else {
      sb.append(", rps actual=").append(String.format("%.2f", rps));
    }
    latencyMinMs().ifPresent(min -> sb.append(", lat(ms) min=").append(min));
    latencyAvgMs().ifPresent(avg -> sb.append(", avg=").append(avg));
    latencyMaxMs().ifPresent(max -> sb.append(", max=").append(max));
    latencyP95Ms().ifPresent(p95 -> sb.append(", p95=").append(p95));
    latencyP99Ms().ifPresent(p99 -> sb.append(", p99=").append(p99));
    if (totalErrors() > 0) {
      sb.append(", errors=").append(totalErrors());
    }
    log.info(sb.toString());
  }

  public TaskRunReport buildReport() {
    var r = new TaskRunReport();

    // identifiers & timing
    r.taskId = config.taskId();
    r.taskType = config.taskType();
    r.model = LoadModelType.valueOf(config.model().name());
    r.startTime = startedAt; // ISO-8601 via Jackson
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
    m.totalRequests = totalRequests();
    m.failureCount = totalErrors();
    m.successCount = Math.max(0, m.totalRequests - m.failureCount);
    m.successRate = m.totalRequests == 0 ? 0.0 : (double) m.successCount / m.totalRequests;
    m.achievedRps = achievedRps();
    TaskRunReport.Latency lat = new TaskRunReport.Latency();
    lat.avg = latencyAvgMs().orElse(0L);
    lat.min = latencyMinMs().orElse(0L);
    lat.max = latencyMaxMs().orElse(0L);
    lat.p95 = latencyP95Ms().orElse(0L);
    lat.p99 = latencyP99Ms().orElse(0L);
    m.latency = lat;

    // error breakdown array
    List<TaskRunReport.ErrorItem> errs = new ArrayList<>();
    for (var e : errorBreakdown.entrySet()) {
      TaskRunReport.ErrorItem item = new TaskRunReport.ErrorItem();
      item.type = e.getKey();
      item.count = e.getValue().get();
      errs.add(item);
    }
    m.errorBreakdown = List.copyOf(errs);

    // error samples
    if (!errorSamples.isEmpty()) {
      m.errorSamples = List.copyOf(errorSamples);
      for (TaskRunReport.ErrorSample sample : errorSamples) {
        var e = new TaskRunReport.ErrorSample();
        e.type = sample.type;
        e.message = sample.message;
        e.stack = sample.stack;
      }
    } else {
      m.errorSamples = List.of();
    }

    // user histogram
    List<TaskRunReport.UserCompletion> histogram = new ArrayList<>();
    for (Map.Entry<Integer, Instant> e : userEndTimes.entrySet()) {
      var start = userStartTimes.get(e.getKey());
      if (start != null) {
        TaskRunReport.UserCompletion uc = new TaskRunReport.UserCompletion();
        uc.userId = e.getKey() + 1;
        uc.completionTimeMs = Duration.between(start, e.getValue()).toMillis();
        uc.iterationsCompleted =
            userIterationsCompleted.getOrDefault(e.getKey(), new AtomicInteger(0)).get();
        histogram.add(uc);
      }
    }
    m.userCompletionHistogram = List.copyOf(histogram);
    m.usersStarted = totalUsersStarted();
    m.usersCompleted = totalUsersCompleted();
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

  private String classifyError(Throwable t) {
    if (t == null) return "UNKNOWN";

    // Look for the root cause for better classification
    Throwable rootCause = t;
    while (rootCause.getCause() != null) {
      rootCause = rootCause.getCause();
    }

    // Use root cause class name for classification
    var clsName = rootCause.getClass().getSimpleName();

    // Map common exceptions to friendly names
    return switch (clsName) {
      case "ConnectException" -> "CONNECTION_REFUSED";
      case "SocketTimeoutException" -> "SOCKET_TIMEOUT";
      case "UnknownHostException" -> "UNKNOWN_HOST";
      case "SSLException" -> "SSL_ERROR";
      case "HttpTimeoutException" -> "HTTP_TIMEOUT";
      default -> clsName.isBlank() ? "EXCEPTION" : clsName;
    };
  }

  public Map<String, Long> errorBreakdown() {
    Map<String, Long> map = new java.util.HashMap<>();
    for (var e : errorBreakdown.entrySet()) map.put(e.getKey(), e.getValue().get());
    return java.util.Map.copyOf(map);
  }

  private TaskRunReport.ErrorSample buildErrorSample(String type, Throwable t) {
    // Get the root cause
    Throwable rootCause = t;
    while (rootCause.getCause() != null) {
      rootCause = rootCause.getCause();
    }

    // Use the most descriptive message available
    String msg = t.getMessage();
    if (msg == null || msg.equals("null")) {
      msg = rootCause.getMessage();
    }
    if (msg == null || msg.equals("null")) {
      msg = rootCause.getClass().getSimpleName() + " occurred";
    }

    List<String> frames = new ArrayList<>();

    // Add root cause info first (most important)
    if (rootCause != t) {
      frames.add(
          "ROOT CAUSE: "
              + rootCause.getClass().getSimpleName()
              + " - "
              + (rootCause.getMessage() != null ? rootCause.getMessage() : "no message"));

      // Add a few root cause stack frames
      StackTraceElement[] rootStack = rootCause.getStackTrace();
      int rootLimit = Math.min(3, rootStack.length);
      for (int i = 0; i < rootLimit; i++) {
        StackTraceElement frame = rootStack[i];
        frames.add("  at " + formatStackFrame(frame));
      }
      frames.add("");
    }

    // Then add the wrapper exception stack
    frames.add("WRAPPED BY: " + t.getClass().getSimpleName());
    StackTraceElement[] stackTrace = t.getStackTrace();
    var limit = Math.min(10, stackTrace.length);
    for (int i = 0; i < limit; i++) {
      frames.add("  at " + formatStackFrame(stackTrace[i]));
    }

    return new TaskRunReport.ErrorSample(type, msg, frames);
  }

  private String formatStackFrame(StackTraceElement frame) {
    return frame.getClassName()
        + "."
        + frame.getMethodName()
        + "("
        + frame.getFileName()
        + ":"
        + frame.getLineNumber()
        + ")";
  }

  public List<TimeSeriesPoint> getTimeSeries() {
    return List.copyOf(timeSeries);
  }
}
