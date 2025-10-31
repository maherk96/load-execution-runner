package com.mk.fx.qa.load.execution.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import com.mk.fx.qa.load.execution.model.LoadModelType;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
  private final AtomicLong requests = new AtomicLong();
  private final LatencyTracker latency = new LatencyTracker(5000);
  private final WindowTracker window = new WindowTracker();
  private final ErrorTracker errorTracker = new ErrorTracker();
  private final UserTracker users = new UserTracker();
  private final List<ProtocolMetricsProvider> protocolProviders = new CopyOnWriteArrayList<>();

  private ScheduledExecutorService snapshots;
  private final List<TimeSeriesPoint> timeSeries = new CopyOnWriteArrayList<>();
  private volatile Instant lastSnapshotAt = startedAt;
  private volatile CompletionInfo completionInfo;

  public Map<Integer, Integer> currentUserIterations() {
    return users.currentIterations();
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

  public void setCompletionContext(
      boolean cancelled, Boolean holdExpired, Integer totalUsers, Integer completedUsers) {
    this.completionInfo = new CompletionInfo(cancelled, holdExpired, totalUsers, completedUsers);
  }

  public void recordUserStarted(int userIndex) {
    users.onUserStarted(userIndex);
    log.info(
        "Task {} user {} started (usersStarted={})",
        config.taskId(),
        userIndex + 1,
        users.totalUsersStarted());
  }

  public void recordUserProgress(int userIndex, int iteration) {
    users.onUserProgress(userIndex, iteration);
  }

  public void recordUserCompleted(int userIndex, int totalIterationsCompleted) {
    users.onUserCompleted(userIndex, totalIterationsCompleted);
    log.info(
        "Task {} user {} completed {} iterations (usersCompleted={})",
        config.taskId(),
        userIndex + 1,
        totalIterationsCompleted,
        users.totalUsersCompleted());
  }

  public void recordRequestSuccess(long latencyMs) {
    requests.incrementAndGet();
    latency.record(latencyMs);
    window.record(latencyMs);
  }

  public void recordRequestFailure(Throwable t) {
    requests.incrementAndGet();
    errorTracker.recordFailure(t);
  }

  public void registerProtocolMetrics(ProtocolMetricsProvider provider) {
    protocolProviders.add(provider);
  }

  public void recordFailure(String category, long latencyMs) {
    requests.incrementAndGet();
    latency.record(latencyMs);
    window.record(latencyMs);
    errorTracker.recordFailureCategory(category);
  }

  public long totalRequests() {
    return requests.get();
  }

  public long totalErrors() {
    return errorTracker.totalErrors();
  }

  public int totalUsersStarted() {
    return users.totalUsersStarted();
  }

  public int totalUsersCompleted() {
    return users.totalUsersCompleted();
  }

  public Optional<Long> latencyAvgMs() {
    var count = requests.get();
    if (count == 0) return Optional.empty();
    return Optional.of(latency.sumMs() / Math.max(1, count));
  }

  public Optional<Long> latencyP95Ms() {
    return latency.p95Ms();
  }

  public Optional<Long> latencyP99Ms() {
    return latency.p99Ms();
  }

  public Optional<Long> latencyMinMs() {
    return latency.minMs();
  }

  public Optional<Long> latencyMaxMs() {
    return latency.maxMs();
  }

  public LoadSnapshot snapshotNow() {
    return new LoadSnapshot(
        config,
        users.totalUsersStarted(),
        users.totalUsersCompleted(),
        requests.get(),
        errorTracker.totalErrors(),
        achievedRps(),
        latencyMinMs().orElse(null),
        latencyAvgMs().orElse(null),
        latencyMaxMs().orElse(null),
        users.currentIterations());
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

    var currentIterations = users.currentIterations();
    if (!currentIterations.isEmpty()) {
      sb.append(", activeUsers=");
      var shown = 0;
      for (Map.Entry<Integer, Integer> e : currentIterations.entrySet()) {
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
    var err = errorTracker.totalErrors();

    timeSeries.add(
        window.snapshotAndReset(
            now, req, err, users.totalUsersStarted(), users.totalUsersCompleted()));
    lastSnapshotAt = now;
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
    return new LoadReportBuilder()
        .build(
            config,
            startedAt,
            totalRequests(),
            totalErrors(),
            achievedRps(),
            latency,
            users,
            errorTracker,
            timeSeries,
            protocolProviders,
            completionInfo);
  }

  public Map<String, Long> errorBreakdown() {
    return errorTracker.breakdownSnapshot();
  }

  public List<TimeSeriesPoint> getTimeSeries() {
    return List.copyOf(timeSeries);
  }
}
