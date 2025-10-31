package com.mk.fx.qa.load.execution.dto.controllerresponse;

import com.mk.fx.qa.load.execution.model.LoadModelType;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * Represents a detailed report of a task run, including configuration, metrics, and
 * protocol-specific details.
 */
public class TaskRunReport {

  public String taskId;
  public String taskType;
  public LoadModelType model;
  public Instant startTime;
  public Instant endTime;
  public double durationSec;

  public EnvInfo environment;
  public Config config;
  public Metrics metrics;

  public List<TimeSeriesEntry> timeSeriesEntries;
  public ProtocolDetails protocolDetails;
  public TestCompletion testCompletion;
  public CapacityAnalysis capacityAnalysis;
  public UserCompletionAnalysis userCompletionAnalysis;
  public Summary summary;

  public static class EnvInfo {
    public String branch;
    public String commit;
    public String host;
    public String triggeredBy;
  }

  public static class Config {
    public Integer users;
    public Integer iterationsPerUser;
    public Integer requestsPerIteration;
    public Duration warmup;
    public Duration rampUp;
    public Duration holdFor;
    public Double arrivalRatePerSec;
    public Duration openDuration;
    public long expectedTotalRequests;
    public Double expectedRps;
  }

  public static class Metrics {

    public long totalRequests;
    public long successCount;
    public long failureCount;
    public double successRate;
    public double achievedRps;
    public Latency latency;
    public List<ErrorItem> errorBreakdown;
    public List<UserCompletion> userCompletionHistogram;
    public List<ErrorSample> errorSamples;
    public int usersStarted;
    public int usersCompleted;
    public Double expectedRps;
  }

  @NoArgsConstructor
  @AllArgsConstructor
  public static class ErrorSample {
    public String type;
    public String message;
    public List<String> stack;
  }

  public static class ErrorItem {
    public String type;
    public long count;
  }

  public static class UserCompletion {
    public int userId;
    public long completionTimeMs;
    public int iterationsCompleted;
  }

  public static class TimeSeriesEntry {
    public Instant timestamp;
    public int usersActive;
    public int usersCompleted;
    public long totalRequestsSoFar;
    public double rpsInWindow;
    public Double expectedRpsInWindow;
    public LatencyWindow latency;
    public long errorsInWindow;
  }

  public static class LatencyWindow {
    public long min;
    public long avg;
    public long max;
  }

  public static class ProtocolDetails {
    public RestDetails rest;
  }

  public static class RestDetails {
    public List<RestEndpoint> endpoints;
  }

  public static class RestEndpoint {
    public String method;
    public String path;
    public long total;
    public long success;
    public long failure;
    public Latency latency;
    public Map<String, Long> statusBreakdown;
    public Boolean outlierDetected;
    public String outlierInfo;
    public java.time.Instant outlierTimestamp;
    public Integer likelyAffectedUser;
  }

  public static class Latency {
    public long min;
    public Long avg;
    public long max;
    public Long p95;
    public Long p99;
  }

  public static class TestCompletion {
    public String reason; // ALL_USERS_FINISHED, HOLD_EXPIRED, CANCELLED, ERROR
    public Double expectedDurationSec;
    public Double actualDurationSec;
    public Integer percentComplete; // 0-100
    public String message;
  }

  public static class CapacityAnalysis {
    public Double targetRps;
    public Double achievedRps;
    public Double utilizationPercent;
    public String assessment; // UNDER_UTILIZED, OPTIMAL, OVER_UTILIZED
    public String recommendation;
  }

  public static class UserCompletionAnalysis {
    public UserCompletionSummary fastest;
    public UserCompletionSummary slowest;
    public Double avgCompletionTimeMs;
    public Double stdDevMs;
    public String varianceAssessment; // LOW, MODERATE, HIGH
    public String insight;
  }

  public static class UserCompletionSummary {
    public Integer userId;
    public Long completionTimeMs;
    public Integer iterationsCompleted;
  }

  public static class Summary {
    public String status; // SUCCESS, PARTIAL_SUCCESS, FAILED
    public String message;
    public List<String> highlights;
    public List<String> concerns;
  }
}
