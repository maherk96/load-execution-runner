package com.mk.fx.qa.load.execution.dto.controllerresponse;

import com.mk.fx.qa.load.execution.model.LoadModelType;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * User-facing execution report for a load task.
 *
 * <p>This report is model-aware (OPEN vs CLOSED), human-readable,
 * and designed to avoid ambiguous or misleading fields.
 */
public class TaskRunReport {

  /* ============================================================
   * Identity & Timing
   * ============================================================ */

  public String taskId;
  public String taskType;
  public LoadModelType model;

  public Instant startTime;
  public Instant endTime;

  /** Wall-clock duration in seconds */
  public double durationSec;

  /**
   * One-line human-readable explanation of the run outcome.
   * Intended for dashboards, notifications, and quick reviews.
   */
  public String executiveSummary;


  /* ============================================================
   * Environment
   * ============================================================ */

  public Environment environment;

  public static class Environment {
    public String branch;
    public String commit;
    public String host;
    public String triggeredBy;
  }

  /* ============================================================
   * Execution Configuration (MODEL AWARE)
   * ============================================================ */

  public ExecutionConfig execution;

  public static class ExecutionConfig {

    /** Present only for CLOSED model */
    public ClosedConfig closed;

    /** Present only for OPEN model */
    public OpenConfig open;

    /** Derived expectations used for reporting */
    public long expectedTotalRequests;
    public Double expectedRps;
  }

  public static class ClosedConfig {
    public Integer users;
    public Integer iterationsPerUser;
    public Integer requestsPerIteration;

    /** Null = not configured */
    public Duration warmup;
    public Duration rampUp;
    public Duration holdFor;
  }

  public static class OpenConfig {
    public Double arrivalRatePerSec;
    public Duration duration;
    public Integer maxConcurrent;
  }

  /* ============================================================
   * Model Semantics (Prevents Misinterpretation)
   * ============================================================ */

  public ModelInfo modelInfo;

  public static class ModelInfo {
    public String description;
    public String throughputExplanation;
    public String configNotes;
  }

  /* ============================================================
   * Aggregated Metrics
   * ============================================================ */

  public Metrics metrics;

  public static class Metrics {

    public long totalRequests;
    public long successCount;
    public long failureCount;
    public double successRate;

    /** Average throughput across full execution */
    public double achievedRps;

    public Latency latency;

    public int usersStarted;
    public int usersCompleted;

    public List<ErrorBreakdownItem> errorBreakdown;
    public List<ErrorSample> errorSamples;

    public List<UserCompletion> userCompletionHistogram;
  }

  public static class Latency {
    public long min;
    public long avg;
    public long max;
    public long p95;
    public long p99;
  }

  public static class ErrorBreakdownItem {
    public String type;
    public long count;
  }

  public static class ErrorSample {
    public String type;
    public String message;
    public List<String> stackTrace;
  }

  public static class UserCompletion {
    /** 1-based user ID (matches logs and UI) */
    public int userId;
    public long completionTimeMs;
    public int iterationsCompleted;
  }

  /* ============================================================
   * Time Series (SNAPSHOTS)
   * ============================================================ */

  public List<TimeSeriesSnapshot> timeSeries;

  public static class TimeSeriesSnapshot {

    public Instant timestamp;

    /** Users currently active at this point in time */
    public int usersActive;

    /** Users completed SO FAR (not final total) */
    public int usersCompletedSoFar;

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

  /* ============================================================
   * Protocol Details
   * ============================================================ */

  public ProtocolDetails protocolDetails;

  public static class ProtocolDetails {
    public RestDetails rest;
    public WebSocketDetails websocket;
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

    /** Informational only */
    public boolean outlierDetected;
    public String outlierInfo;
    public Instant outlierTimestamp;

    /** 1-based user ID most likely affected */
    public Integer likelyAffectedUserId;
  }

  public static class WebSocketDetails {
    public List<WebSocketMessage> messages;
  }

  public static class WebSocketMessage {
    public String name;
    public long total;
    public long success;
    public long failure;
    public Latency latency;
  }

  /* ============================================================
   * Completion & Interpretation
   * ============================================================ */

  public TestCompletion completion;

  public static class TestCompletion {
    public CompletionReason reason;
    public double actualDurationSec;
    public Double expectedDurationSec;
    public int percentComplete;
    public String message;
  }

  public enum CompletionReason {
    ALL_USERS_FINISHED,
    HOLD_EXPIRED,
    CANCELLED,
    ERROR
  }

  /* ============================================================
   * Capacity Analysis
   * ============================================================ */

  public CapacityAnalysis capacity;

  public static class CapacityAnalysis {

    public Double targetRps;
    public Double achievedRps;
    public Double utilizationPercent;

    public CapacityAssessment assessment;
    public String recommendation;
    public String note;
  }

  public enum CapacityAssessment {
    UNDER_UTILIZED,
    OPTIMAL,
    OVER_UTILIZED,
    NOT_APPLICABLE
  }

  /* ============================================================
   * User Completion Analysis
   * ============================================================ */

  public UserCompletionAnalysis userCompletionAnalysis;

  public static class UserCompletionAnalysis {

    public UserCompletionSummary fastest;
    public UserCompletionSummary slowest;

    public double avgCompletionTimeMs;
    public double stdDevMs;

    public VarianceAssessment variance;
    public String insight;
  }

  public static class UserCompletionSummary {
    public int userId;
    public long completionTimeMs;
    public int iterationsCompleted;
  }

  public enum VarianceAssessment {
    LOW,
    MODERATE,
    HIGH
  }

  /* ============================================================
   * Final Summary (What users actually read)
   * ============================================================ */

  public Summary summary;

  public static class Summary {

    public ExecutionStatus status;
    public Severity severity;

    public String message;

    public List<String> highlights;
    public List<String> concerns;
  }

  public enum ExecutionStatus {
    SUCCESS,
    PARTIAL_SUCCESS,
    FAILED
  }

  public enum Severity {
    INFO,
    WARNING,
    CRITICAL
  }
}