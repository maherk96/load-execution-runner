package com.mk.fx.qa.load.execution.dto.controllerresponse;

import com.mk.fx.qa.load.execution.model.LoadModelType;
import java.time.Duration;
import java.util.Map;
import lombok.Data;

/**
 * Represents the live metrics of a task, including its configuration and current state. This class
 * is used to provide real-time feedback on the task's execution.
 */
@Data
public class TaskLiveMetricsResponse {

  private String taskId;
  private String taskType;
  private String baseUrl;
  private LoadModelType model;
  private Integer users;
  private Integer iterationsPerUser;
  private Duration warmup;
  private Duration rampUp;
  private Duration holdFor;
  private Double arrivalRatePerSec;
  private Duration duration;
  private int requestsPerIteration;
  private long expectedTotalRequests;
  private Double expectedRps;

  private int usersStarted;
  private int usersCompleted;
  private long totalRequests;
  private long totalErrors;
  private Double achievedRps;
  private Long latencyMinMs;
  private Long latencyAvgMs;
  private Long latencyMaxMs;
  private Map<Integer, Integer> activeUserIterations;
}
