package com.mk.fx.qa.load.execution.dto.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

/** Optional options for CLOSED load model behavior. */
@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClosedModelOptionsConfig {

  /** Stop condition: ITERATIONS or DURATION. */
  @JsonProperty("stopMode")
  private String stopMode;

  /** Failure mode for a virtual user: STOP_USER or CANCEL_TEST. */
  @JsonProperty("failureMode")
  private String failureMode;

  /** Shutdown policy for the thread pool: FORCEFUL_ON_TIMEOUT or GRACEFUL_ONLY. */
  @JsonProperty("shutdownPolicy")
  private String shutdownPolicy;
}

