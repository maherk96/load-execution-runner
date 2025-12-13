package com.mk.fx.qa.load.execution.dto.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

/** Optional options for OPEN load model behavior. */
@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class OpenModelOptionsConfig {

  /** Saturation policy when maxConcurrency is reached: DROP or DELAY. */
  @JsonProperty("saturationPolicy")
  private String saturationPolicy;

  /** Iteration failure behavior: CANCEL_TEST or CONTINUE. */
  @JsonProperty("iterationFailure")
  private String iterationFailure;
}

