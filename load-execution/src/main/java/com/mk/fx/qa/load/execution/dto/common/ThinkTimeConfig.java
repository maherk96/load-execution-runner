package com.mk.fx.qa.load.execution.dto.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mk.fx.qa.load.execution.model.ThinkTimeType;
import lombok.Getter;

/** Think time configuration for load testing. */
@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class ThinkTimeConfig {

  @JsonProperty("type")
  private ThinkTimeType type;

  @JsonProperty("min")
  private Long min;

  @JsonProperty("max")
  private Long max;

  @JsonProperty("fixedMs")
  private Long fixedMs;
}
