package com.mk.fx.qa.load.execution.dto.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mk.fx.qa.load.execution.model.LoadModelType;
import lombok.Getter;

@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class LoadModelConfig {

  @JsonProperty("type")
  private LoadModelType type;

  @JsonProperty("arrivalRatePerSec")
  private Double arrivalRatePerSec;

  @JsonProperty("maxConcurrent")
  private Integer maxConcurrent;

  @JsonProperty("duration")
  private String duration;

  @JsonProperty("iterations")
  private Integer iterations;

  @JsonProperty("users")
  private Integer users;

  @JsonProperty("rampUp")
  private String rampUp;

  @JsonProperty("holdFor")
  private String holdFor;

  @JsonProperty("warmup")
  private String warmup;
}
