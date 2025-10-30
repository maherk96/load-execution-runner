package com.mk.fx.qa.load.execution.dto.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import java.util.Map;

@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExecutionConfig {

    @JsonProperty("thinkTime")
    private ThinkTimeConfig thinkTime;

    @JsonProperty("loadModel")
    private LoadModelConfig loadModel;

    @JsonProperty("globalSla")
    private Map<String, Object> globalSla;
}