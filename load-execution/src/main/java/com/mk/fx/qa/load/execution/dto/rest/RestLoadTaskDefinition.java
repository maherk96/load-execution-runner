package com.mk.fx.qa.load.execution.dto.rest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mk.fx.qa.load.execution.dto.common.ExecutionConfig;
import lombok.Getter;

import java.util.List;
import java.util.Map;

/**
 * Represents the REST load task definition, including test specifications,
 * execution configurations, and request specifications.
 */
@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class RestLoadTaskDefinition {

    @JsonProperty("testSpec")
    private RestTestSpec testSpec;

    @JsonProperty("execution")
    private ExecutionConfig execution;

    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RestTestSpec {

        @JsonProperty("id")
        private String id;

        @JsonProperty("globalConfig")
        private GlobalConfig globalConfig;

        @JsonProperty("scenarios")
        private List<Scenario> scenarios;
    }

    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Scenario {

        @JsonProperty("name")
        private String name;

        @JsonProperty("requests")
        private List<RequestSpec> requests;
    }

    @Getter
    public static class RequestSpec {

        @JsonProperty("method")
        private String method;

        @JsonProperty("path")
        private String path;

        @JsonProperty("headers")
        private Map<String, String> headers;

        @JsonProperty("query")
        private Map<String, String> query;

        @JsonProperty("body")
        private Object body;
    }

        @Getter
        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class GlobalConfig {

            @JsonProperty("baseUrl")
            private String baseUrl;

            @JsonProperty("headers")
            private Map<String, String> headers;

            @JsonProperty("vars")
            private Map<String, String> vars;

            @JsonProperty("timeouts")
            private TimeoutConfig timeouts;
        }

        @Getter
        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class TimeoutConfig {

            @JsonProperty("connectionTimeoutMs")
            private Integer connectionTimeoutMs;

            @JsonProperty("requestTimeoutMs")
            private Integer requestTimeoutMs;

    }

}