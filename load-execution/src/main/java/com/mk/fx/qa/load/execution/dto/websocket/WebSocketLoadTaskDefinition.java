package com.mk.fx.qa.load.execution.dto.websocket;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mk.fx.qa.load.execution.dto.common.ExecutionConfig;
import java.util.List;
import java.util.Map;
import lombok.Getter;

/** Represents the WebSocket load task definition, mirroring the REST structure. */
@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class WebSocketLoadTaskDefinition {

  @JsonProperty("testSpec")
  private WsTestSpec testSpec;

  @JsonProperty("execution")
  private ExecutionConfig execution;

  @Getter
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class WsTestSpec {

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

    @JsonProperty("messages")
    private List<MessageSpec> messages;
  }

  @Getter
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class MessageSpec {

    @JsonProperty("name")
    private String name;

    // Provide either text or json; if json is provided it will be serialized.
    @JsonProperty("text")
    private String text;

    @JsonProperty("json")
    private Object json;

    // Optional: wait for a pattern in incoming messages to consider success and measure RTT.
    @JsonProperty("awaitPattern")
    private String awaitPattern;

    @JsonProperty("awaitTimeoutMs")
    private Integer awaitTimeoutMs;
  }

  @Getter
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class GlobalConfig {

    // Full ws/wss URL is required for WebSocket.
    @JsonProperty("url")
    private String url;

    @JsonProperty("headers")
    private Map<String, String> headers;

    // Variables to resolve in url and message text/json string values.
    @JsonProperty("vars")
    private Map<String, String> vars;

    @JsonProperty("timeouts")
    private TimeoutConfig timeouts;

    @JsonProperty("subprotocols")
    private List<String> subprotocols;
  }

  @Getter
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class TimeoutConfig {

    @JsonProperty("connectionTimeoutMs")
    private Integer connectionTimeoutMs;

    // Timeout used when awaiting a response pattern for a sent message, if not overridden per msg
    @JsonProperty("messageTimeoutMs")
    private Integer messageTimeoutMs;
  }
}
