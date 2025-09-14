package org.load.execution.runner.load;

import lombok.Data;

import java.util.Map;

@Data
public class RestResponseData {
  private int statusCode;
  private Map<String, String> headers;
  private String body;
  private long responseTimeMs;
}
