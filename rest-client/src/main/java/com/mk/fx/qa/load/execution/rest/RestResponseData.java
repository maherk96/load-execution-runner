package com.mk.fx.qa.load.execution.rest;

import java.util.Map;
import lombok.Data;

@Data
public class RestResponseData {
  private int statusCode;
  private Map<String, String> headers;
  private String body;
  private long responseTimeMs;
}
