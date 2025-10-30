package com.mk.fx.qa.load.execution.rest;

import java.util.Map;
import lombok.Data;

@Data
public class Request {
  private HttpMethod method;
  private String path;
  private Map<String, String> headers;
  private Map<String, String> query;
  private Object body;
}
