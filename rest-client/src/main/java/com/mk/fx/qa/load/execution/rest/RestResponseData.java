package com.mk.fx.qa.load.execution.rest;

import lombok.Data;
import java.util.Map;

@Data
public class RestResponseData {
    private int statusCode;
    private Map<String, String> headers;
    private String body;
    private long responseTimeMs;
}