package com.mk.fx.qa.load.execution.metrics;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

final class ErrorTracker {
  private static final int MAX_ERROR_SAMPLES = 5;

  private final AtomicLong totalErrors = new AtomicLong();
  private final Map<String, AtomicLong> errorBreakdown = new ConcurrentHashMap<>();
  private final List<TaskRunReport.ErrorSample> errorSamples = new CopyOnWriteArrayList<>();

  void recordFailure(Throwable t) {
    totalErrors.incrementAndGet();
    String key = classifyError(t);
    errorBreakdown.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
    if (t != null && errorSamples.size() < MAX_ERROR_SAMPLES) {
      errorSamples.add(buildErrorSample(key, t));
    }
  }

  void recordFailureCategory(String category) {
    totalErrors.incrementAndGet();
    String key = category == null || category.isBlank() ? "UNKNOWN" : category.toUpperCase();
    errorBreakdown.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
  }

  long totalErrors() {
    return totalErrors.get();
  }

  Map<String, Long> breakdownSnapshot() {
    Map<String, Long> map = new java.util.HashMap<>();
    for (var e : errorBreakdown.entrySet()) map.put(e.getKey(), e.getValue().get());
    return java.util.Map.copyOf(map);
  }

  List<TaskRunReport.ErrorSample> samplesSnapshot() {
    return List.copyOf(errorSamples);
  }

  private String classifyError(Throwable t) {
    if (t == null) return "UNKNOWN";
    Throwable rootCause = t;
    while (rootCause.getCause() != null) {
      rootCause = rootCause.getCause();
    }
    var clsName = rootCause.getClass().getSimpleName();
    return switch (clsName) {
      case "ConnectException" -> "CONNECTION_REFUSED";
      case "SocketTimeoutException" -> "SOCKET_TIMEOUT";
      case "UnknownHostException" -> "UNKNOWN_HOST";
      case "SSLException" -> "SSL_ERROR";
      case "HttpTimeoutException" -> "HTTP_TIMEOUT";
      default -> clsName.isBlank() ? "EXCEPTION" : clsName;
    };
  }

  private TaskRunReport.ErrorSample buildErrorSample(String type, Throwable t) {
    Throwable rootCause = t;
    while (rootCause.getCause() != null) {
      rootCause = rootCause.getCause();
    }

    String msg = t.getMessage();
    if (msg == null || msg.equals("null")) {
      msg = rootCause.getMessage();
    }
    if (msg == null || msg.equals("null")) {
      msg = rootCause.getClass().getSimpleName() + " occurred";
    }

    List<String> frames = new ArrayList<>();

    if (rootCause != t) {
      frames.add(
          "ROOT CAUSE: "
              + rootCause.getClass().getSimpleName()
              + " - "
              + (rootCause.getMessage() != null ? rootCause.getMessage() : "no message"));
      StackTraceElement[] rootStack = rootCause.getStackTrace();
      int rootLimit = Math.min(3, rootStack.length);
      for (int i = 0; i < rootLimit; i++) {
        frames.add("  at " + formatStackFrame(rootStack[i]));
      }
      frames.add("");
    }

    frames.add("WRAPPED BY: " + t.getClass().getSimpleName());
    StackTraceElement[] stackTrace = t.getStackTrace();
    int limit = Math.min(10, stackTrace.length);
    for (int i = 0; i < limit; i++) {
      frames.add("  at " + formatStackFrame(stackTrace[i]));
    }

    return new TaskRunReport.ErrorSample(type, msg, frames);
  }

  private String formatStackFrame(StackTraceElement frame) {
    return frame.getClassName()
        + "."
        + frame.getMethodName()
        + "("
        + frame.getFileName()
        + ":"
        + frame.getLineNumber()
        + ")";
  }
}
