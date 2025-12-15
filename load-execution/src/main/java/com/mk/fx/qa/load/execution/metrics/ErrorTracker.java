package com.mk.fx.qa.load.execution.metrics;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks failures during execution, providing totals, categorized breakdown and capped samples.
 *
 * <p>Classification derives a coarse error type from the root cause class, with special handling
 * for common network failures. A small number of formatted stack sample entries are retained for
 * inclusion in the final report.
 */
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
    String key = category == null || category.isBlank() ? "UNKNOWN" : category;
    errorBreakdown.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
  }

  long totalErrors() {
    return totalErrors.get();
  }

  Map<String, Long> breakdownSnapshot() {
    Map<String, Long> snapshot = new java.util.HashMap<>();
    for (var e : errorBreakdown.entrySet()) {
      snapshot.put(e.getKey(), e.getValue().get());
    }
    return Map.copyOf(snapshot);
  }

  List<TaskRunReport.ErrorSample> samplesSnapshot() {
    return List.copyOf(errorSamples);
  }

  /* ------------------------------------------------------------ */

  private String classifyError(Throwable t) {
    if (t == null) return "UNKNOWN";

    Throwable root = t;
    while (root.getCause() != null) {
      root = root.getCause();
    }

    return switch (root.getClass().getSimpleName()) {
      case "ConnectException" -> "CONNECTION_REFUSED";
      case "SocketTimeoutException" -> "SOCKET_TIMEOUT";
      case "UnknownHostException" -> "UNKNOWN_HOST";
      case "SSLException" -> "SSL_ERROR";
      case "HttpTimeoutException" -> "HTTP_TIMEOUT";
      default -> root.getClass().getSimpleName().isBlank()
              ? "EXCEPTION"
              : root.getClass().getSimpleName();
    };
  }

  private TaskRunReport.ErrorSample buildErrorSample(String type, Throwable t) {
    Throwable root = t;
    while (root.getCause() != null) {
      root = root.getCause();
    }

    String message = t.getMessage();
    if (message == null || message.equals("null")) {
      message = root.getMessage();
    }
    if (message == null || message.equals("null")) {
      message = root.getClass().getSimpleName() + " occurred";
    }

    List<String> frames = new ArrayList<>();

    if (root != t) {
      frames.add(
              "ROOT CAUSE: "
                      + root.getClass().getSimpleName()
                      + " - "
                      + (root.getMessage() != null ? root.getMessage() : "no message"));

      StackTraceElement[] rootStack = root.getStackTrace();
      for (int i = 0; i < Math.min(3, rootStack.length); i++) {
        frames.add("  at " + formatStackFrame(rootStack[i]));
      }
      frames.add("");
    }

    frames.add("WRAPPED BY: " + t.getClass().getSimpleName());
    StackTraceElement[] stack = t.getStackTrace();
    for (int i = 0; i < Math.min(10, stack.length); i++) {
      frames.add("  at " + formatStackFrame(stack[i]));
    }

    TaskRunReport.ErrorSample sample = new TaskRunReport.ErrorSample();
    sample.type = type;
    sample.message = message;
    sample.stackTrace = frames;

    return sample;
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