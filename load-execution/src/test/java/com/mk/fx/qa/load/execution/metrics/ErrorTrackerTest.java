package com.mk.fx.qa.load.execution.metrics;

import static org.junit.jupiter.api.Assertions.*;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;
import javax.net.ssl.SSLException;
import org.junit.jupiter.api.Test;

class ErrorTrackerTest {

  @Test
  void recordFailure_classifiesCommonNetworkErrors_andSamplesCapped() {
    ErrorTracker t = new ErrorTracker();
    t.recordFailure(new ConnectException("refused"));
    t.recordFailure(new java.net.SocketTimeoutException("so slow"));
    t.recordFailure(new UnknownHostException("nohost"));
    t.recordFailure(new SSLException("ssl"));
    t.recordFailure(new HttpTimeoutException("http timeout"));
    // Generic
    t.recordFailure(new RuntimeException("other"));

    assertEquals(6, t.totalErrors());
    var breakdown = t.breakdownSnapshot();
    assertTrue(breakdown.get("CONNECTION_REFUSED") >= 1);
    assertTrue(breakdown.get("SOCKET_TIMEOUT") >= 1);
    assertTrue(breakdown.get("UNKNOWN_HOST") >= 1);
    assertTrue(breakdown.get("SSL_ERROR") >= 1);
    assertTrue(breakdown.get("HTTP_TIMEOUT") >= 1);

    // Samples limited to 5
    assertTrue(t.samplesSnapshot().size() <= 5);
  }

  @Test
  void recordFailureCategory_incrementsWithUppercase_andUnknownOnBlank() {
    ErrorTracker t = new ErrorTracker();
    t.recordFailureCategory("http_500");
    t.recordFailureCategory("");
    t.recordFailureCategory(null);
    assertEquals(3, t.totalErrors());
    var map = t.breakdownSnapshot();
    assertEquals(1L, map.get("HTTP_500"));
    assertEquals(2L, map.get("UNKNOWN"));
  }

  @Test
  void samplesContainRootCauseFrames_whenWrapped() {
    ErrorTracker t = new ErrorTracker();
    var cause = new IllegalStateException("cause");
    var ex = new RuntimeException("wrapper", cause);
    t.recordFailure(ex);
    var samples = t.samplesSnapshot();
    assertFalse(samples.isEmpty());
    TaskRunReport.ErrorSample s = samples.get(0);
    assertNotNull(s.message);
    assertFalse(s.stack.isEmpty());
    assertTrue(s.stack.get(0).startsWith("ROOT CAUSE:"));
  }
}
