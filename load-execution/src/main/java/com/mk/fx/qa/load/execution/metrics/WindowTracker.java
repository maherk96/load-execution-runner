package com.mk.fx.qa.load.execution.metrics;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Aggregates latency and request stats within rolling windows for time-series reporting. Resets
 * counters after each snapshot, returning a {@link TimeSeriesPoint}.
 */
final class WindowTracker {
  private final AtomicLong windowLatencyMin = new AtomicLong(Long.MAX_VALUE);
  private final AtomicLong windowLatencyMax = new AtomicLong(0);
  private final AtomicLong windowLatencySum = new AtomicLong(0);
  private final AtomicLong windowRequests = new AtomicLong(0);

  void record(long latencyMs) {
    long v = Math.max(0, latencyMs);
    windowLatencyMax.accumulateAndGet(v, Math::max);
    windowLatencyMin.accumulateAndGet(v, Math::min);
    windowLatencySum.addAndGet(v);
    windowRequests.incrementAndGet();
  }

  TimeSeriesPoint snapshotAndReset(
      Instant timestamp,
      long totalRequests,
      long totalErrors,
      int usersStarted,
      int usersCompleted) {
    long minLocal = windowLatencyMin.get();
    long maxLocal = windowLatencyMax.get();
    long countLocal = windowRequests.get();
    long sumLocal = windowLatencySum.get();
    long avgLocal = countLocal == 0 ? 0 : (sumLocal / countLocal);

    // reset for next window
    windowLatencyMin.set(Long.MAX_VALUE);
    windowLatencyMax.set(0);
    windowLatencySum.set(0);
    windowRequests.set(0);

    return new TimeSeriesPoint(
        timestamp,
        totalRequests,
        totalErrors,
        minLocal == Long.MAX_VALUE ? 0 : minLocal,
        maxLocal,
        avgLocal,
        usersStarted,
        usersCompleted);
  }
}
