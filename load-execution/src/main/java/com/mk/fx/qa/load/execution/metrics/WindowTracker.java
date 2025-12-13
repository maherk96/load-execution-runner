package com.mk.fx.qa.load.execution.metrics;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Aggregates latency and request stats within rolling windows for time-series reporting.
 *
 * <p>Thread-safety & semantics:
 * <ul>
 *   <li>Only requests with known latency are recorded.</li>
 *   <li>Snapshot + reset is atomic per field using getAndSet.</li>
 *   <li>Empty windows always report min=0, avg=0, max=0.</li>
 * </ul>
 */
final class WindowTracker {

  private final AtomicLong windowLatencyMin = new AtomicLong(Long.MAX_VALUE);
  private final AtomicLong windowLatencyMax = new AtomicLong(0);
  private final AtomicLong windowLatencySum = new AtomicLong(0);
  private final AtomicLong windowSamples = new AtomicLong(0);

  /** Record a latency sample into the current window. */
  void record(long latencyMs) {
    long v = Math.max(0, latencyMs);
    windowLatencyMin.accumulateAndGet(v, Math::min);
    windowLatencyMax.accumulateAndGet(v, Math::max);
    windowLatencySum.addAndGet(v);
    windowSamples.incrementAndGet();
  }

  /**
   * Snapshots the current window and resets it atomically.
   */
  TimeSeriesPoint snapshotAndReset(
          Instant timestamp,
          long totalRequests,
          long totalErrors,
          int usersStarted,
          int usersCompleted) {

    // Atomically capture + reset
    long count = windowSamples.getAndSet(0);
    long sum = windowLatencySum.getAndSet(0);
    long min = windowLatencyMin.getAndSet(Long.MAX_VALUE);
    long max = windowLatencyMax.getAndSet(0);

    long latMin;
    long latMax;
    long latAvg;

    if (count == 0) {
      latMin = 0;
      latMax = 0;
      latAvg = 0;
    } else {
      latMin = (min == Long.MAX_VALUE) ? 0 : min;
      latMax = max;
      latAvg = sum / count;
    }

    return new TimeSeriesPoint(
            timestamp,
            totalRequests,
            totalErrors,
            latMin,
            latMax,
            latAvg,
            usersStarted,
            usersCompleted);
  }
}