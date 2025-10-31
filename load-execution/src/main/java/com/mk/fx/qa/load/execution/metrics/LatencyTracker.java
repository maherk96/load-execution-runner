package com.mk.fx.qa.load.execution.metrics;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks latency distribution statistics in a thread-safe manner.
 *
 * <p>Maintains running min/max/sum and a {@link Reservoir} for approximate percentiles.
 */
final class LatencyTracker {

  private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
  private final AtomicLong max = new AtomicLong(0);
  private final AtomicLong sum = new AtomicLong();
  private final Reservoir reservoir;

  LatencyTracker(int reservoirCapacity) {
    this.reservoir = new Reservoir(reservoirCapacity);
  }

  void record(long latencyMs) {
    long v = Math.max(0, latencyMs);
    sum.addAndGet(v);
    max.accumulateAndGet(v, Math::max);
    min.accumulateAndGet(v, Math::min);
    reservoir.add(v);
  }

  Optional<Long> minMs() {
    long v = min.get();
    return v == Long.MAX_VALUE ? Optional.empty() : Optional.of(v);
  }

  Optional<Long> maxMs() {
    long v = max.get();
    return v == 0 ? Optional.empty() : Optional.of(v);
  }

  long sumMs() {
    return sum.get();
  }

  Optional<Long> p95Ms() {
    return reservoir.percentile(95);
  }

  Optional<Long> p99Ms() {
    return reservoir.percentile(99);
  }
}
