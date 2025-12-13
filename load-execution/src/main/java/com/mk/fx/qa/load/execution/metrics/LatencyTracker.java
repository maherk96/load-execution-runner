package com.mk.fx.qa.load.execution.metrics;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks latency distribution statistics in a thread-safe manner.
 *
 * <p>Maintains running min/max/sum and a {@link Reservoir} for approximate percentiles.
 * Only calls to {@link #record(long)} create latency samples.
 */
final class LatencyTracker {

  private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
  private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
  private final AtomicLong sum = new AtomicLong();
  private final AtomicLong samples = new AtomicLong();
  private final Reservoir reservoir;

  LatencyTracker(int reservoirCapacity) {
    this.reservoir = new Reservoir(reservoirCapacity);
  }



  long sampleCount() {
    return samples.get();
  }

  long sumMs() {
    return sum.get();
  }

  Optional<Long> minMs() {
    return samples.get() == 0
            ? Optional.empty()
            : Optional.of(min.get());
  }

  Optional<Long> maxMs() {
    return samples.get() == 0
            ? Optional.empty()
            : Optional.of(max.get());
  }

  Optional<Long> avgMs() {
    long c = samples.get();
    return c == 0 ? Optional.empty() : Optional.of(sum.get() / c);
  }

  Optional<Long> p95Ms() {
    return samples.get() == 0 ? Optional.empty() : reservoir.percentile(95);
  }

  Optional<Long> p99Ms() {
    return samples.get() == 0 ? Optional.empty() : reservoir.percentile(99);
  }

  void record(long latencyMs) {
    long v = Math.max(0, latencyMs);
    samples.incrementAndGet();
    sum.addAndGet(v);
    max.accumulateAndGet(v, Math::max);
    min.accumulateAndGet(v, Math::min);
    reservoir.add(v);
  }
}
