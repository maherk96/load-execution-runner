package com.mk.fx.qa.load.execution.metrics;

import java.util.Arrays;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe fixed-size reservoir for approximate percentile estimation.
 * Implements classic reservoir sampling (Algorithm R).
 */
public final class Reservoir {
  private final int capacity;
  private final AtomicLong count = new AtomicLong();
  private final long[] data;
  private final Random rnd;

  public Reservoir(int capacity) {
    if (capacity <= 0) throw new IllegalArgumentException("Capacity must be > 0");
    this.capacity = capacity;
    this.data = new long[capacity];
    this.rnd = new Random();
  }

  public synchronized void add(long value) {
    long c = count.incrementAndGet();
    if (c <= capacity) {
      data[(int) c - 1] = value;
    } else {
      long j = rnd.nextLong(c);
      if (j < capacity) {
        data[(int) j] = value;
      }
    }
  }

  public synchronized Optional<Long> percentile(int p) {
    if (p < 0 || p > 100)
      throw new IllegalArgumentException("Percentile must be between 0 and 100");

    long c = Math.min(count.get(), capacity);
    if (c == 0) return Optional.empty();

    long[] copy = Arrays.copyOf(data, (int) c);
    Arrays.sort(copy);
    int idx = Math.min((int) c - 1, Math.max(0, (int) Math.ceil((p / 100.0) * c) - 1));
    return Optional.of(copy[idx]);
  }
}