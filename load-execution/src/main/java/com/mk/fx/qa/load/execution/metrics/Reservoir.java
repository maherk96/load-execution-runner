package com.mk.fx.qa.load.execution.metrics;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public final class Reservoir {
    private final int capacity;
    private final AtomicLong count = new AtomicLong();
    private final long[] data;
    private final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    public Reservoir(int capacity) {
        this.capacity = capacity;
        this.data = new long[capacity];
    }

    public void add(long value) {
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

    public Optional<Long> percentile(int p) {
        long c = Math.min(count.get(), capacity);
        if (c == 0) return Optional.empty();
        long[] copy = java.util.Arrays.copyOf(data, (int) c);
        java.util.Arrays.sort(copy);
        int idx = Math.min((int) c - 1, Math.max(0, (int) Math.ceil((p / 100.0) * c) - 1));
        return Optional.of(copy[idx]);
    }
}