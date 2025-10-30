package com.mk.fx.qa.load.execution.service.stratigies;

import com.mk.fx.qa.load.execution.dto.common.ThinkTimeConfig;
import com.mk.fx.qa.load.execution.model.ThinkTimeType;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThinkTimeStrategy {

  private static final long SLEEP_CHUNK_MILLIS = 200L;

  private final ThinkTimeType type;
  private final long min;
  private final long max;
  private final long fixed;

  private ThinkTimeStrategy(ThinkTimeType type, long min, long max, long fixed) {
    this.type = type;
    this.min = min;
    this.max = max;
    this.fixed = fixed;
  }

  public static ThinkTimeStrategy from(ThinkTimeConfig config) {
    if (config == null || config.getType() == null) {
      return new ThinkTimeStrategy(ThinkTimeType.NONE, 0, 0, 0);
    }
    return switch (config.getType()) {
      case NONE -> new ThinkTimeStrategy(ThinkTimeType.NONE, 0, 0, 0);
      case FIXED -> new ThinkTimeStrategy(
          ThinkTimeType.FIXED, 0, 0, config.getFixedMs() != null ? config.getFixedMs() : 0);
      case RANDOM -> new ThinkTimeStrategy(
          ThinkTimeType.RANDOM,
          config.getMin() != null ? config.getMin() : 0,
          config.getMax() != null ? config.getMax() : 0,
          0);
    };
  }

  public boolean isEnabled() {
    return type != ThinkTimeType.NONE;
  }

  public void pause(AtomicBoolean cancelled) throws InterruptedException {
    long delay =
        switch (type) {
          case NONE -> 0;
          case FIXED -> Math.max(0, fixed);
          case RANDOM -> computeRandomDelay();
        };
    if (delay > 0) {
      var remaining = delay;
      while (remaining > 0) {
        if (cancelled.get() || Thread.currentThread().isInterrupted()) {
          throw new InterruptedException("Task cancelled");
        }
        var chunk = Math.min(SLEEP_CHUNK_MILLIS, remaining);
        TimeUnit.MILLISECONDS.sleep(chunk);
        remaining -= chunk;
      }
    }
  }

  private long computeRandomDelay() {
    var lower = Math.max(0, min);
    var upper = Math.max(lower, max);
    if (upper == lower) {
      return lower;
    }
    return ThreadLocalRandom.current().nextLong(lower, upper + 1);
  }
}
