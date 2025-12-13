package com.mk.fx.qa.load.execution.executors.closed;

/** Options for closed-model execution behavior. */
public record ClosedLoadOptions(
    StopMode stopMode, FailureMode failureMode, ShutdownPolicy shutdownPolicy) {

  public static ClosedLoadOptions defaults(ClosedLoadParameters p) {
    var mode = p.holdFor() != null && !p.holdFor().isZero() ? StopMode.DURATION : StopMode.ITERATIONS;
    return new ClosedLoadOptions(mode, FailureMode.STOP_USER, ShutdownPolicy.FORCEFUL_ON_TIMEOUT);
  }

  /** Whether to stop users by iteration count or by duration (hold-for). */
  public enum StopMode {
    ITERATIONS,
    DURATION
  }

  /** How to react to a single virtual-user iteration failure. */
  public enum FailureMode {
    /** Stop only the failing user; others continue. */
    STOP_USER,
    /** Cancel the entire test upon first user failure. */
    CANCEL_TEST
  }

  /** Shutdown behavior for user thread pool when finishing. */
  public enum ShutdownPolicy {
    /** Request shutdown and only force if termination times out. */
    FORCEFUL_ON_TIMEOUT,
    /** Request shutdown and wait only; never call shutdownNow(). */
    GRACEFUL_ONLY
  }
}

