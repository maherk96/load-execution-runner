package com.mk.fx.qa.load.execution.executors.open;

/**
 * Options controlling open-model saturation and failure behavior.
 */
public record OpenLoadOptions(
    SaturationPolicy saturationPolicy, IterationFailurePolicy iterationFailurePolicy) {

  public static OpenLoadOptions defaults() {
    return new OpenLoadOptions(SaturationPolicy.DROP, IterationFailurePolicy.CANCEL_TEST);
  }

  /** Controls how to behave when max concurrency is saturated at a tick. */
  public enum SaturationPolicy {
    /** Skip this iteration; do not queue it for later. */
    DROP,
    /** Defer this iteration for later execution when permits free up. */
    DELAY
  }

  /** Controls how to react when an iteration fails with an exception. */
  public enum IterationFailurePolicy {
    /** Cancel the entire open test execution on first failure. */
    CANCEL_TEST,
    /** Continue scheduling further iterations; record failures for metrics. */
    CONTINUE
  }
}

