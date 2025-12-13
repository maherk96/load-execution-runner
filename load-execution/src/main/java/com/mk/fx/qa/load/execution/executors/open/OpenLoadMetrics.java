package com.mk.fx.qa.load.execution.executors.open;

/**
 * Snapshot of open-model metrics for observability and testing.
 */
public record OpenLoadMetrics(
    long scheduled, // tick opportunities
    long launched, // submitted to executor
    long started, // iteration body entered
    long completed, // iteration finished
    long failed, // iteration threw
    long dropped, // saturation DROP policy events
    long inflight, // started - completed
    long backlog // delayed iterations pending
    ) {}

