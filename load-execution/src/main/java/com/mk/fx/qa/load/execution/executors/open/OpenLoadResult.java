package com.mk.fx.qa.load.execution.executors.open;

/**
 * Result of an open load execution.
 *
 * @param launched number of iterations launched by the scheduler
 * @param completed number of iterations that completed and released their permit
 * @param cancelled true if execution was cancelled due to external request or iteration error
 */
public record OpenLoadResult(long launched, long completed, boolean cancelled) {}
