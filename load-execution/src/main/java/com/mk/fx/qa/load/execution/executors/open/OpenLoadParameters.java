package com.mk.fx.qa.load.execution.executors.open;

import java.time.Duration;

/**
 * Parameters for an open load execution.
 *
 * @param arrivalRatePerSec target request arrival rate per second
 * @param maxConcurrent maximum number of concurrent iterations allowed
 * @param duration total duration to run the open model before stopping
 */
public record OpenLoadParameters(double arrivalRatePerSec, int maxConcurrent, Duration duration) {}
