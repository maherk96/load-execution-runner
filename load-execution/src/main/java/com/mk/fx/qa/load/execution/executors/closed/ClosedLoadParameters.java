package com.mk.fx.qa.load.execution.executors.closed;

import java.time.Duration;

/**
 * Parameters for a closed load test execution.
 *
 * <p>This record encapsulates the configuration for a closed load test, including the number of
 * users, iterations, and timing parameters for warmup, ramp-up, and hold phases.
 *
 * @param users the number of users participating in the load test
 * @param iterations the number of iterations each user will perform
 * @param warmup the duration of the warmup phase before the actual load test starts
 * @param rampUp the duration over which users are gradually introduced to the system
 * @param holdFor the duration for which the load is maintained at its peak level
 */
public record ClosedLoadParameters(
    int users, int iterations, Duration warmup, Duration rampUp, Duration holdFor) {}