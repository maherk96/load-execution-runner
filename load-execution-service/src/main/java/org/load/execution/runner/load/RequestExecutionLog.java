// File: org/load/execution/runner/load/execution/RequestExecutionLog.java
package org.load.execution.runner.load;


import java.time.Instant;

/**
 * Record for logging detailed request execution information.
 *
 * @author Load Test Framework
 * @since 1.0
 */
public record RequestExecutionLog(
        Instant timestamp,
        org.load.execution.runner.load.TestPhaseManager.TestPhase phase,
        int userId,
        String method,
        String path,
        long durationMs,
        boolean backPressured,
        boolean success,
        int statusCode
) {}