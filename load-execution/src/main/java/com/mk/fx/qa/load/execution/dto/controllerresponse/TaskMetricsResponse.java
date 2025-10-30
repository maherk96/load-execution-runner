package com.mk.fx.qa.load.execution.dto.controllerresponse;

/**
 * This class represents the response structure for task metrics.
 * It contains fields for total completed, failed, cancelled tasks,
 * average processing time, success rate, and total processed tasks.
 */
public record TaskMetricsResponse(
        long totalCompleted,
        long totalFailed,
        long totalCancelled,
        double averageProcessingTimeMillis,
        double successRate,
        long totalProcessed
) {
}