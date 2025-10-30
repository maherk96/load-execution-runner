package com.mk.fx.qa.load.execution.dto.controllerresponse;

/**
 * Represents the status of a task queue, including the current size of the queue,
 * the number of active tasks, and whether the queue is accepting new tasks.
 */
public record QueueStatusResponse(int queueSize, int activeTasks, boolean acceptingTasks) {
}