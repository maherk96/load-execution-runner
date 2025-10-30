package com.mk.fx.qa.load.execution.executors.closed;

/**
 * Represents the result of a closed load execution.
 *
 * @param totalUsers The total number of users involved in the load execution.
 * @param completedUsers The number of users that completed the load execution.
 * @param cancelled Indicates if the load execution was cancelled.
 * @param holdExpired Indicates if the hold period expired during the load execution.
 */
public record ClosedLoadResult(
    int totalUsers, int completedUsers, boolean cancelled, boolean holdExpired) {}