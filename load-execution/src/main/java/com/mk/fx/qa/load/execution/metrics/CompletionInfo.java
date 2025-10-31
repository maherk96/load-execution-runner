package com.mk.fx.qa.load.execution.metrics;

final class CompletionInfo {
  final boolean cancelled;
  final Boolean holdExpired; // null when not applicable
  final Integer totalUsers; // null for OPEN
  final Integer completedUsers; // null for OPEN

  CompletionInfo(
      boolean cancelled, Boolean holdExpired, Integer totalUsers, Integer completedUsers) {
    this.cancelled = cancelled;
    this.holdExpired = holdExpired;
    this.totalUsers = totalUsers;
    this.completedUsers = completedUsers;
  }
}
