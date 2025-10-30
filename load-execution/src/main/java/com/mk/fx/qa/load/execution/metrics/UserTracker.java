package com.mk.fx.qa.load.execution.metrics;

import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

final class UserTracker {
  private final AtomicInteger usersStarted = new AtomicInteger();
  private final AtomicInteger usersCompleted = new AtomicInteger();
  private final Map<Integer, Integer> userIterations = new ConcurrentHashMap<>();
  private final Map<Integer, Instant> userStartTimes = new ConcurrentHashMap<>();
  private final Map<Integer, Instant> userEndTimes = new ConcurrentHashMap<>();
  private final Map<Integer, AtomicInteger> userIterationsCompleted = new ConcurrentHashMap<>();

  void onUserStarted(int userIndex) {
    userIterations.putIfAbsent(userIndex, 0);
    usersStarted.incrementAndGet();
    userStartTimes.put(userIndex, Instant.now());
    userIterationsCompleted.putIfAbsent(userIndex, new AtomicInteger());
  }

  void onUserProgress(int userIndex, int iteration) {
    userIterations.put(userIndex, iteration);
    userIterationsCompleted.computeIfAbsent(userIndex, k -> new AtomicInteger()).set(iteration + 1);
  }

  void onUserCompleted(int userIndex, int totalIterationsCompleted) {
    usersCompleted.incrementAndGet();
    userIterations.remove(userIndex);
    userEndTimes.put(userIndex, Instant.now());
    userIterationsCompleted
        .computeIfAbsent(userIndex, k -> new AtomicInteger())
        .set(totalIterationsCompleted);
  }

  int totalUsersStarted() {
    return usersStarted.get();
  }

  int totalUsersCompleted() {
    return usersCompleted.get();
  }

  Map<Integer, Integer> currentIterations() {
    return Map.copyOf(userIterations);
  }

  List<TaskRunReport.UserCompletion> buildHistogram() {
    List<TaskRunReport.UserCompletion> histogram = new ArrayList<>();
    for (Map.Entry<Integer, Instant> e : userEndTimes.entrySet()) {
      var start = userStartTimes.get(e.getKey());
      if (start != null) {
        TaskRunReport.UserCompletion uc = new TaskRunReport.UserCompletion();
        uc.userId = e.getKey() + 1;
        uc.completionTimeMs = Duration.between(start, e.getValue()).toMillis();
        uc.iterationsCompleted =
            userIterationsCompleted.getOrDefault(e.getKey(), new AtomicInteger(0)).get();
        histogram.add(uc);
      }
    }
    return List.copyOf(histogram);
  }
}
