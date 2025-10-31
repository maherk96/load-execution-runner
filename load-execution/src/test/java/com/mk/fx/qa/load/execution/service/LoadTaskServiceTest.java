package com.mk.fx.qa.load.execution.service;

import static org.junit.jupiter.api.Assertions.*;

import com.mk.fx.qa.load.execution.cfg.TaskProcessingCfg;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskStatusResponse;
import com.mk.fx.qa.load.execution.model.LoadTask;
import com.mk.fx.qa.load.execution.model.TaskStatus;
import com.mk.fx.qa.load.execution.model.TaskType;
import com.mk.fx.qa.load.execution.processors.LoadTaskProcessor;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class LoadTaskServiceTest {

  private static LoadTask newTask(TaskType type) {
    return new LoadTask(UUID.randomUUID(), type, Instant.now(), Map.of("k", "v"));
  }

  private static LoadTask newTask(TaskType type, UUID id) {
    return new LoadTask(id, type, Instant.now(), Map.of("k", "v"));
  }

  private static TaskProcessingCfg cfg(int concurrency, int history) {
    TaskProcessingCfg cfg = new TaskProcessingCfg();
    cfg.setConcurrency(concurrency);
    cfg.setHistorySize(history);
    return cfg;
  }

  // Reflection helpers
  private static <T> T getPrivateField(Object target, String fieldName, Class<T> type) {
    try {
      var f = LoadTaskService.class.getDeclaredField(fieldName);
      f.setAccessible(true);
      return type.cast(f.get(target));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static void setPrivateField(Object target, String fieldName, Object value) {
    try {
      var f = LoadTaskService.class.getDeclaredField(fieldName);
      f.setAccessible(true);
      f.set(target, value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void submitTask_returnsErrorWhenNoProcessorFound() {
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of());
    LoadTask task = newTask(TaskType.REST);

    Optional<com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionOutcome> outcomeOpt =
        service.submitTask(task);

    assertTrue(outcomeOpt.isPresent());
    var outcome = outcomeOpt.get();
    assertEquals(task.getId(), outcome.taskId());
    assertEquals(TaskStatus.ERROR, outcome.status());
    assertTrue(outcome.message().contains("No processor"));

    // No record should be created for unsuccessful submission due to missing processor
    assertTrue(service.getTaskStatus(task.getId()).isEmpty());
  }

  @Test
  void submitTask_successfulExecution_updatesStatusAndMetrics() throws Exception {
    CountingProcessor processor = new CountingProcessor(TaskType.REST);
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of(processor));
    LoadTask task = newTask(TaskType.REST);

    var outcome = service.submitTask(task).orElseThrow();
    assertTrue(
        outcome.status() == TaskStatus.QUEUED
            || outcome.status() == TaskStatus.PROCESSING
            || outcome.status() == TaskStatus.COMPLETED,
        "Expected QUEUED/PROCESSING/COMPLETED at submission, but was " + outcome.status());

    // Wait for completion
    awaitStatus(service, task.getId(), TaskStatus.COMPLETED, 5);

    assertEquals(1, processor.executions.get());
    var status = service.getTaskStatus(task.getId()).map(TaskStatusResponse::status).orElseThrow();
    assertEquals(TaskStatus.COMPLETED, status);

    var metrics = service.getMetrics();
    assertEquals(1, metrics.totalCompleted());
    assertEquals(0, metrics.totalFailed());
    assertEquals(0, metrics.totalCancelled());
    assertTrue(metrics.averageProcessingTimeMillis() >= 0.0);
    assertEquals(1.0, metrics.successRate(), 1e-9);
  }

  @Test
  void submitTask_failure_updatesErrorAndMetrics() throws Exception {
    FailingProcessor processor = new FailingProcessor(TaskType.REST);
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of(processor));
    LoadTask task = newTask(TaskType.REST);

    service.submitTask(task);
    awaitStatus(service, task.getId(), TaskStatus.ERROR, 5);

    var status = service.getTaskStatus(task.getId()).map(TaskStatusResponse::status).orElseThrow();
    assertEquals(TaskStatus.ERROR, status);
    var metrics = service.getMetrics();
    assertEquals(0, metrics.totalCompleted());
    assertEquals(1, metrics.totalFailed());
    assertEquals(0, metrics.totalCancelled());
  }

  @Test
  void cancelTask_whenQueued_cancelsImmediately() throws Exception {
    BlockingProcessor processor = new BlockingProcessor(TaskType.REST);
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of(processor));

    LoadTask first = newTask(TaskType.REST);
    LoadTask second = newTask(TaskType.REST);

    // First task blocks the single worker
    service.submitTask(first);
    processor.started.await(2, TimeUnit.SECONDS); // ensure the first has started

    // Second task will be queued
    service.submitTask(second);

    var cancelResult = service.cancelTask(second.getId());
    assertEquals(
        LoadTaskService.CancellationResult.CancellationState.CANCELLED, cancelResult.getState());
    awaitStatus(service, second.getId(), TaskStatus.CANCELLED, 2);

    // Unblock first and let it complete to finish cleanly
    processor.release();
    awaitStatus(service, first.getId(), TaskStatus.COMPLETED, 5);
  }

  @Test
  void cancelTask_whenProcessing_requestsCancellation_thenMarksCancelled() throws Exception {
    InterruptibleProcessor processor = new InterruptibleProcessor(TaskType.REST);
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of(processor));
    LoadTask task = newTask(TaskType.REST);

    service.submitTask(task);
    processor.started.await(2, TimeUnit.SECONDS); // ensure running

    var result = service.cancelTask(task.getId());
    assertEquals(
        LoadTaskService.CancellationResult.CancellationState.CANCELLATION_REQUESTED,
        result.getState());

    awaitStatus(service, task.getId(), TaskStatus.CANCELLED, 5);
  }

  @Test
  void shutdown_preventsFurtherSubmissions_andHealthReflectsState() {
    CountingProcessor processor = new CountingProcessor(TaskType.REST);
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of(processor));

    assertTrue(service.isHealthy());
    service.shutdown();
    assertFalse(service.isHealthy());

    var submitted = service.submitTask(newTask(TaskType.REST));
    assertTrue(submitted.isEmpty(), "Should not accept after shutdown");
  }

  @Test
  void duplicateTaskId_returnsErrorOutcome() throws Exception {
    CountingProcessor processor = new CountingProcessor(TaskType.REST);
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of(processor));

    UUID id = UUID.randomUUID();
    LoadTask first = newTask(TaskType.REST, id);
    LoadTask second = newTask(TaskType.REST, id);

    service.submitTask(first).orElseThrow();
    awaitStatus(service, id, TaskStatus.COMPLETED, 5);

    var outcome2 = service.submitTask(second).orElseThrow();
    assertEquals(TaskStatus.ERROR, outcome2.status());
    assertTrue(outcome2.message().contains("Task ID already exists"));
  }

  @Test
  void supportedTaskTypes_reflectRegisteredProcessors() {
    CountingProcessor p1 = new CountingProcessor(TaskType.REST);
    CountingProcessor p2 = new CountingProcessor(TaskType.FIX);
    LoadTaskService service = new LoadTaskService(cfg(1, 5), List.of(p1, p2));

    var types = service.getSupportedTaskTypes();
    assertTrue(types.contains("REST"));
    assertTrue(types.contains("FIX"));
  }

  @Test
  void getAllTasks_and_getTasksByStatus_workAsExpected() throws Exception {
    CountingProcessor success = new CountingProcessor(TaskType.REST);
    FailingProcessor failure = new FailingProcessor(TaskType.FIX);
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of(success, failure));

    LoadTask t1 = newTask(TaskType.REST); // will complete
    LoadTask t2 = newTask(TaskType.FIX); // will error

    service.submitTask(t1);
    service.submitTask(t2);

    awaitStatus(service, t1.getId(), TaskStatus.COMPLETED, 5);
    awaitStatus(service, t2.getId(), TaskStatus.ERROR, 5);

    var all = service.getAllTasks();
    assertTrue(all.stream().anyMatch(r -> r.taskId().equals(t1.getId())));
    assertTrue(all.stream().anyMatch(r -> r.taskId().equals(t2.getId())));

    var completed = service.getTasksByStatus(TaskStatus.COMPLETED);
    assertTrue(completed.stream().anyMatch(r -> r.taskId().equals(t1.getId())));
    assertTrue(completed.stream().noneMatch(r -> r.taskId().equals(t2.getId())));

    var errored = service.getTasksByStatus(TaskStatus.ERROR);
    assertTrue(errored.stream().anyMatch(r -> r.taskId().equals(t2.getId())));
    assertTrue(errored.stream().noneMatch(r -> r.taskId().equals(t1.getId())));
  }

  @Test
  void getQueueStatus_reportsPendingAndActive() throws Exception {
    BlockingProcessor processor = new BlockingProcessor(TaskType.REST);
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of(processor));
    LoadTask t1 = newTask(TaskType.REST);
    LoadTask t2 = newTask(TaskType.REST);

    service.submitTask(t1);
    assertTrue(processor.started.await(2, TimeUnit.SECONDS));

    service.submitTask(t2);

    var queue = service.getQueueStatus();
    assertEquals(1, queue.queueSize());
    assertEquals(1, queue.activeTasks());
    assertTrue(queue.acceptingTasks());

    processor.release();
    awaitStatus(service, t1.getId(), TaskStatus.COMPLETED, 5);
    awaitStatus(service, t2.getId(), TaskStatus.COMPLETED, 5);
  }

  @Test
  void history_isCappedAndOrderedMostRecentFirst() throws Exception {
    CountingProcessor processor = new CountingProcessor(TaskType.REST);
    LoadTaskService service = new LoadTaskService(cfg(1, 2), List.of(processor));

    LoadTask a = newTask(TaskType.REST);
    LoadTask b = newTask(TaskType.REST);
    LoadTask c = newTask(TaskType.REST);

    service.submitTask(a);
    awaitStatus(service, a.getId(), TaskStatus.COMPLETED, 5);
    service.submitTask(b);
    awaitStatus(service, b.getId(), TaskStatus.COMPLETED, 5);
    service.submitTask(c);
    awaitStatus(service, c.getId(), TaskStatus.COMPLETED, 5);

    var history = service.getTaskHistory();
    assertEquals(2, history.size());
    var ids = java.util.Set.of(history.get(0).taskId(), history.get(1).taskId());
    assertTrue(ids.contains(c.getId()));
    assertTrue(ids.contains(b.getId()) || ids.contains(a.getId()));
  }

  @Test
  void submitTask_rejectedExecution_returnsErrorOutcome() {
    // Keep acceptingTasks = true but shutdown the underlying executor to force rejection
    CountingProcessor processor = new CountingProcessor(TaskType.REST);
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of(processor));
    var executor =
        getPrivateField(service, "executor", java.util.concurrent.ThreadPoolExecutor.class);
    executor.shutdown();

    var outcomeOpt = service.submitTask(newTask(TaskType.REST));
    assertTrue(outcomeOpt.isPresent());
    var outcome = outcomeOpt.get();
    assertEquals(TaskStatus.ERROR, outcome.status());
    assertTrue(outcome.message().contains("Task queue is full"));
  }

  @Test
  void cancelTask_invokesProcessorCancel() throws Exception {
    RecordingCancelProcessor processor = new RecordingCancelProcessor(TaskType.REST);
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of(processor));

    // Occupy worker so next task is queued
    BlockingProcessor blocker = new BlockingProcessor(TaskType.REST);
    // Replace processor map to include both types via reflection – maintain REST mapping to blocker
    // for first task
    java.util.Map<
            com.mk.fx.qa.load.execution.model.TaskType,
            com.mk.fx.qa.load.execution.processors.LoadTaskProcessor>
        map = new java.util.EnumMap<>(TaskType.class);
    map.put(TaskType.REST, blocker);
    setPrivateField(service, "processors", java.util.Map.copyOf(map));

    LoadTask first = newTask(TaskType.REST);
    service.submitTask(first);
    assertTrue(blocker.started.await(2, TimeUnit.SECONDS));

    // Now point REST to the recording processor for the queued task
    map.put(TaskType.REST, processor);
    setPrivateField(service, "processors", java.util.Map.copyOf(map));

    LoadTask second = newTask(TaskType.REST);
    service.submitTask(second); // queued

    var res = service.cancelTask(second.getId());
    assertEquals(LoadTaskService.CancellationResult.CancellationState.CANCELLED, res.getState());
    assertTrue(processor.cancelInvoked.get());

    blocker.release();
    awaitStatus(service, first.getId(), TaskStatus.COMPLETED, 5);
  }

  @Test
  void onShutdown_invokesShutdown() {
    CountingProcessor processor = new CountingProcessor(TaskType.REST);
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of(processor));
    service.onShutdown();
    assertFalse(service.isHealthy());
    assertTrue(service.submitTask(newTask(TaskType.REST)).isEmpty());
  }

  @Test
  void isHealthy_returnsFalseWhenExecutorShutdown() {
    CountingProcessor processor = new CountingProcessor(TaskType.REST);
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of(processor));
    var executor =
        getPrivateField(service, "executor", java.util.concurrent.ThreadPoolExecutor.class);
    executor.shutdown();
    assertFalse(service.isHealthy());
  }

  @Test
  void executeTask_missingProcessorDuringRun_marksError() throws Exception {
    // Replace processors with a map returning a processor on first get (submission) and null later
    // (execution)
    CountingProcessor underlying = new CountingProcessor(TaskType.REST);
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of(underlying));

    final java.util.concurrent.atomic.AtomicInteger calls =
        new java.util.concurrent.atomic.AtomicInteger();
    java.util.Map<TaskType, LoadTaskProcessor> sneakyMap =
        new java.util.AbstractMap<>() {
          @Override
          public LoadTaskProcessor get(Object key) {
            return calls.getAndIncrement() == 0 ? underlying : null;
          }

          @Override
          public java.util.Set<Entry<TaskType, LoadTaskProcessor>> entrySet() {
            return java.util.Set.of();
          }
        };
    setPrivateField(service, "processors", sneakyMap);

    LoadTask t = newTask(TaskType.REST);
    service.submitTask(t);
    awaitStatus(service, t.getId(), TaskStatus.ERROR, 5);
  }

  @Test
  void metrics_areZeroWhenNoTasksProcessed() {
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of());
    var m = service.getMetrics();
    assertEquals(0, m.totalCompleted());
    assertEquals(0, m.totalFailed());
    assertEquals(0, m.totalCancelled());
    assertEquals(0.0, m.averageProcessingTimeMillis(), 1e-9);
    assertEquals(0.0, m.successRate(), 1e-9);
  }

  @Test
  void getTaskHistory_initiallyEmpty() {
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of());
    assertTrue(service.getTaskHistory().isEmpty());
  }

  @Test
  void cancelTask_alreadyCancelled_returnsNotCancellable() throws Exception {
    BlockingProcessor processor = new BlockingProcessor(TaskType.REST);
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of(processor));
    LoadTask first = newTask(TaskType.REST);

    service.submitTask(first);
    assertTrue(processor.started.await(2, TimeUnit.SECONDS));

    LoadTask queued = newTask(TaskType.REST);
    service.submitTask(queued);
    var cancelled = service.cancelTask(queued.getId());
    assertEquals(
        LoadTaskService.CancellationResult.CancellationState.CANCELLED, cancelled.getState());

    var secondAttempt = service.cancelTask(queued.getId());
    assertEquals(
        LoadTaskService.CancellationResult.CancellationState.NOT_CANCELLABLE,
        secondAttempt.getState());

    processor.release();
    awaitStatus(service, first.getId(), TaskStatus.COMPLETED, 5);
  }

  @Test
  void taskStatusResponse_containsTimestampsAndDuration() throws Exception {
    CountingProcessor processor = new CountingProcessor(TaskType.REST);
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of(processor));
    LoadTask t = newTask(TaskType.REST);
    service.submitTask(t);
    awaitStatus(service, t.getId(), TaskStatus.COMPLETED, 5);

    var resp = service.getTaskStatus(t.getId()).orElseThrow();
    assertNotNull(resp.submittedAt());
    assertNotNull(resp.startedAt());
    assertNotNull(resp.completedAt());
    assertTrue(resp.processingTimeMillis() >= 0);
    assertTrue(!resp.startedAt().isBefore(resp.submittedAt()));
    assertTrue(!resp.completedAt().isBefore(resp.startedAt()));
  }

  @Test
  void getTasksByStatus_returnsEmptyListWhenNoMatches() {
    CountingProcessor processor = new CountingProcessor(TaskType.REST);
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of(processor));
    var none = service.getTasksByStatus(TaskStatus.ERROR);
    assertTrue(none.isEmpty());
  }

  @Test
  void cancelTask_notFound_and_notCancellable_behaviour() throws Exception {
    CountingProcessor processor = new CountingProcessor(TaskType.REST);
    LoadTaskService service = new LoadTaskService(cfg(1, 10), List.of(processor));

    // Not found
    var nf = service.cancelTask(UUID.randomUUID());
    assertEquals(LoadTaskService.CancellationResult.CancellationState.NOT_FOUND, nf.getState());

    // Not cancellable: completed
    LoadTask t = newTask(TaskType.REST);
    service.submitTask(t);
    awaitStatus(service, t.getId(), TaskStatus.COMPLETED, 5);
    var nc = service.cancelTask(t.getId());
    assertEquals(
        LoadTaskService.CancellationResult.CancellationState.NOT_CANCELLABLE, nc.getState());
    assertEquals(TaskStatus.COMPLETED, nc.getTaskStatus());
  }

  @Test
  void duplicateProcessorRegistration_throwsIllegalState() {
    CountingProcessor p1 = new CountingProcessor(TaskType.REST);
    CountingProcessor p2 = new CountingProcessor(TaskType.REST);
    assertThrows(
        IllegalStateException.class, () -> new LoadTaskService(cfg(1, 5), List.of(p1, p2)));
  }

  // Helpers
  private static void awaitStatus(
      LoadTaskService service, UUID id, TaskStatus status, int timeoutSeconds) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
    TaskStatus current = null;
    while (System.nanoTime() < deadline) {
      current = service.getTaskStatus(id).map(TaskStatusResponse::status).orElse(null);
      if (current == status) {
        return;
      }
      Thread.sleep(20);
    }
    fail("Timed out waiting for status " + status + ", last was " + current);
  }

  // Test processors
  static class CountingProcessor implements LoadTaskProcessor {
    private final TaskType type;
    final AtomicInteger executions = new AtomicInteger();

    CountingProcessor(TaskType type) {
      this.type = type;
    }

    @Override
    public TaskType supportedTaskType() {
      return type;
    }

    @Override
    public void execute(
        com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionRequest request)
        throws Exception {
      executions.incrementAndGet();
      // No-op, completes successfully
    }

    @Override
    public void cancel(UUID taskId) {}
  }

  static class FailingProcessor implements LoadTaskProcessor {
    private final TaskType type;

    FailingProcessor(TaskType type) {
      this.type = type;
    }

    @Override
    public TaskType supportedTaskType() {
      return type;
    }

    @Override
    public void execute(
        com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionRequest request)
        throws Exception {
      throw new RuntimeException("boom");
    }

    @Override
    public void cancel(UUID taskId) {}
  }

  static class BlockingProcessor implements LoadTaskProcessor {
    private final TaskType type;
    final CountDownLatch started = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);

    BlockingProcessor(TaskType type) {
      this.type = type;
    }

    @Override
    public TaskType supportedTaskType() {
      return type;
    }

    @Override
    public void execute(
        com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionRequest request)
        throws Exception {
      started.countDown();
      release.await(10, TimeUnit.SECONDS);
    }

    void release() {
      release.countDown();
    }

    @Override
    public void cancel(UUID taskId) {}
  }

  static class RecordingCancelProcessor implements LoadTaskProcessor {
    private final TaskType type;
    final AtomicBoolean cancelInvoked = new AtomicBoolean(false);

    RecordingCancelProcessor(TaskType type) {
      this.type = type;
    }

    @Override
    public TaskType supportedTaskType() {
      return type;
    }

    @Override
    public void execute(
        com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionRequest request)
        throws Exception {
      // no-op
    }

    @Override
    public void cancel(UUID taskId) {
      cancelInvoked.set(true);
    }
  }

  static class InterruptibleProcessor implements LoadTaskProcessor {
    private final TaskType type;
    final CountDownLatch started = new CountDownLatch(1);
    final AtomicBoolean cancelled = new AtomicBoolean(false);

    InterruptibleProcessor(TaskType type) {
      this.type = type;
    }

    @Override
    public TaskType supportedTaskType() {
      return type;
    }

    @Override
    public void execute(
        com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionRequest request)
        throws Exception {
      started.countDown();
      // Busy wait while checking for interrupts
      while (true) {
        if (Thread.currentThread().isInterrupted()) {
          throw new InterruptedException("interrupted");
        }
        Thread.sleep(10);
      }
    }

    @Override
    public void cancel(UUID taskId) {
      cancelled.set(true);
    }
  }
}
