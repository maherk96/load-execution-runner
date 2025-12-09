package com.mk.fx.qa.load.execution.processors.websocket;

import static com.mk.fx.qa.load.execution.utils.LoadUtils.parseDuration;
import static com.mk.fx.qa.load.execution.utils.LoadUtils.toSeconds;

import com.mk.fx.qa.load.execution.dto.common.ExecutionConfig;
import com.mk.fx.qa.load.execution.dto.common.LoadModelConfig;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionRequest;
import com.mk.fx.qa.load.execution.dto.websocket.WebSocketLoadTaskDefinition;
import com.mk.fx.qa.load.execution.executors.closed.ClosedLoadExecutor;
import com.mk.fx.qa.load.execution.executors.closed.ClosedLoadParameters;
import com.mk.fx.qa.load.execution.executors.closed.ClosedLoadResult;
import com.mk.fx.qa.load.execution.executors.open.OpenLoadExecutor;
import com.mk.fx.qa.load.execution.executors.open.OpenLoadParameters;
import com.mk.fx.qa.load.execution.executors.open.OpenLoadResult;
import com.mk.fx.qa.load.execution.metrics.LoadMetrics;
import com.mk.fx.qa.load.execution.metrics.LoadMetricsRegistry;
import com.mk.fx.qa.load.execution.metrics.TaskConfig;
import com.mk.fx.qa.load.execution.metrics.websocket.WebSocketProtocolMetrics;
import com.mk.fx.qa.load.execution.model.LoadModelType;
import com.mk.fx.qa.load.execution.model.TaskType;
import com.mk.fx.qa.load.execution.processors.LoadTaskProcessor;
import com.mk.fx.qa.load.execution.rest.JsonUtil;
import com.mk.fx.qa.load.execution.service.stratigies.ThinkTimeStrategy;
import com.mk.fx.qa.load.execution.ws.WsClient;
import com.mk.fx.qa.load.execution.ws.WsSendResult;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/** Processor for executing WebSocket load tasks. */
@Slf4j
@Component
public class WebSocketLoadTaskProcessor implements LoadTaskProcessor {

  public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 5_000;
  public static final int DEFAULT_MESSAGE_TIMEOUT_MS = 15_000;
  private static final Duration DEFAULT_OPEN_DURATION = Duration.ofMinutes(1);

  private final Map<UUID, AtomicBoolean> cancellationTokens = new ConcurrentHashMap<>();
  private final LoadMetricsRegistry metricsRegistry;

  public WebSocketLoadTaskProcessor(LoadMetricsRegistry metricsRegistry) {
    this.metricsRegistry = metricsRegistry;
  }

  @Override
  public TaskType supportedTaskType() {
    return TaskType.WEBSOCKET;
  }

  @Override
  public void execute(TaskSubmissionRequest request) throws Exception {
    Objects.requireNonNull(request, "Task request must not be null");
    UUID taskId = UUID.fromString(request.getTaskId());
    AtomicBoolean cancelled =
        cancellationTokens.computeIfAbsent(taskId, key -> new AtomicBoolean(false));
    cancelled.set(false);

    WebSocketLoadTaskDefinition definition =
        JsonUtil.mapper().convertValue(request.getData(), WebSocketLoadTaskDefinition.class);
    validateDefinition(definition);

    var testSpec = definition.getTestSpec();
    ExecutionConfig execution = definition.getExecution();
    ThinkTimeStrategy thinkTime = ThinkTimeStrategy.from(execution.getThinkTime());
    LoadModelConfig loadModel = execution.getLoadModel();

    try {
      switch (loadModel.getType()) {
        case OPEN -> executeOpenModel(taskId, testSpec, thinkTime, loadModel, cancelled);
        case CLOSED -> executeClosedModel(taskId, testSpec, thinkTime, loadModel, cancelled);
        default -> throw new IllegalArgumentException(
            "Unsupported load model type: " + loadModel.getType());
      }
    } finally {
      cancellationTokens.remove(taskId);
    }
  }

  @Override
  public void cancel(UUID taskId) {
    cancellationTokens.computeIfAbsent(taskId, key -> new AtomicBoolean(true)).set(true);
  }

  private void executeOpenModel(
      UUID taskId,
      WebSocketLoadTaskDefinition.WsTestSpec testSpec,
      ThinkTimeStrategy thinkTime,
      LoadModelConfig loadModel,
      AtomicBoolean cancelled)
      throws Exception {
    int maxConcurrent =
        Math.max(1, loadModel.getMaxConcurrent() != null ? loadModel.getMaxConcurrent() : 1);
    double rate = loadModel.getArrivalRatePerSec() != null ? loadModel.getArrivalRatePerSec() : 1.0;
    if (rate <= 0.0) {
      throw new IllegalArgumentException("OPEN load model requires arrivalRatePerSec > 0");
    }
    Duration duration =
        loadModel.getDuration() != null
            ? parseDuration(loadModel.getDuration())
            : DEFAULT_OPEN_DURATION;
    int msgsPerIteration = countMessagesPerIteration(testSpec.getScenarios());

    long expectedIterations = Math.max(0, (long) Math.floor(duration.toMillis() / 1000.0 * rate));
    long expectedTotalRequests = expectedIterations * msgsPerIteration;
    Double expectedRps = rate * msgsPerIteration;

    LoadMetrics metrics =
        new LoadMetrics(
            new TaskConfig(
                taskId.toString(),
                supportedTaskType().name(),
                testSpec.getGlobalConfig() != null ? testSpec.getGlobalConfig().getUrl() : "",
                LoadModelType.OPEN,
                null,
                null,
                null,
                null,
                null,
                rate,
                duration,
                msgsPerIteration,
                expectedTotalRequests,
                expectedRps));

    metrics.start();
    metricsRegistry.register(taskId, metrics);
    var wsMetrics = new WebSocketProtocolMetrics();
    metrics.registerProtocolMetrics(wsMetrics);

    var parameters = new OpenLoadParameters(rate, maxConcurrent, duration);
    OpenLoadResult result = null;
    try {
      result =
          OpenLoadExecutor.execute(
              taskId,
              parameters,
              cancelled::get,
              () -> {
                try {
                  executeAllScenarios(testSpec, thinkTime, cancelled, metrics, wsMetrics, null);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              });
    } finally {
      metrics.stopAndSummarise();
      boolean wasCancelled = result != null && result.cancelled();
      metrics.setCompletionContext(wasCancelled, null, null, null);
      metricsRegistry.complete(taskId, metrics.snapshotNow());
      var report = metrics.buildReport();
      metricsRegistry.saveReport(taskId, report);
      try {
        log.info("Task {} run report:\n{}", taskId, JsonUtil.toJson(report));
      } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
        log.info("Task {} run report (unformatted): {}", taskId, report);
      }
    }
  }

  private void executeClosedModel(
      UUID taskId,
      WebSocketLoadTaskDefinition.WsTestSpec testSpec,
      ThinkTimeStrategy thinkTime,
      LoadModelConfig loadModel,
      AtomicBoolean cancelled)
      throws Exception {
    var users = Math.max(1, loadModel.getUsers() != null ? loadModel.getUsers() : 1);
    var iterations = Math.max(1, loadModel.getIterations() != null ? loadModel.getIterations() : 1);
    var rampUp =
        loadModel.getRampUp() != null ? parseDuration(loadModel.getRampUp()) : Duration.ZERO;
    var warmup =
        loadModel.getWarmup() != null ? parseDuration(loadModel.getWarmup()) : Duration.ZERO;
    var holdFor =
        loadModel.getHoldFor() != null ? parseDuration(loadModel.getHoldFor()) : Duration.ZERO;
    var msgsPerIteration = countMessagesPerIteration(testSpec.getScenarios());
    var expectedTotalRequests = (long) users * iterations * msgsPerIteration;

    var metrics =
        new LoadMetrics(
            new TaskConfig(
                taskId.toString(),
                supportedTaskType().name(),
                testSpec.getGlobalConfig() != null ? testSpec.getGlobalConfig().getUrl() : "",
                LoadModelType.CLOSED,
                users,
                iterations,
                warmup,
                rampUp,
                holdFor,
                null,
                null,
                msgsPerIteration,
                expectedTotalRequests,
                null));
    metrics.start();
    metricsRegistry.register(taskId, metrics);
    var wsMetrics = new WebSocketProtocolMetrics();
    metrics.registerProtocolMetrics(wsMetrics);

    var parameters = new ClosedLoadParameters(users, iterations, warmup, rampUp, holdFor);
    ClosedLoadResult result = null;
    try {
      result =
          ClosedLoadExecutor.execute(
              taskId,
              parameters,
              cancelled::get,
              (userIndex, iteration) -> {
                if (iteration == 0) metrics.recordUserStarted(userIndex);
                metrics.recordUserProgress(userIndex, iteration);
                executeAllScenarios(testSpec, thinkTime, cancelled, metrics, wsMetrics, userIndex);
                if (iteration == (iterations - 1))
                  metrics.recordUserCompleted(userIndex, iterations);
              });
    } finally {
      metrics.stopAndSummarise();
      if (result != null) {
        metrics.setCompletionContext(
            result.cancelled(), result.holdExpired(), result.totalUsers(), result.completedUsers());
      }
      metricsRegistry.complete(taskId, metrics.snapshotNow());
      var report = metrics.buildReport();
      metricsRegistry.saveReport(taskId, report);
      try {
        log.info("Task {} run report:\n{}", taskId, JsonUtil.toJson(report));
      } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
        log.info("Task {} run report (unformatted): {}", taskId, report);
      }
    }
  }

  private int countMessagesPerIteration(List<WebSocketLoadTaskDefinition.Scenario> scenarios) {
    if (scenarios == null) return 0;
    int total = 0;
    for (WebSocketLoadTaskDefinition.Scenario s : scenarios) {
      if (s.getMessages() != null) total += s.getMessages().size();
    }
    return total;
  }

  private void executeAllScenarios(
      WebSocketLoadTaskDefinition.WsTestSpec testSpec,
      ThinkTimeStrategy thinkTime,
      AtomicBoolean cancelled,
      LoadMetrics metrics,
      WebSocketProtocolMetrics wsMetrics,
      Integer userIndex)
      throws InterruptedException {
    if (testSpec.getScenarios() == null || testSpec.getScenarios().isEmpty()) {
      throw new IllegalArgumentException("testSpec.scenarios must contain at least one scenario");
    }

    var global = testSpec.getGlobalConfig();
    int connTimeoutSec =
        toSeconds(
            global.getTimeouts() != null
                ? global.getTimeouts().getConnectionTimeoutMs()
                : DEFAULT_CONNECTION_TIMEOUT_MS);
    int msgTimeoutSec =
        toSeconds(
            global.getTimeouts() != null
                ? global.getTimeouts().getMessageTimeoutMs()
                : DEFAULT_MESSAGE_TIMEOUT_MS);

    for (WebSocketLoadTaskDefinition.Scenario scenario : testSpec.getScenarios()) {
      checkCancelled(cancelled);
      if (scenario.getMessages() == null || scenario.getMessages().isEmpty()) {
        log.warn("Scenario {} has no messages", scenario.getName());
        continue;
      }
      try (WsClient client =
          new WsClient(
              global.getUrl(),
              connTimeoutSec,
              msgTimeoutSec,
              global.getHeaders(),
              global.getSubprotocols(),
              global.getVars())) {
        client.connect();
        int idx = 0;
        for (WebSocketLoadTaskDefinition.MessageSpec msg : scenario.getMessages()) {
          checkCancelled(cancelled);
          String name = msg.getName() != null ? msg.getName() : scenario.getName() + "#" + idx++;
          try {
            WsSendResult res;
            if (msg.getJson() != null) {
              res = client.sendJson(msg.getJson(), msg.getAwaitPattern(), msg.getAwaitTimeoutMs());
            } else {
              String payload = msg.getText() != null ? msg.getText() : "";
              res = client.sendText(payload, msg.getAwaitPattern(), msg.getAwaitTimeoutMs());
            }

            if (res.success() && !res.timedOut()) {
              metrics.recordRequestSuccess(res.latencyMs());
              wsMetrics.recordSuccess(name, res.latencyMs(), userIndex);
            } else if (res.timedOut()) {
              metrics.recordFailure("WS_TIMEOUT", res.latencyMs());
              wsMetrics.recordTimeout(name, res.latencyMs());
            } else {
              metrics.recordFailure("WS_ERROR", res.latencyMs());
              wsMetrics.recordError(name, "WS_ERROR");
            }
          } catch (RuntimeException ex) {
            metrics.recordRequestFailure(ex);
            wsMetrics.recordError(name, "WS_EXCEPTION");
            throw ex;
          }

          if (thinkTime.isEnabled()) {
            thinkTime.pause(cancelled);
          }
        }
      }
    }
  }

  private void validateDefinition(WebSocketLoadTaskDefinition definition) {
    if (definition == null)
      throw new IllegalArgumentException("data.testSpec definition is required");
    if (definition.getTestSpec() == null)
      throw new IllegalArgumentException("testSpec payload is required");
    var global = definition.getTestSpec().getGlobalConfig();
    if (global == null || global.getUrl() == null || global.getUrl().isBlank()) {
      throw new IllegalArgumentException("globalConfig.url must be provided");
    }
    if (definition.getExecution() == null || definition.getExecution().getLoadModel() == null) {
      throw new IllegalArgumentException("execution.loadModel must be defined");
    }
    if (definition.getTestSpec().getScenarios() == null
        || definition.getTestSpec().getScenarios().isEmpty()) {
      throw new IllegalArgumentException("testSpec.scenarios must be provided");
    }
  }

  private void checkCancelled(AtomicBoolean cancelled) throws InterruptedException {
    if (cancelled.get() || Thread.currentThread().isInterrupted()) {
      throw new InterruptedException("Task cancelled");
    }
  }
}
