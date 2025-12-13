package com.mk.fx.qa.load.execution.processors.rest;

import static com.mk.fx.qa.load.execution.utils.LoadUtils.parseDuration;
import static com.mk.fx.qa.load.execution.utils.LoadUtils.toSeconds;

import com.mk.fx.qa.load.execution.dto.common.ClosedModelOptionsConfig;
import com.mk.fx.qa.load.execution.dto.common.ExecutionConfig;
import com.mk.fx.qa.load.execution.dto.common.LoadModelConfig;
import com.mk.fx.qa.load.execution.dto.common.OpenModelOptionsConfig;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionRequest;
import com.mk.fx.qa.load.execution.dto.rest.RestLoadTaskDefinition;
import com.mk.fx.qa.load.execution.executors.closed.ClosedLoadExecutor;
import com.mk.fx.qa.load.execution.executors.closed.ClosedLoadOptions;
import com.mk.fx.qa.load.execution.executors.closed.ClosedLoadParameters;
import com.mk.fx.qa.load.execution.executors.closed.ClosedLoadResult;
import com.mk.fx.qa.load.execution.executors.open.OpenLoadExecutor;
import com.mk.fx.qa.load.execution.executors.open.OpenLoadOptions;
import com.mk.fx.qa.load.execution.executors.open.OpenLoadParameters;
import com.mk.fx.qa.load.execution.executors.open.OpenLoadResult;
import com.mk.fx.qa.load.execution.metrics.LoadMetrics;
import com.mk.fx.qa.load.execution.metrics.LoadMetricsRegistry;
import com.mk.fx.qa.load.execution.metrics.TaskConfig;
import com.mk.fx.qa.load.execution.metrics.rest.RestProtocolMetrics;
import com.mk.fx.qa.load.execution.model.LoadModelType;
import com.mk.fx.qa.load.execution.model.TaskType;
import com.mk.fx.qa.load.execution.processors.LoadTaskProcessor;
import com.mk.fx.qa.load.execution.rest.HttpMethod;
import com.mk.fx.qa.load.execution.rest.JsonUtil;
import com.mk.fx.qa.load.execution.rest.LoadHttpClient;
import com.mk.fx.qa.load.execution.rest.Request;
import com.mk.fx.qa.load.execution.rest.RestResponseData;
import com.mk.fx.qa.load.execution.service.stratigies.ThinkTimeStrategy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Processor for executing REST load tasks based on the provided task definition and execution
 * configuration.
 *
 * <p>NOTE: This processor currently uses a GLOBAL, thread-safe HTTP client shared across all
 * executions within a task. Future extensions may introduce PER_USER client scope to support
 * session isolation and per-user variables.
 */
@Slf4j
@Component
public class RestLoadTaskProcessor implements LoadTaskProcessor {

  private static final Duration DEFAULT_OPEN_DURATION = Duration.ofMinutes(1);
  public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 5_000;
  public static final int DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

  private final Map<UUID, AtomicBoolean> cancellationTokens = new ConcurrentHashMap<>();
  private final LoadMetricsRegistry metricsRegistry;

  public RestLoadTaskProcessor(LoadMetricsRegistry metricsRegistry) {
    this.metricsRegistry = metricsRegistry;
  }

  @Override
  public TaskType supportedTaskType() {
    return TaskType.REST;
  }

  @Override
  public void execute(TaskSubmissionRequest request) throws Exception {
    Objects.requireNonNull(request, "Task request must not be null");

    UUID taskId = UUID.fromString(request.getTaskId());
    AtomicBoolean cancelled =
            cancellationTokens.computeIfAbsent(taskId, key -> new AtomicBoolean(false));

    // Reset token for a fresh run (if caller reuses taskId). If you want strict single-run IDs,
    // replace this with a guard that rejects re-execution.
    cancelled.set(false);

    if (cancelled.get()) {
      throw new InterruptedException("Task " + taskId + " was cancelled before execution started");
    }

    RestLoadTaskDefinition definition =
            JsonUtil.mapper().convertValue(request.getData(), RestLoadTaskDefinition.class);
    validateDefinition(definition);

    RestLoadTaskDefinition.RestTestSpec testSpec = definition.getTestSpec();
    ExecutionConfig execution = definition.getExecution();
    ThinkTimeStrategy thinkTime = ThinkTimeStrategy.from(execution.getThinkTime());
    LoadModelConfig loadModel = execution.getLoadModel();

    try (LoadHttpClient client = buildClient(testSpec)) {
      switch (loadModel.getType()) {
        case OPEN -> executeOpenModel(taskId, client, testSpec, thinkTime, loadModel, cancelled);
        case CLOSED -> executeClosedModel(taskId, client, testSpec, thinkTime, loadModel, cancelled);
        default ->
                throw new IllegalArgumentException(
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

  private LoadHttpClient buildClient(RestLoadTaskDefinition.RestTestSpec testSpec) {
    RestLoadTaskDefinition.GlobalConfig globalConfig = testSpec.getGlobalConfig();
    if (globalConfig == null
            || globalConfig.getBaseUrl() == null
            || globalConfig.getBaseUrl().isBlank()) {
      throw new IllegalArgumentException("URL must be provided");
    }

    RestLoadTaskDefinition.TimeoutConfig timeouts = globalConfig.getTimeouts();
    int connTimeoutSeconds =
            toSeconds(
                    timeouts != null ? timeouts.getConnectionTimeoutMs() : DEFAULT_CONNECTION_TIMEOUT_MS);
    int requestTimeoutSeconds =
            toSeconds(timeouts != null ? timeouts.getRequestTimeoutMs() : DEFAULT_REQUEST_TIMEOUT_MS);

    Map<String, String> headers = globalConfig.getHeaders();
    Map<String, String> vars = globalConfig.getVars();

    return new LoadHttpClient(
            globalConfig.getBaseUrl(), connTimeoutSeconds, requestTimeoutSeconds, headers, vars);
  }

  private void executeOpenModel(
          UUID taskId,
          LoadHttpClient client,
          RestLoadTaskDefinition.RestTestSpec testSpec,
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

    int requestsPerIteration = countRequestsPerIteration(testSpec.getScenarios());

    // These values are theoretical/configured. Real throughput depends on execution time,
    // think time, errors, and cancellation.
    long theoreticalIterations =
            Math.max(0, (long) Math.floor(duration.toMillis() / 1000.0 * rate));
    long theoreticalTotalRequests = theoreticalIterations * requestsPerIteration;
    Double configuredRps = rate * requestsPerIteration;

    LoadMetrics metrics =
            new LoadMetrics(
                    new TaskConfig(
                            taskId.toString(),
                            supportedTaskType().name(),
                            testSpec.getGlobalConfig() != null ? testSpec.getGlobalConfig().getBaseUrl() : "",
                            LoadModelType.OPEN,
                            null,
                            null,
                            null,
                            null,
                            null,
                            rate,
                            duration,
                            requestsPerIteration,
                            theoreticalTotalRequests,
                            configuredRps));

    metrics.start();
    metricsRegistry.register(taskId, metrics);

    RestProtocolMetrics restMetrics = new RestProtocolMetrics();
    metrics.registerProtocolMetrics(restMetrics);

    OpenLoadParameters parameters = new OpenLoadParameters(rate, maxConcurrent, duration);
    OpenLoadOptions openOptions = toOpenOptions(loadModel.getOpenOptions());

    OpenLoadResult result = null;
    try {
      result =
              OpenLoadExecutor.execute(
                      taskId,
                      parameters,
                      cancelled::get,
                      () -> {
                        // OPEN model: one "iteration" == one full scenario execution.
                        try {
                          executeAllScenarios(
                                  client,
                                  testSpec.getScenarios(),
                                  thinkTime,
                                  cancelled,
                                  metrics,
                                  restMetrics,
                                  null);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }
                      },
                      openOptions,
                      null);
    } finally {
      boolean wasCancelled =
              (result != null && result.cancelled())
                      || cancelled.get()
                      || Thread.currentThread().isInterrupted();

      completeMetrics(taskId, metrics, wasCancelled, null, null, null);
    }

    if (result != null) {
      log.info(
              "Open load completed: launched={} completed={} cancelled={} rate={} maxConcurrent={} duration={}",
              result.launched(),
              result.completed(),
              result.cancelled(),
              rate,
              maxConcurrent,
              duration);
    }
  }

  private void executeClosedModel(
          UUID taskId,
          LoadHttpClient client,
          RestLoadTaskDefinition.RestTestSpec testSpec,
          ThinkTimeStrategy thinkTime,
          LoadModelConfig loadModel,
          AtomicBoolean cancelled)
          throws Exception {

    int users = Math.max(1, loadModel.getUsers() != null ? loadModel.getUsers() : 1);
    int iterations = Math.max(1, loadModel.getIterations() != null ? loadModel.getIterations() : 1);

    Duration rampUp =
            loadModel.getRampUp() != null ? parseDuration(loadModel.getRampUp()) : Duration.ZERO;
    Duration warmup =
            loadModel.getWarmup() != null ? parseDuration(loadModel.getWarmup()) : Duration.ZERO;
    Duration holdFor =
            loadModel.getHoldFor() != null ? parseDuration(loadModel.getHoldFor()) : Duration.ZERO;

    int requestsPerIteration = countRequestsPerIteration(testSpec.getScenarios());
    long theoreticalTotalRequests = (long) users * iterations * requestsPerIteration;

    LoadMetrics metrics =
            new LoadMetrics(
                    new TaskConfig(
                            taskId.toString(),
                            supportedTaskType().name(),
                            testSpec.getGlobalConfig() != null ? testSpec.getGlobalConfig().getBaseUrl() : "",
                            LoadModelType.CLOSED,
                            users,
                            iterations,
                            warmup,
                            rampUp,
                            holdFor,
                            null,
                            null,
                            requestsPerIteration,
                            theoreticalTotalRequests,
                            null));

    metrics.start();
    metricsRegistry.register(taskId, metrics);

    RestProtocolMetrics restMetrics = new RestProtocolMetrics();
    metrics.registerProtocolMetrics(restMetrics);

    ClosedLoadParameters parameters = new ClosedLoadParameters(users, iterations, warmup, rampUp, holdFor);
    ClosedLoadOptions closedOptions = toClosedOptions(loadModel.getClosedOptions(), parameters);

    ClosedLoadResult result = null;
    try {
      result =
              ClosedLoadExecutor.execute(
                      taskId,
                      parameters,
                      cancelled::get,
                      (userIndex, iteration) -> {
                        // CLOSED model: each virtual user executes iterations sequentially.
                        if (iteration == 0) {
                          metrics.recordUserStarted(userIndex);
                        }
                        metrics.recordUserProgress(userIndex, iteration);

                        executeAllScenarios(
                                client,
                                testSpec.getScenarios(),
                                thinkTime,
                                cancelled,
                                metrics,
                                restMetrics,
                                userIndex);

                        if (iteration == (iterations - 1)) {
                          metrics.recordUserCompleted(userIndex, iterations);
                        }
                      },
                      closedOptions);
    } finally {
      if (result != null) {
        completeMetrics(
                taskId,
                metrics,
                result.cancelled(),
                result.holdExpired(),
                result.totalUsers(),
                result.completedUsers());
      } else {
        boolean wasCancelled = cancelled.get() || Thread.currentThread().isInterrupted();
        completeMetrics(taskId, metrics, wasCancelled, null, null, null);
      }
    }

    if (result != null) {
      log.info(
              "Task {} closed load finished: usersCompleted={}/{} cancelled={} holdExpired={}",
              taskId,
              result.completedUsers(),
              result.totalUsers(),
              result.cancelled(),
              result.holdExpired());
    }
  }

  private void completeMetrics(
          UUID taskId,
          LoadMetrics metrics,
          boolean cancelled,
          Boolean holdExpired,
          Integer totalUsers,
          Integer completedUsers) {

    metrics.stopAndSummarise();
    metrics.setCompletionContext(cancelled, holdExpired, totalUsers, completedUsers);

    metricsRegistry.complete(taskId, metrics.snapshotNow());

    var report = metrics.buildReport();
    metricsRegistry.saveReport(taskId, report);

    try {
      log.info("Task {} run report:\n{}", taskId, JsonUtil.toJson(report));
    } catch (Exception e) {
      log.info("Task {} run report (unformatted): {}", taskId, report);
    }
  }

  private OpenLoadOptions toOpenOptions(OpenModelOptionsConfig cfg) {
    if (cfg == null) {
      return OpenLoadOptions.defaults();
    }

    OpenLoadOptions.SaturationPolicy saturation = OpenLoadOptions.SaturationPolicy.DROP;
    if (cfg.getSaturationPolicy() != null) {
      try {
        saturation =
                OpenLoadOptions.SaturationPolicy.valueOf(cfg.getSaturationPolicy().toUpperCase());
      } catch (IllegalArgumentException ignored) {
        log.warn("Unknown open.saturationPolicy '{}', using DROP", cfg.getSaturationPolicy());
      }
    }

    OpenLoadOptions.IterationFailurePolicy failure = OpenLoadOptions.IterationFailurePolicy.CANCEL_TEST;
    if (cfg.getIterationFailure() != null) {
      try {
        failure =
                OpenLoadOptions.IterationFailurePolicy.valueOf(cfg.getIterationFailure().toUpperCase());
      } catch (IllegalArgumentException ignored) {
        log.warn(
                "Unknown open.iterationFailure '{}', using CANCEL_TEST", cfg.getIterationFailure());
      }
    }

    return new OpenLoadOptions(saturation, failure);
  }

  private ClosedLoadOptions toClosedOptions(ClosedModelOptionsConfig cfg, ClosedLoadParameters params) {
    if (cfg == null) {
      return ClosedLoadOptions.defaults(params);
    }

    ClosedLoadOptions.StopMode stopMode = ClosedLoadOptions.defaults(params).stopMode();
    if (cfg.getStopMode() != null) {
      try {
        stopMode = ClosedLoadOptions.StopMode.valueOf(cfg.getStopMode().toUpperCase());
      } catch (IllegalArgumentException ignored) {
        log.warn("Unknown closed.stopMode '{}', using {}", cfg.getStopMode(), stopMode);
      }
    }

    ClosedLoadOptions.FailureMode failure = ClosedLoadOptions.FailureMode.STOP_USER;
    if (cfg.getFailureMode() != null) {
      try {
        failure = ClosedLoadOptions.FailureMode.valueOf(cfg.getFailureMode().toUpperCase());
      } catch (IllegalArgumentException ignored) {
        log.warn("Unknown closed.failureMode '{}', using STOP_USER", cfg.getFailureMode());
      }
    }

    ClosedLoadOptions.ShutdownPolicy shutdown = ClosedLoadOptions.ShutdownPolicy.FORCEFUL_ON_TIMEOUT;
    if (cfg.getShutdownPolicy() != null) {
      try {
        shutdown = ClosedLoadOptions.ShutdownPolicy.valueOf(cfg.getShutdownPolicy().toUpperCase());
      } catch (IllegalArgumentException ignored) {
        log.warn(
                "Unknown closed.shutdownPolicy '{}', using FORCEFUL_ON_TIMEOUT",
                cfg.getShutdownPolicy());
      }
    }

    return new ClosedLoadOptions(stopMode, failure, shutdown);
  }

  private int countRequestsPerIteration(List<RestLoadTaskDefinition.Scenario> scenarios) {
    if (scenarios == null) return 0;
    int total = 0;
    for (RestLoadTaskDefinition.Scenario s : scenarios) {
      if (s != null && s.getRequests() != null) {
        total += s.getRequests().size();
      }
    }
    return total;
  }

  /**
   * Executes one full iteration (all scenarios).
   *
   * <p>FAILURE SEMANTICS:
   * <ul>
   *   <li>Request failure => iteration failure</li>
   *   <li>Iteration failure is propagated to the executor so it can apply policy:</li>
   *   <li>OPEN: {@link OpenLoadOptions.IterationFailurePolicy}</li>
   *   <li>CLOSED: {@link ClosedLoadOptions.FailureMode}</li>
   * </ul>
   *
   * <p>This method MUST throw on failure so executors can enforce iteration/test policy correctly.
   */
  private void executeAllScenarios(
          LoadHttpClient client,
          List<RestLoadTaskDefinition.Scenario> scenarios,
          ThinkTimeStrategy thinkTime,
          AtomicBoolean cancelled,
          LoadMetrics metrics,
          RestProtocolMetrics restMetrics,
          Integer userIndex)
          throws InterruptedException {

    if (scenarios == null || scenarios.isEmpty()) {
      throw new IllegalArgumentException("testSpec.scenarios must contain at least one scenario");
    }

    for (RestLoadTaskDefinition.Scenario scenario : scenarios) {
      checkCancelled(cancelled);

      if (scenario == null) {
        continue;
      }
      if (scenario.getRequests() == null || scenario.getRequests().isEmpty()) {
        log.warn("Scenario {} has no requests", scenario.getName());
        continue;
      }

      for (RestLoadTaskDefinition.RequestSpec requestSpec : scenario.getRequests()) {
        checkCancelled(cancelled);

        Request request = toHttpRequest(requestSpec);

        long startNs = System.nanoTime();
        try {
          RestResponseData response = client.execute(request);

          if (response.getStatusCode() >= 400) {
            String category =
                    restMetrics.recordHttpFailure(
                            response.getStatusCode(),
                            request.getMethod().name(),
                            request.getPath(),
                            userIndex);

            metrics.recordFailure(category, response.getResponseTimeMs());
          } else {
            recordSuccess(metrics, restMetrics, request, response, userIndex);
          }
        } catch (RuntimeException ex) {
          long elapsedMs = Math.max(0L, (System.nanoTime() - startNs) / 1_000_000);

          metrics.recordRequestFailure(ex, elapsedMs);
          restMetrics.recordException(
                  request.getMethod().name(),
                  request.getPath(),
                  classifyException(ex),
                  userIndex);

          // Propagate to executor so iteration/test failure policy can be applied.
          throw ex;
        }

        if (thinkTime.isEnabled()) {
          thinkTime.pause(cancelled);
        }
      }
    }
  }

  private void recordSuccess(
          LoadMetrics metrics,
          RestProtocolMetrics restMetrics,
          Request request,
          RestResponseData response,
          Integer userIndex) {

    metrics.recordRequestSuccess(response.getResponseTimeMs());
    restMetrics.recordSuccess(
            request.getMethod().name(),
            request.getPath(),
            response.getResponseTimeMs(),
            response.getStatusCode(),
            userIndex);
  }

  private String classifyException(Throwable t) {
    if (t == null) return "EXCEPTION";

    Throwable root = t;
    while (root.getCause() != null) {
      root = root.getCause();
    }

    String name = root.getClass().getSimpleName();
    return switch (name) {
      case "ConnectException" -> "CONNECTION_REFUSED";
      case "SocketTimeoutException" -> "SOCKET_TIMEOUT";
      case "UnknownHostException" -> "UNKNOWN_HOST";
      case "SSLException" -> "SSL_ERROR";
      case "HttpTimeoutException" -> "HTTP_TIMEOUT";
      default -> (name == null || name.isBlank()) ? "EXCEPTION" : name;
    };
  }

  private Request toHttpRequest(RestLoadTaskDefinition.RequestSpec requestSpec) {
    if (requestSpec == null) {
      throw new IllegalArgumentException("Request spec is required");
    }
    if (requestSpec.getMethod() == null || requestSpec.getMethod().isBlank()) {
      throw new IllegalArgumentException("Request method is required");
    }
    if (requestSpec.getPath() == null || requestSpec.getPath().isBlank()) {
      throw new IllegalArgumentException("Request path is required");
    }

    HttpMethod method;
    try {
      method = HttpMethod.valueOf(requestSpec.getMethod().toUpperCase());
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException("Unsupported HTTP method: " + requestSpec.getMethod(), ex);
    }

    Request request = new Request();
    request.setMethod(method);
    request.setPath(requestSpec.getPath());
    request.setHeaders(requestSpec.getHeaders());
    request.setQuery(requestSpec.getQuery());
    request.setBody(requestSpec.getBody());
    return request;
  }

  private void validateDefinition(RestLoadTaskDefinition definition) {
    if (definition == null) {
      throw new IllegalArgumentException("data.testSpec definition is required");
    }
    if (definition.getTestSpec() == null) {
      throw new IllegalArgumentException("testSpec payload is required");
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