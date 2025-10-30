package com.mk.fx.qa.load.execution.processors.rest;

import com.mk.fx.qa.load.execution.dto.common.ExecutionConfig;
import com.mk.fx.qa.load.execution.dto.common.LoadModelConfig;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionRequest;
import com.mk.fx.qa.load.execution.dto.rest.RestLoadTaskDefinition;
import com.mk.fx.qa.load.execution.executors.closed.ClosedLoadExecutor;
import com.mk.fx.qa.load.execution.executors.closed.ClosedLoadParameters;
import com.mk.fx.qa.load.execution.executors.closed.ClosedLoadResult;
import com.mk.fx.qa.load.execution.executors.open.OpenLoadExecutor;
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
import com.mk.fx.qa.load.execution.service.stratigies.ThinkTimeStrategy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mk.fx.qa.load.execution.utils.LoadUtils.parseDuration;
import static com.mk.fx.qa.load.execution.utils.LoadUtils.toSeconds;

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
        cancelled.set(false);

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

            long expectedIterations = Math.max(0, (long) Math.floor(duration.toMillis() / 1000.0 * rate));
            long expectedTotalRequests = expectedIterations * requestsPerIteration;
            Double expectedRps = rate * requestsPerIteration;

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
                                    expectedTotalRequests,
                                    expectedRps));


            metrics.start();
            metricsRegistry.register(taskId, metrics);
            var restMetrics = new RestProtocolMetrics();
            metrics.registerProtocolMetrics(restMetrics);

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
                                        executeAllScenarios(
                                                client, testSpec.getScenarios(), thinkTime, cancelled, metrics, restMetrics);
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                    }
                                });
            } finally {
                metrics.stopAndSummarise();
                metricsRegistry.complete(taskId, metrics.snapshotNow());
                var report = metrics.buildReport();
                metricsRegistry.saveReport(taskId, report);
                try {
                    log.info("Task {} run report:\n{}", taskId, JsonUtil.toJson(report));
                } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                    log.info("Task {} run report (unformatted): {}", taskId, report);
                }
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
        var users = Math.max(1, loadModel.getUsers() != null ? loadModel.getUsers() : 1);
        var iterations = Math.max(1, loadModel.getIterations() != null ? loadModel.getIterations() : 1);
        var rampUp =
                loadModel.getRampUp() != null ? parseDuration(loadModel.getRampUp()) : Duration.ZERO;
        var warmup =
                loadModel.getWarmup() != null ? parseDuration(loadModel.getWarmup()) : Duration.ZERO;
        var holdFor =
                loadModel.getHoldFor() != null ? parseDuration(loadModel.getHoldFor()) : Duration.ZERO;
        var requestsPerIteration = countRequestsPerIteration(testSpec.getScenarios());
        var expectedTotalRequests = (long) users * iterations * requestsPerIteration;

        var metrics =
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
                                expectedTotalRequests,
                                null));
        metrics.start();
        metricsRegistry.register(taskId, metrics);
        var restMetrics = new RestProtocolMetrics();
        metrics.registerProtocolMetrics(restMetrics);

        var parameters = new ClosedLoadParameters(users, iterations, warmup, rampUp, holdFor);
        ClosedLoadResult result = null;
        try {
            result =
                    ClosedLoadExecutor.execute(
                            taskId,
                            parameters,
                            cancelled::get,
                            (userIndex, iteration) -> {
                                if (iteration == 0) {
                                    metrics.recordUserStarted(userIndex);
                                }
                                metrics.recordUserProgress(userIndex, iteration);
                                executeAllScenarios(
                                        client, testSpec.getScenarios(), thinkTime, cancelled, metrics, restMetrics);
                                if (iteration == (iterations - 1)) {
                                    metrics.recordUserCompleted(userIndex, iterations);
                                }
                            });
        } finally {
            metrics.stopAndSummarise();
            metricsRegistry.complete(taskId, metrics.snapshotNow());
            var report = metrics.buildReport();
            metricsRegistry.saveReport(taskId, report);
            try {
                log.info("Task {} run report:\n{}", taskId, JsonUtil.toJson(report));
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                log.info("Task {} run report (unformatted): {}", taskId, report);
            }
        }
        log.info(
                "Task {} closed load finished: usersCompleted={}/{} cancelled={} holdExpired={}",
                taskId,
                result.completedUsers(),
                result.totalUsers(),
                result.cancelled(),
                result.holdExpired());
    }

    private int countRequestsPerIteration(List<RestLoadTaskDefinition.Scenario> scenarios) {
        if (scenarios == null) return 0;
        int total = 0;
        for (RestLoadTaskDefinition.Scenario s : scenarios) {
            if (s.getRequests() != null) {
                total += s.getRequests().size();
            }
        }
        return total;
    }

    private void executeAllScenarios(
            LoadHttpClient client,
            List<RestLoadTaskDefinition.Scenario> scenarios,
            ThinkTimeStrategy thinkTime,
            AtomicBoolean cancelled,
            LoadMetrics metrics,
            RestProtocolMetrics restMetrics)
            throws InterruptedException {
        if (scenarios == null || scenarios.isEmpty()) {
            throw new IllegalArgumentException("testSpec.scenarios must contain at least one scenario");
        }
        for (RestLoadTaskDefinition.Scenario scenario : scenarios) {
            checkCancelled(cancelled);
            if (scenario.getRequests() == null || scenario.getRequests().isEmpty()) {
                log.warn("Scenario {} has no requests", scenario.getName());
                continue;
            }
            for (RestLoadTaskDefinition.RequestSpec requestSpec : scenario.getRequests()) {
                checkCancelled(cancelled);
                var request = toHttpRequest(requestSpec);
                try {
                    var response = client.execute(request);
                    if (response.getStatusCode() >= 400) {
                        String category =
                                restMetrics.recordHttpFailure(
                                        response.getStatusCode(), request.getMethod().name(), request.getPath());
                        metrics.recordFailure(category, response.getResponseTimeMs());
                    } else {
                        metrics.recordRequestSuccess(response.getResponseTimeMs());
                        restMetrics.recordSuccess(
                                request.getMethod().name(),
                                request.getPath(),
                                response.getResponseTimeMs(),
                                response.getStatusCode());
                    }
                } catch (RuntimeException ex) {
                    metrics.recordRequestFailure(ex);
                    throw ex;
                }
                if (thinkTime.isEnabled()) {
                    thinkTime.pause(cancelled);
                }
            }
        }
    }

    private Request toHttpRequest(RestLoadTaskDefinition.RequestSpec requestSpec) {
        if (requestSpec.getMethod() == null || requestSpec.getMethod().isBlank()) {
            throw new IllegalArgumentException("Request method is required");
        }
        HttpMethod method;
        try {
            method = HttpMethod.valueOf(requestSpec.getMethod().toUpperCase());
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Unsupported HTTP method: " + requestSpec.getMethod(), ex);
        }
        var request = new Request();
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