package com.mk.fx.qa.load.execution.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * HTTP client implementation for executing REST requests using a Request DTO. Supports variable
 * resolution, header management, and timeout configuration. This implementation does not include
 * retry logic.
 */
@Slf4j
public class LoadHttpClient implements AutoCloseable{

    /** Default request timeout in seconds. */
    private static final int DEFAULT_REQUEST_TIMEOUT_SECONDS = 30;

    /** The underlying Java HTTP client. */
    private final HttpClient httpClient;

    /** Global headers to be included in all requests. */
    private final Map<String, String> headers;

    /** Variables for resolving placeholders in request paths and queries. */
    private final Map<String, String> variables;

    /** Base URL for all requests. */
    private final String baseUrl;

    /** Timeout duration for requests. */
    private final Duration requestTimeout;

    /**
     * Constructs a RestHttpClient with default request timeout.
     *
     * @param baseUrl the base URL for all requests
     * @param connTimeOutSeconds connection timeout in seconds
     * @param headers global headers to include in all requests
     * @param variables variables for resolving placeholders in paths and queries
     */
    public LoadHttpClient(
            String baseUrl,
            int connTimeOutSeconds,
            Map<String, String> headers,
            Map<String, String> variables) {
        this(baseUrl, connTimeOutSeconds, DEFAULT_REQUEST_TIMEOUT_SECONDS, headers, variables);
    }


    /**
     * Constructs a RestHttpClient with a specified request timeout.
     *
     * @param baseUrl the base URL for all requests
     * @param connTimeOutSeconds connection timeout in seconds
     * @param requestTimeoutSeconds request timeout in seconds
     * @param headers global headers to include in all requests
     * @param variables variables for resolving placeholders in paths and queries
     */
    public LoadHttpClient(
            String baseUrl,
            int connTimeOutSeconds,
            int requestTimeoutSeconds,
            Map<String, String> headers,
            Map<String, String> variables) {

        this.baseUrl = validateAndNormalizeBaseUrl(baseUrl);
        this.requestTimeout = Duration.ofSeconds(requestTimeoutSeconds);


        this.httpClient = HttpClient.newBuilder()

                .connectTimeout(Duration.ofSeconds(connTimeOutSeconds))
                .build();

        this.headers = headers != null ? Map.copyOf(headers) : Map.of();
        this.variables = variables != null ? Map.copyOf(variables) : Map.of();

        log.debug("RestHttpClient initialized with base URL: {}", baseUrl);

        log.info(
                "RestHttpClient initialised - Base URL: {}, Connection timeout: {}s, Request timeout: {}s",
                baseUrl,
                connTimeOutSeconds,
                requestTimeoutSeconds);
    }


    /**
     * Executes a synchronous REST request.
     *
     * @param request the REST request to execute
     * @return the response data
     * @throws RuntimeException if an error occurs during execution
     */
    public RestResponseData execute(Request request) {
        Objects.requireNonNull(request, "Request cannot be null");

        try {
            var startTime = System.nanoTime();
            var httpRequest = buildHttpRequest(request);

            log.debug("Executing {} request to {}", request.getMethod(), httpRequest.uri());

            var response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            var duration = (System.nanoTime() - startTime) / 1_000_000;

            var result = buildResponseData(response, duration);

            log.debug("Request completed in {} ms with status {}", duration, response.statusCode());
            return result;

        } catch (HttpTimeoutException e) {
            log.error("Request timed out after {} seconds: {}", requestTimeout.getSeconds(), e.getMessage());
            throw new RuntimeException(
                    "Request timed out after " + requestTimeout.getSeconds() + "s: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error("Error executing request: {}", e.getMessage());
            throw new RuntimeException("Error executing request: " + e.getMessage(), e);
        }
    }

    /**
     * Executes an asynchronous REST request.
     *
     * @param request the REST request to execute
     * @return a CompletableFuture containing the response data
     */
    public CompletableFuture<RestResponseData> executeAsync(Request request) {
        Objects.requireNonNull(request, "Request cannot be null");

        var startTime = System.nanoTime();

        try {
            var httpRequest = buildHttpRequest(request);

            log.debug("Executing async {} request to {}", request.getMethod(), httpRequest.uri());

            return httpClient
                    .sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
                    .thenApply(response -> {
                        var duration = (System.nanoTime() - startTime) / 1_000_000;
                        var result = buildResponseData(response, duration);
                        log.info("Async request completed in {} ms with status {}", duration, response.statusCode());
                        return result;
                    })
                    .exceptionally(throwable -> {
                        throw new RuntimeException("Async request failed: " + throwable.getMessage(), throwable);
                    });

        } catch (Exception e) {
            return CompletableFuture.failedFuture(
                    new RuntimeException("Error building async request: " + e.getMessage(), e));
        }
    }

    /**
     * Builds an HTTP request from the given RestRequest.
     *
     * @param request the RestRequest to build
     * @return the constructed HttpRequest
     * @throws RuntimeException if an error occurs while building the request
     */
    private HttpRequest buildHttpRequest(Request request) {
        try {
            var resolvedPath = resolveVars(request.getPath(), variables);
            var url = baseUrl + (resolvedPath != null ? resolvedPath : "");

            if (request.getQuery() != null && !request.getQuery().isEmpty()) {
                var queryStr = buildQueryString(request.getQuery(), variables);
                url += "?" + queryStr;
            }

            var requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(requestTimeout);

            // global headers
            headers.forEach(requestBuilder::header);

            // request-specific headers override
            if (request.getHeaders() != null) {
                request.getHeaders().forEach(requestBuilder::setHeader);
            }

            if (request.getBody() != null) {
                try {
                    var jsonBody = JsonUtil.toJson(request.getBody());
                    requestBuilder
                            .method(request.getMethod().name(), HttpRequest.BodyPublishers.ofString(jsonBody))
                            .header("Content-Type", "application/json");
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Failed to serialize request body: " + e.getMessage(), e);
                }
            } else {
                requestBuilder.method(request.getMethod().name(), HttpRequest.BodyPublishers.noBody());
            }

            return requestBuilder.build();

        } catch (Exception e) {
            throw new RuntimeException("Error building HTTP request: " + e.getMessage(), e);
        }
    }

    /**
     * Builds a RestResponseData object from the HTTP response.
     *
     * @param response the HTTP response
     * @param durationMs the duration of the request in milliseconds
     * @return the constructed RestResponseData
     */
    private RestResponseData buildResponseData(HttpResponse<String> response, long durationMs) {
        var result = new RestResponseData();
        result.setStatusCode(response.statusCode());
        result.setHeaders(
                response.headers().map().entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> String.join(",", e.getValue()))));
        result.setBody(response.body());
        result.setResponseTimeMs(durationMs);
        return result;
    }

    /**
     * Builds a query string from the given query parameters and variables.
     *
     * @param query the query parameters
     * @param variables the variables for resolving placeholders
     * @return the constructed query string
     */
    private String buildQueryString(Map<String, String> query, Map<String, String> variables) {
        return query.entrySet().stream()
                .filter(e -> e.getKey() != null && e.getValue() != null)
                .map(e ->
                        encode(resolveVars(e.getKey(), variables))
                                + "="
                                + encode(resolveVars(e.getValue(), variables)))
                .collect(Collectors.joining("&"));
    }

    /**
     * Encodes a string for use in a URL.
     *
     * @param value the string to encode
     * @return the encoded string
     */
    private String encode(String value) {
        return value != null ? URLEncoder.encode(value, StandardCharsets.UTF_8) : "";
    }

    /**
     * Resolves variables in the given text using the provided variables map.
     *
     * @param text the text containing placeholders
     * @param variables the variables map
     * @return the text with placeholders replaced by variable values
     */
    private String resolveVars(String text, Map<String, String> variables) {
        if (text == null || text.isEmpty() || variables.isEmpty()) {
            return text;
        }

        var result = text;
        for (var entry : variables.entrySet()) {
            var placeholder = "{{" + entry.getKey() + "}}";
            if (result.contains(placeholder)) {
                result = result.replace(placeholder, entry.getValue());
            }
        }
        return result;
    }

    /**
     * Validates and normalizes the base URL.
     *
     * @param baseUrl the base URL to validate
     * @return the normalized base URL
     * @throws IllegalArgumentException if the base URL is null or empty
     */
    private String validateAndNormalizeBaseUrl(String baseUrl) {
        Objects.requireNonNull(baseUrl, "Base URL cannot be null");
        var trimmed = baseUrl.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("Base URL cannot be empty");
        }
        return trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
    }


    @Override
    public void close() throws Exception {

    }
}