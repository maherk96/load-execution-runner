package com.mk.fx.qa.load.execution.ws;

import com.mk.fx.qa.load.execution.rest.JsonUtil;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;

/** Simple WebSocket client tailored for load testing use cases. */
@Slf4j
public class WsClient implements AutoCloseable {

  private final String url;
  private final Map<String, String> headers;
  private final Map<String, String> vars;
  private final List<String> subprotocols;
  private final Duration connectTimeout;
  private final Duration defaultMessageTimeout;

  private final HttpClient httpClient;
  private volatile WebSocket webSocket;
  private final AtomicBoolean open = new AtomicBoolean(false);
  private final LinkedBlockingQueue<String> inbound = new LinkedBlockingQueue<>();
  private final AtomicReference<Throwable> lastError = new AtomicReference<>();
  private final ReentrantLock sendLock = new ReentrantLock();

  public WsClient(
      String url,
      int connectTimeoutSeconds,
      int messageTimeoutSeconds,
      Map<String, String> headers,
      List<String> subprotocols,
      Map<String, String> vars) {
    this.url = validateAndResolveUrl(Objects.requireNonNull(url));
    this.headers = headers != null ? Map.copyOf(headers) : Map.of();
    this.vars = vars != null ? Map.copyOf(vars) : Map.of();
    this.subprotocols = subprotocols != null ? List.copyOf(subprotocols) : List.of();
    this.connectTimeout = Duration.ofSeconds(Math.max(1, connectTimeoutSeconds));
    this.defaultMessageTimeout = Duration.ofSeconds(Math.max(1, messageTimeoutSeconds));
    this.httpClient = HttpClient.newBuilder().connectTimeout(this.connectTimeout).build();
  }

  public void connect() {
    if (open.get()) return;
    try {
      var builder = httpClient.newWebSocketBuilder().connectTimeout(connectTimeout);
      if (!subprotocols.isEmpty()) builder.subprotocols(String.join(",", subprotocols));
      headers.forEach(builder::header);
      var resolved = resolveVars(url, vars);
      CompletableFuture<WebSocket> fut =
          builder.buildAsync(URI.create(resolved), new ListenerImpl(inbound, lastError, open));
      this.webSocket = fut.get(connectTimeout.toMillis(), TimeUnit.MILLISECONDS);
      open.set(true);
    } catch (Exception e) {
      throw new RuntimeException("WebSocket connect failed: " + e.getMessage(), e);
    }
  }

  public WsSendResult sendText(String text, String awaitPattern, Integer awaitTimeoutMs) {
    ensureOpen();
    long start = System.nanoTime();
    try {
      sendLock.lock();
      CompletableFuture<WebSocket> cf = webSocket.sendText(resolveVars(text, vars), true);
      cf.join();
    } catch (Throwable t) {
      lastError.set(t);
      throw new RuntimeException("WebSocket send failed: " + t.getMessage(), t);
    } finally {
      sendLock.unlock();
    }

    if (awaitPattern == null || awaitPattern.isBlank()) {
      long tookMs = (System.nanoTime() - start) / 1_000_000;
      return new WsSendResult(true, false, tookMs);
    }

    long timeout =
        awaitTimeoutMs != null && awaitTimeoutMs > 0
            ? awaitTimeoutMs
            : defaultMessageTimeout.toMillis();
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
    try {
      String pattern = resolveVars(awaitPattern, vars);
      while (System.nanoTime() < deadline) {
        if (lastError.get() != null) {
          throw new RuntimeException("WebSocket error while awaiting response", lastError.get());
        }
        String msg = inbound.poll(50, TimeUnit.MILLISECONDS);
        if (msg != null && msg.contains(pattern)) {
          long tookMs = (System.nanoTime() - start) / 1_000_000;
          return new WsSendResult(true, false, tookMs);
        }
      }
      long tookMs = (System.nanoTime() - start) / 1_000_000;
      return new WsSendResult(false, true, tookMs);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while awaiting response", ie);
    }
  }

  public WsSendResult sendJson(Object json, String awaitPattern, Integer awaitTimeoutMs) {
    try {
      String body = JsonUtil.toJson(json);
      return sendText(body, awaitPattern, awaitTimeoutMs);
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      throw new RuntimeException("Failed to serialize JSON: " + e.getMessage(), e);
    }
  }

  private void ensureOpen() {
    if (!open.get() || webSocket == null) {
      connect();
    }
  }

  @Override
  public void close() {
    try {
      if (webSocket != null) {
        webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "bye").join();
      }
    } catch (Throwable t) {
      log.warn("Error during WebSocket close: {}", t.getMessage());
    } finally {
      open.set(false);
    }
  }

  private static String validateAndResolveUrl(String url) {
    String u = url.trim();
    if (u.isEmpty()) throw new IllegalArgumentException("WebSocket URL cannot be empty");
    if (!(u.startsWith("ws://") || u.startsWith("wss://"))) {
      throw new IllegalArgumentException("WebSocket URL must start with ws:// or wss://");
    }
    return u;
  }

  private static String resolveVars(String text, Map<String, String> variables) {
    if (text == null || text.isEmpty() || variables == null || variables.isEmpty()) return text;
    String result = text;
    for (var e : variables.entrySet()) {
      String ph = "{{" + e.getKey() + "}}";
      if (result.contains(ph)) result = result.replace(ph, e.getValue());
    }
    return result;
  }

  private static final class ListenerImpl implements WebSocket.Listener {
    private final LinkedBlockingQueue<String> inbound;
    private final AtomicReference<Throwable> lastError;
    private final AtomicBoolean open;

    private ListenerImpl(
        LinkedBlockingQueue<String> inbound,
        AtomicReference<Throwable> lastError,
        AtomicBoolean open) {
      this.inbound = inbound;
      this.lastError = lastError;
      this.open = open;
    }

    @Override
    public void onOpen(WebSocket webSocket) {
      open.set(true);
      WebSocket.Listener.super.onOpen(webSocket);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
      inbound.offer(data.toString());
      return WebSocket.Listener.super.onText(webSocket, data, last);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
      lastError.set(error);
      WebSocket.Listener.super.onError(webSocket, error);
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
      open.set(false);
      return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
    }
  }
}
