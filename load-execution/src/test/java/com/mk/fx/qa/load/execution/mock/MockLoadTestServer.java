package com.mk.fx.qa.load.execution.mock;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Standalone mock REST API server for load testing.
 * Simulates realistic API behavior with varying response times and occasional errors.
 *
 * Run this server first, then execute load tests against http://localhost:8080
 */
public class MockLoadTestServer {

  private static final int PORT = 8080;
  private static final AtomicInteger totalRequests = new AtomicInteger(0);

  public static void main(String[] args) throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);

    System.out.println("🚀 Starting Mock Load Test Server on port " + PORT);
    System.out.println("Base URL: http://localhost:" + PORT);
    System.out.println("Press Ctrl+C to stop\n");

    // Health check endpoint - very fast
    server.createContext("/api/health", new FastEndpoint("health", 5, 15, 200, 0.99));

    // User endpoints
    server.createContext("/api/users", new FastEndpoint("users", 10, 50, 200, 0.95));

    // Product endpoints
    server.createContext("/api/products", new MediumEndpoint("products", 30, 100, 200, 0.92));

    // Cart endpoints
    server.createContext("/api/cart", new FastEndpoint("cart", 15, 60, 200, 0.94));

    // Order endpoints - slower due to "processing"
    server.createContext("/api/orders", new SlowEndpoint("orders", 80, 250, 201, 0.88));

    // Search endpoint - variable performance
    server.createContext("/api/search", new MediumEndpoint("search", 40, 150, 200, 0.90));

    // Analytics endpoints - fast writes
    server.createContext("/api/analytics", new FastEndpoint("analytics", 10, 40, 201, 0.96));

    // Reports endpoint - slow, resource-intensive
    server.createContext("/api/reports", new SlowEndpoint("reports", 150, 500, 200, 0.75));

    // Recommendations endpoint
    server.createContext("/api/recommendations", new MediumEndpoint("recommendations", 50, 120, 200, 0.85));

    server.setExecutor(null); // Use default executor
    server.start();

    System.out.println("✅ Server is ready to handle load tests!\n");

    // Print stats periodically
    Thread statsThread = new Thread(() -> {
      while (true) {
        try {
          Thread.sleep(10000); // Every 10 seconds
          System.out.println("📊 Total requests handled: " + totalRequests.get());
        } catch (InterruptedException e) {
          break;
        }
      }
    });
    statsThread.setDaemon(true);
    statsThread.start();
  }

  private static class FastEndpoint implements HttpHandler {
    private final String name;
    private final int minDelayMs;
    private final int maxDelayMs;
    private final int successCode;
    private final double successRate;
    private final Random random = new Random();

    public FastEndpoint(String name, int minDelayMs, int maxDelayMs, int successCode, double successRate) {
      this.name = name;
      this.minDelayMs = minDelayMs;
      this.maxDelayMs = maxDelayMs;
      this.successCode = successCode;
      this.successRate = successRate;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      totalRequests.incrementAndGet();

      // Simulate processing delay
      int delay = minDelayMs + random.nextInt(maxDelayMs - minDelayMs + 1);
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      // Determine success or failure
      boolean success = random.nextDouble() < successRate;
      int statusCode = success ? successCode : getErrorCode();

      String responseBody = buildResponse(exchange.getRequestMethod(), success, delay);
      sendResponse(exchange, statusCode, responseBody);
    }

    protected int getErrorCode() {
      Random r = new Random();
      int choice = r.nextInt(5);
      return switch (choice) {
        case 0 -> 400; // Bad Request
        case 1 -> 404; // Not Found
        case 2 -> 429; // Too Many Requests
        case 3 -> 500; // Internal Server Error
        default -> 503; // Service Unavailable
      };
    }

    protected String buildResponse(String method, boolean success, int delay) {
      if (success) {
        return String.format("""
            {
              "status": "success",
              "endpoint": "%s",
              "method": "%s",
              "processingTimeMs": %d,
              "timestamp": "%s",
              "data": {
                "id": "%s",
                "count": %d
              }
            }
            """, name, method, delay, java.time.Instant.now(),
            java.util.UUID.randomUUID(), new Random().nextInt(100));
      } else {
        return String.format("""
            {
              "status": "error",
              "endpoint": "%s",
              "message": "Request failed during processing",
              "timestamp": "%s"
            }
            """, name, java.time.Instant.now());
      }
    }

    protected void sendResponse(HttpExchange exchange, int statusCode, String responseBody) throws IOException {
      byte[] body = responseBody.getBytes();
      exchange.getResponseHeaders().set("Content-Type", "application/json");
      exchange.sendResponseHeaders(statusCode, body.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(body);
      }
    }
  }

  private static class MediumEndpoint extends FastEndpoint {
    public MediumEndpoint(String name, int minDelayMs, int maxDelayMs, int successCode, double successRate) {
      super(name, minDelayMs, maxDelayMs, successCode, successRate);
    }
  }

  private static class SlowEndpoint extends FastEndpoint {
    public SlowEndpoint(String name, int minDelayMs, int maxDelayMs, int successCode, double successRate) {
      super(name, minDelayMs, maxDelayMs, successCode, successRate);
    }

    @Override
    protected String buildResponse(String method, boolean success, int delay) {
      if (success) {
        return String.format("""
            {
              "status": "success",
              "endpoint": "%s",
              "method": "%s",
              "processingTimeMs": %d,
              "timestamp": "%s",
              "warning": "This endpoint is resource-intensive",
              "data": {
                "recordsProcessed": %d,
                "generatedReport": "%s"
              }
            }
            """, super.name, method, delay, java.time.Instant.now(),
            new Random().nextInt(10000), java.util.UUID.randomUUID());
      } else {
        return super.buildResponse(method, success, delay);
      }
    }
  }
}

