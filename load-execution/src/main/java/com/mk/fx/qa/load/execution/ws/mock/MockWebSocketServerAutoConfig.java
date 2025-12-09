package com.mk.fx.qa.load.execution.ws.mock;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-starts a local echo WebSocket server for manual testing when enabled via properties.
 *
 * <p>Properties: - ws.mock.enabled=true|false (default false) - ws.mock.port=<port> (default 0 for
 * random free port)
 */
@Slf4j
@Configuration
public class MockWebSocketServerAutoConfig {

  @Value("${ws.mock.enabled:false}")
  private boolean enabled;

  @Value("${ws.mock.port:0}")
  private int configuredPort;

  private LocalEchoWsServer server;
  private int actualPort;

  @PostConstruct
  void start() {
    if (!enabled) {
      log.info("Mock WebSocket server disabled (set ws.mock.enabled=true to enable)");
      return;
    }
    try {
      int port = configuredPort > 0 ? configuredPort : LocalEchoWsServer.findFreePort();
      server = new LocalEchoWsServer(port);
      server.start();
      actualPort = port;
      log.info(
          "Mock WebSocket server started on ws://127.0.0.1:{} (ping->pong echo). Use this URL in Swagger.",
          actualPort);
    } catch (Exception e) {
      log.error("Failed to start mock WebSocket server: {}", e.getMessage(), e);
    }
  }

  @PreDestroy
  void stop() {
    if (server != null) {
      try {
        server.stop();
        log.info("Mock WebSocket server stopped (port {}):", actualPort);
      } catch (Exception ignored) {
      }
    }
  }
}
