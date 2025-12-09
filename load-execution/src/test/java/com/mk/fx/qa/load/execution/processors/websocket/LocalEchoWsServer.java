package com.mk.fx.qa.load.execution.processors.websocket;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicBoolean;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

/** Simple local WebSocket echo server for tests. */
public class LocalEchoWsServer extends WebSocketServer {

  private final AtomicBoolean pong = new AtomicBoolean(true);

  public static int findFreePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  public LocalEchoWsServer(int port) {
    super(new InetSocketAddress("127.0.0.1", port));
  }

  public void setPongEnabled(boolean enabled) {
    pong.set(enabled);
  }

  @Override
  public void onOpen(WebSocket conn, ClientHandshake handshake) {}

  @Override
  public void onClose(WebSocket conn, int code, String reason, boolean remote) {}

  @Override
  public void onMessage(WebSocket conn, String message) {
    String response =
        pong.get() && message.contains("ping") ? message.replace("ping", "pong") : message;
    conn.send(response);
  }

  @Override
  public void onError(WebSocket conn, Exception ex) {}

  @Override
  public void onStart() {}
}
