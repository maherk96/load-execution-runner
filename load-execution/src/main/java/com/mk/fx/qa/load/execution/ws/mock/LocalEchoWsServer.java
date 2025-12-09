package com.mk.fx.qa.load.execution.ws.mock;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

/** Simple echo WebSocket server used for local/manual testing. */
public class LocalEchoWsServer extends WebSocketServer {

  public static int findFreePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  public LocalEchoWsServer(int port) {
    super(new InetSocketAddress("127.0.0.1", port));
  }

  @Override
  public void onOpen(WebSocket conn, ClientHandshake handshake) {}

  @Override
  public void onClose(WebSocket conn, int code, String reason, boolean remote) {}

  @Override
  public void onMessage(WebSocket conn, String message) {
    String response = message.contains("ping") ? message.replace("ping", "pong") : message;
    conn.send(response);
  }

  @Override
  public void onError(WebSocket conn, Exception ex) {}

  @Override
  public void onStart() {}
}
