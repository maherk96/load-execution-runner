package com.mk.fx.qa.load.execution.processors.websocket;

import jakarta.websocket.Endpoint;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.MessageHandler;
import jakarta.websocket.Session;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;
import org.springframework.web.socket.server.standard.ServerEndpointRegistration;

@TestConfiguration
public class TestWebSocketConfig {

  @Bean
  public ServerEndpointExporter serverEndpointExporter() {
    return new ServerEndpointExporter();
  }

  @Bean
  public ServerEndpointRegistration echoEndpoint() {
    return new ServerEndpointRegistration("/ws/echo", new EchoEndpoint());
  }

  public static class EchoEndpoint extends Endpoint {
    @Override
    public void onOpen(Session session, EndpointConfig config) {
      session.addMessageHandler(
          (MessageHandler.Whole<String>)
              message -> {
                String response =
                    message.contains("ping") ? message.replace("ping", "pong") : message;
                session.getAsyncRemote().sendText(response);
              });
    }
  }
}
