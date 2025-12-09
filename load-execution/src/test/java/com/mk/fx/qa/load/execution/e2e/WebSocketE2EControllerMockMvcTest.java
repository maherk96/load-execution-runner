package com.mk.fx.qa.load.execution.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mk.fx.qa.load.execution.LoadExecutionRunnerMain;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskRunReport;
import com.mk.fx.qa.load.execution.dto.controllerresponse.TaskSubmissionRequest;
import com.mk.fx.qa.load.execution.processors.websocket.LocalEchoWsServer;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@SpringBootTest(classes = LoadExecutionRunnerMain.class)
@AutoConfigureMockMvc
class WebSocketE2EControllerMockMvcTest {

  @Autowired private MockMvc mvc;
  @Autowired private ObjectMapper mapper;

  private LocalEchoWsServer wsServer;
  private String wsUrl;

  @BeforeEach
  void startEcho() throws Exception {
    int wsPort = LocalEchoWsServer.findFreePort();
    wsServer = new LocalEchoWsServer(wsPort);
    wsServer.start();
    wsUrl = "ws://127.0.0.1:" + wsPort;
  }

  @AfterEach
  void stopEcho() throws Exception {
    if (wsServer != null) wsServer.stop();
  }

  @Test
  void e2e_closed_and_open_models_via_controller_endpoints() throws Exception {
    // Health check
    mvc.perform(get("/api/tasks/healthy")).andExpect(status().isOk());

    // CLOSED submission
    TaskSubmissionRequest closedReq = buildClosed(wsUrl);
    MvcResult subClosed =
        mvc.perform(
                post("/api/tasks")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsBytes(closedReq)))
            .andExpect(status().isAccepted())
            .andReturn();
    String closedId =
        mapper.readTree(subClosed.getResponse().getContentAsString()).get("taskId").asText();

    awaitCompletion(UUID.fromString(closedId), Duration.ofSeconds(5));

    // Report/metrics
    mvc.perform(get("/api/tasks/{id}/metrics", closedId)).andExpect(status().isOk());
    MvcResult repClosed =
        mvc.perform(get("/api/tasks/{id}/report", closedId)).andExpect(status().isOk()).andReturn();
    TaskRunReport reportClosed =
        mapper.readValue(repClosed.getResponse().getContentAsByteArray(), TaskRunReport.class);
    assertThat(reportClosed.protocolDetails).isNotNull();
    assertThat(reportClosed.protocolDetails.websocket).isNotNull();
    assertThat(reportClosed.protocolDetails.websocket.messages).isNotEmpty();

    // OPEN submission
    TaskSubmissionRequest openReq = buildOpen(wsUrl, 5.0, 2, "200ms");
    MvcResult subOpen =
        mvc.perform(
                post("/api/tasks")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(mapper.writeValueAsBytes(openReq)))
            .andExpect(status().isAccepted())
            .andReturn();
    String openId =
        mapper.readTree(subOpen.getResponse().getContentAsString()).get("taskId").asText();

    awaitCompletion(UUID.fromString(openId), Duration.ofSeconds(5));
    MvcResult repOpen =
        mvc.perform(get("/api/tasks/{id}/report", openId)).andExpect(status().isOk()).andReturn();
    TaskRunReport reportOpen =
        mapper.readValue(repOpen.getResponse().getContentAsByteArray(), TaskRunReport.class);
    assertThat(reportOpen.metrics.totalRequests).isEqualTo(2); // one iteration -> 2 messages

    // Listings and queue endpoints
    mvc.perform(get("/api/tasks")).andExpect(status().isOk());
    mvc.perform(get("/api/tasks/history")).andExpect(status().isOk());
    mvc.perform(get("/api/tasks/queue")).andExpect(status().isOk());
    mvc.perform(get("/api/tasks/types")).andExpect(status().isOk());
  }

  private void awaitCompletion(UUID taskId, Duration timeout) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      MvcResult res = mvc.perform(get("/api/tasks/{id}", taskId)).andReturn();
      int sc = res.getResponse().getStatus();
      if (sc == 200) {
        String body = res.getResponse().getContentAsString();
        String status = mapper.readTree(body).get("status").asText();
        if (List.of("COMPLETED", "ERROR", "CANCELLED").contains(status)) {
          return;
        }
      }
      Thread.sleep(50);
    }
    throw new AssertionError("Task did not complete in time: " + taskId);
  }

  private TaskSubmissionRequest buildClosed(String wsUrl) {
    Map<String, Object> data = new HashMap<>();
    Map<String, Object> testSpec = new HashMap<>();
    testSpec.put("globalConfig", Map.of("url", wsUrl));

    List<Map<String, Object>> messages =
        List.of(
            Map.of(
                "name", "m1",
                "text", "ping",
                "awaitPattern", "pong",
                "awaitTimeoutMs", 1000),
            Map.of("name", "m2", "text", "hello"));

    testSpec.put("scenarios", List.of(Map.of("name", "s1", "messages", messages)));

    Map<String, Object> exec = new HashMap<>();
    exec.put("thinkTime", Map.of("type", "NONE"));
    exec.put("loadModel", Map.of("type", "CLOSED", "users", 1, "iterations", 1));
    data.put("testSpec", testSpec);
    data.put("execution", exec);

    TaskSubmissionRequest req = new TaskSubmissionRequest();
    req.setTaskType("WEBSOCKET");
    req.setData(data);
    return req;
  }

  private TaskSubmissionRequest buildOpen(String wsUrl, double rate, int maxConc, String duration) {
    Map<String, Object> data = new HashMap<>();
    Map<String, Object> testSpec = new HashMap<>();
    testSpec.put("globalConfig", Map.of("url", wsUrl));

    List<Map<String, Object>> messages =
        List.of(
            Map.of(
                "name", "m1",
                "text", "ping",
                "awaitPattern", "pong",
                "awaitTimeoutMs", 1000),
            Map.of("name", "m2", "text", "hello"));

    testSpec.put("scenarios", List.of(Map.of("name", "s1", "messages", messages)));

    Map<String, Object> exec = new HashMap<>();
    exec.put("thinkTime", Map.of("type", "NONE"));
    exec.put(
        "loadModel",
        Map.of(
            "type", "OPEN",
            "arrivalRatePerSec", rate,
            "maxConcurrent", maxConc,
            "duration", duration));
    data.put("testSpec", testSpec);
    data.put("execution", exec);

    TaskSubmissionRequest req = new TaskSubmissionRequest();
    req.setTaskType("WEBSOCKET");
    req.setData(data);
    return req;
  }
}
