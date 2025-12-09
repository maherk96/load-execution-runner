# WebSocket Load Testing JSON Schema

This mirrors the REST schema so clients can submit a `taskType: WEBSOCKET` task to `/api/tasks`.

## TaskSubmissionRequest

- `taskType`: `"WEBSOCKET"`
- `data`: object matching `WebSocketLoadTaskDefinition`

## WebSocketLoadTaskDefinition

```
{
  "testSpec": {
    "id": "optional-id",
    "globalConfig": {
      "url": "ws:// or wss:// required",
      "headers": { "Header-Name": "Value" },
      "vars": { "k": "v" },
      "subprotocols": ["chat", "json"],
      "timeouts": {
        "connectionTimeoutMs": 5000,
        "messageTimeoutMs": 15000
      }
    },
    "scenarios": [
      {
        "name": "scenario-name",
        "messages": [
          {
            "name": "m1",
            "text": "ping {{userId}}",
            "awaitPattern": "pong",
            "awaitTimeoutMs": 3000
          },
          {
            "name": "m2",
            "json": { "type": "chat", "msg": "hi" }
          }
        ]
      }
    ]
  },
  "execution": {
    "thinkTime": { "type": "NONE" | "FIXED" | "RANDOM", "fixedMs": 0, "min": 0, "max": 0 },
    "loadModel": {
      // OPEN model
      "type": "OPEN",
      "arrivalRatePerSec": 10.0,
      "maxConcurrent": 50,
      "duration": "2m"
      // or CLOSED model
      // "type": "CLOSED", "users": 10, "iterations": 5, "warmup":"10s", "rampUp":"30s", "holdFor":"2m"
    }
  }
}
```

## Semantics

- A "request" equals one message send. If `awaitPattern` is set, success requires receiving a message containing the substring within the timeout; otherwise it is recorded as `WS_TIMEOUT`.
- `vars` substitute `{{name}}` in `url`, message `text`, and serialized `json`.
- Connection is created per scenario; messages in a scenario reuse the same connection.
- Think time applies between messages.

## Examples

### CLOSED model

```
{
  "taskType": "WEBSOCKET",
  "data": {
    "testSpec": {
      "globalConfig": { "url": "wss://echo.example.com/ws" },
      "scenarios": [
        { "name": "chat", "messages": [
          { "name": "hello", "text": "ping", "awaitPattern": "pong", "awaitTimeoutMs": 3000 },
          { "name": "msg", "json": { "msg": "hi" } }
        ]}
      ]
    },
    "execution": {
      "thinkTime": { "type": "NONE" },
      "loadModel": { "type": "CLOSED", "users": 2, "iterations": 3 }
    }
  }
}
```

### OPEN model

```
{
  "taskType": "WEBSOCKET",
  "data": {
    "testSpec": {
      "globalConfig": { "url": "ws://127.0.0.1:8080/ws", "vars": {"room":"r1"} },
      "scenarios": [
        { "name": "broadcast", "messages": [
          { "text": "{\"join\":\"{{room}}\"}" },
          { "text": "{\"msg\":\"hi\"}", "awaitPattern": "\"ack\":\"hi\"" }
        ]}
      ]
    },
    "execution": {
      "thinkTime": { "type": "RANDOM", "min": 100, "max": 300 },
      "loadModel": { "type": "OPEN", "arrivalRatePerSec": 10, "maxConcurrent": 50, "duration": "1m" }
    }
  }
}
```

