package com.mk.fx.qa.load.execution.ws;

/** Result of a WebSocket send operation. */
public record WsSendResult(boolean success, boolean timedOut, long latencyMs) {}
