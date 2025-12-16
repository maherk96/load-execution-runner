package com.mk.fx.qa.load.execution.processors.rest;

public record EndpointProfile(
        String name,
        int minDelayMs,
        int maxDelayMs,
        int successStatus,
        double successRate,
        boolean allowTimeouts,
        int payloadBytes
) {}