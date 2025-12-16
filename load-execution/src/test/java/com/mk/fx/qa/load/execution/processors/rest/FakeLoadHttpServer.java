package com.mk.fx.qa.load.execution.processors.rest;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FakeLoadHttpServer {

    private final HttpServer server;
    private final AtomicInteger totalRequests = new AtomicInteger();
    private final Map<String, AtomicInteger> endpointCounters = new HashMap<>();

    public FakeLoadHttpServer(Map<String, EndpointProfile> endpoints) throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);

        endpoints.forEach((path, profile) ->
                server.createContext(
                        path,
                        FakeHttpHandlerFactory.create(profile, totalRequests, endpointCounters)
                )
        );
    }

    public void start() {
        server.start();
    }

    public void stop() {
        server.stop(0);
    }

    public String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    public int totalRequests() {
        return totalRequests.get();
    }

    public Map<String, AtomicInteger> endpointCounters() {
        return endpointCounters;
    }
}