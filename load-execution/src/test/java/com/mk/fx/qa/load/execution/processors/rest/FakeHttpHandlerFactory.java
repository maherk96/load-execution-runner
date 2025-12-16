package com.mk.fx.qa.load.execution.processors.rest;

import com.sun.net.httpserver.HttpHandler;
import java.io.OutputStream;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public final class FakeHttpHandlerFactory {

    public static HttpHandler create(
            EndpointProfile profile,
            AtomicInteger globalCounter,
            Map<String, AtomicInteger> perEndpoint
    ) {
        perEndpoint.put(profile.name(), new AtomicInteger());

        return exchange -> {
            globalCounter.incrementAndGet();
            perEndpoint.get(profile.name()).incrementAndGet();

            long delay = LoadBehaviorEngine.randomDelay(
                    profile.minDelayMs(),
                    profile.maxDelayMs()
            );
            LoadBehaviorEngine.delay(delay);

            boolean success = LoadBehaviorEngine.isSuccess(profile.successRate());

            if (!success && profile.allowTimeouts()) {
                // Instead of hanging the handler thread, respond with 504
                byte[] timeoutBody = "{\"error\":\"timeout\"}".getBytes();
                exchange.sendResponseHeaders(504, timeoutBody.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(timeoutBody);
                }
                return;
            }

            int status = success
                    ? profile.successStatus()
                    : 500 + ThreadLocalRandom.current().nextInt(3);

            byte[] body = success
                    ? LoadBehaviorEngine.payload(profile.payloadBytes())
                    : "{\"error\":\"internal\"}".getBytes();

            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        };
    }
}
