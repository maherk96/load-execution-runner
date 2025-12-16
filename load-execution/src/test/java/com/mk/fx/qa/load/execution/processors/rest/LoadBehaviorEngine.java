package com.mk.fx.qa.load.execution.processors.rest;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

final class LoadBehaviorEngine {

    private static final Random RANDOM = new Random();

    static long randomDelay(int min, int max) {
        return min + RANDOM.nextInt(max - min + 1);
    }

    static boolean isSuccess(double successRate) {
        return RANDOM.nextDouble() < successRate;
    }

    static void delay(long ms) {
        if (ms > 0) {
            LockSupport.parkNanos(Duration.ofMillis(ms).toNanos());
        }
    }

    static byte[] payload(int bytes) {
        byte[] data = new byte[Math.max(bytes, 0)];
        RANDOM.nextBytes(data);
        return data;
    }

    static void simulateTimeout() {
        LockSupport.park();
    }
}