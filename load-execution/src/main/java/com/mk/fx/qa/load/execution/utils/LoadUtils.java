package com.mk.fx.qa.load.execution.utils;

import java.time.Duration;

public final class LoadUtils {

    private LoadUtils() {
        // Utility class, no instantiation
    }

    public static Duration toDuration(Duration duration) {
        return duration != null ? duration : Duration.ZERO;
    }

    public static int toSeconds(Integer millis) {
        int value = millis != null ? millis : 5000;
        return (int) Math.max(1, Math.ceil(value / 1000.0));
    }

    public static Duration parseDuration(String value) {
        if (value == null || value.isBlank()) {
            return Duration.ZERO;
        }
        String trimmed = value.trim().toLowerCase();
        if (trimmed.endsWith("ms")) {
            long ms = Long.parseLong(trimmed.substring(0, trimmed.length() - 2));
            return Duration.ofMillis(ms);
        }
        char unit = trimmed.charAt(trimmed.length() - 1);
        long amount = Long.parseLong(trimmed.substring(0, trimmed.length() - 1));
        return switch (unit) {
            case 's' -> Duration.ofSeconds(amount);
            case 'm' -> Duration.ofMinutes(amount);
            case 'h' -> Duration.ofHours(amount);
            default -> throw new IllegalArgumentException("Unrecognised duration unit in " + value);
        };
    }
}