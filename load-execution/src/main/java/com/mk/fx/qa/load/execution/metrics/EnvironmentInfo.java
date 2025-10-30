package com.mk.fx.qa.load.execution.metrics;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Utility class to retrieve environment information such as the host name and
 * the user who triggered the execution.
 */
public final class EnvironmentInfo {

    private EnvironmentInfo() {
        // Prevent instantiation
    }

    /**
     * Retrieves the host name of the machine where the application is running.
     *
     * @return the host name, or {@code "unknown"} if it cannot be determined.
     */
    public static String host() {
        String env = firstNonBlank(System.getenv("HOSTNAME"), System.getenv("COMPUTERNAME"), null);
        if (env != null) return env;

        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }

    /**
     * Retrieves the user who triggered the execution, either from environment variables or system properties.
     *
     * @return the user name, or {@code "unknown"} if it cannot be determined.
     */
    public static String triggeredBy() {
        return firstNonBlank(System.getenv("TRIGGERED_BY"), System.getProperty("user.name"), "unknown");
    }

    /**
     * Retrieves the first non-blank string from the provided arguments, or returns the default value
     * if both are blank or {@code null}.
     *
     * @param a   the first string to check
     * @param b   the second string to check
     * @param def the default value to return if both are null or blank
     * @return the first non-blank string, or {@code def} if none found
     */
    private static String firstNonBlank(String a, String b, String def) {
        if (a != null && !a.isBlank()) return a;
        if (b != null && !b.isBlank()) return b;
        return def;
    }
}