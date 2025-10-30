package com.mk.fx.qa.load.execution.model;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public enum TaskType {
    REST,
    FIX;

    public static TaskType fromValue(String value) {
        return Arrays.stream(values())
                .filter(type -> type.name().equalsIgnoreCase(value))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unsupported task type: " + value));
    }

    public static Set<String> asStrings() {
        return Arrays.stream(values()).map(Enum::name).collect(Collectors.toUnmodifiableSet());
    }
}