package com.mk.fx.qa.load.execution.executors.open;

import java.time.Duration;

public record OpenLoadParameters(double arrivalRatePerSec, int maxConcurrent, Duration duration) {}
