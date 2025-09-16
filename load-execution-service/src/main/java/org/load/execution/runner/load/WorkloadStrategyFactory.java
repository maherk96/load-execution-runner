package org.load.execution.runner.load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Enhanced workload strategy factory that provides robust strategy creation, validation,
 * and lifecycle management for load testing workload patterns.
 * 
 * <p>This factory supports multiple strategy types with comprehensive validation,
 * configuration management, and extensibility for custom workload patterns:
 * 
 * <ul>
 *   <li><b>Strategy Registration:</b> Dynamic registration of custom strategies</li>
 *   <li><b>Validation:</b> Comprehensive validation of strategy configurations</li>
 *   <li><b>Lifecycle Management:</b> Proper initialization and cleanup of strategies</li>
 *   <li><b>Metrics Collection:</b> Strategy usage and performance metrics</li>
 *   <li><b>Resource Management:</b> Efficient resource allocation and cleanup</li>
 *   <li><b>Error Handling:</b> Robust error recovery and fallback mechanisms</li>
 * </ul>
 * 
 * <p><b>Strategy Types Supported:</b>
 * <ul>
 *   <li><b>CLOSED:</b> Fixed number of virtual users with iterations</li>
 *   <li><b>OPEN:</b> Arrival rate-based request generation</li>
 *   <li><b>CUSTOM:</b> User-defined strategies via registration</li>
 * </ul>
 * 
 * <p><b>Thread Safety:</b> All operations are thread-safe and designed for concurrent
 * access during load test execution.
 * 
 * @author Load Test Team
 * @version 2.0.0
 */
public class WorkloadStrategyFactory {
    private static final Logger log = LoggerFactory.getLogger(WorkloadStrategyFactory.class);

    // ================================
    // CONFIGURATION
    // ================================

    /**
     * Configuration for strategy factory behavior.
     */
    public static class FactoryConfig {
        private final boolean enableValidation;
        private final boolean enableMetrics;
        private final boolean enableCaching;
        private final Duration strategyTimeout;
        private final int maxCachedStrategies;
        private final boolean allowCustomStrategies;
        private final Map<String, Object> defaultProperties;

        private FactoryConfig(Builder builder) {
            this.enableValidation = builder.enableValidation;
            this.enableMetrics = builder.enableMetrics;
            this.enableCaching = builder.enableCaching;
            this.strategyTimeout = builder.strategyTimeout;
            this.maxCachedStrategies = Math.max(1, builder.maxCachedStrategies);
            this.allowCustomStrategies = builder.allowCustomStrategies;
            this.defaultProperties = Map.copyOf(builder.defaultProperties);
        }

        public static FactoryConfig defaultConfig() {
            return new Builder().build();
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private boolean enableValidation = true;
            private boolean enableMetrics = true;
            private boolean enableCaching = false;
            private Duration strategyTimeout = Duration.ofMinutes(2);
            private int maxCachedStrategies = 10;
            private boolean allowCustomStrategies = true;
            private Map<String, Object> defaultProperties = new HashMap<>();

            public Builder enableValidation(boolean enable) {
                this.enableValidation = enable;
                return this;
            }

            public Builder enableMetrics(boolean enable) {
                this.enableMetrics = enable;
                return this;
            }

            public Builder enableCaching(boolean enable) {
                this.enableCaching = enable;
                return this;
            }

            public Builder strategyTimeout(Duration timeout) {
                this.strategyTimeout = timeout;
                return this;
            }

            public Builder allowCustomStrategies(boolean allow) {
                this.allowCustomStrategies = allow;
                return this;
            }

            public Builder defaultProperty(String key, Object value) {
                this.defaultProperties.put(key, value);
                return this;
            }

            public FactoryConfig build() {
                return new FactoryConfig(this);
            }
        }

        // Getters
        public boolean isValidationEnabled() { return enableValidation; }
        public boolean isMetricsEnabled() { return enableMetrics; }
        public boolean isCachingEnabled() { return enableCaching; }
        public Duration getStrategyTimeout() { return strategyTimeout; }
        public int getMaxCachedStrategies() { return maxCachedStrategies; }
        public boolean isCustomStrategiesAllowed() { return allowCustomStrategies; }
        public Map<String, Object> getDefaultProperties() { return defaultProperties; }
    }

    /**
     * Metadata about a workload strategy.
     */
    public static class StrategyMetadata {
        private final String name;
        private final String description;
        private final TestPlanSpec.WorkLoadModel type;
        private final Set<String> requiredParameters;
        private final Set<String> optionalParameters;
        private final Map<String, Object> defaultValues;
        private final boolean isCustom;

        public StrategyMetadata(String name, String description, TestPlanSpec.WorkLoadModel type,
                              Set<String> requiredParameters, Set<String> optionalParameters,
                              Map<String, Object> defaultValues, boolean isCustom) {
            this.name = name;
            this.description = description;
            this.type = type;
            this.requiredParameters = Set.copyOf(requiredParameters);
            this.optionalParameters = Set.copyOf(optionalParameters);
            this.defaultValues = Map.copyOf(defaultValues);
            this.isCustom = isCustom;
        }

        // Getters
        public String getName() { return name; }
        public String getDescription() { return description; }
        public TestPlanSpec.WorkLoadModel getType() { return type; }
        public Set<String> getRequiredParameters() { return requiredParameters; }
        public Set<String> getOptionalParameters() { return optionalParameters; }
        public Map<String, Object> getDefaultValues() { return defaultValues; }
        public boolean isCustom() { return isCustom; }

        @Override
        public String toString() {
            return String.format("StrategyMetadata{name='%s', type=%s, custom=%s}", name, type, isCustom);
        }
    }

    /**
     * Factory metrics for monitoring strategy creation and usage.
     */
    public static class FactoryMetrics {
        private final long totalStrategiesCreated;
        private final long cachedStrategiesUsed;
        private final long validationFailures;
        private final long creationFailures;
        private final Map<TestPlanSpec.WorkLoadModel, Long> strategyTypeDistribution;
        private final Map<String, Long> customStrategyUsage;
        private final double averageCreationTimeMs;
        private final Instant lastCreated;
        private final Instant metricsSnapshot;

        public FactoryMetrics(long totalStrategiesCreated, long cachedStrategiesUsed, 
                            long validationFailures, long creationFailures,
                            Map<TestPlanSpec.WorkLoadModel, Long> strategyTypeDistribution,
                            Map<String, Long> customStrategyUsage, double averageCreationTimeMs,
                            Instant lastCreated) {
            this.totalStrategiesCreated = totalStrategiesCreated;
            this.cachedStrategiesUsed = cachedStrategiesUsed;
            this.validationFailures = validationFailures;
            this.creationFailures = creationFailures;
            this.strategyTypeDistribution = Map.copyOf(strategyTypeDistribution);
            this.customStrategyUsage = Map.copyOf(customStrategyUsage);
            this.averageCreationTimeMs = averageCreationTimeMs;
            this.lastCreated = lastCreated;
            this.metricsSnapshot = Instant.now();
        }

        // Getters
        public long getTotalStrategiesCreated() { return totalStrategiesCreated; }
        public long getCachedStrategiesUsed() { return cachedStrategiesUsed; }
        public long getValidationFailures() { return validationFailures; }
        public long getCreationFailures() { return creationFailures; }
        public Map<TestPlanSpec.WorkLoadModel, Long> getStrategyTypeDistribution() { return strategyTypeDistribution; }
        public Map<String, Long> getCustomStrategyUsage() { return customStrategyUsage; }
        public double getAverageCreationTimeMs() { return averageCreationTimeMs; }
        public Instant getLastCreated() { return lastCreated; }
        public Instant getMetricsSnapshot() { return metricsSnapshot; }

        public double getSuccessRate() {
            long total = totalStrategiesCreated + creationFailures;
            return total > 0 ? (double) totalStrategiesCreated / total : 1.0;
        }

        @Override
        public String toString() {
            return String.format("FactoryMetrics[created=%d, cached=%d, failures=%d, successRate=%.2f%%, avgTime=%.2fms]",
                    totalStrategiesCreated, cachedStrategiesUsed, creationFailures, getSuccessRate() * 100, averageCreationTimeMs);
        }
    }

    // ================================
    // STRATEGY INTERFACES
    // ================================

    /**
     * Enhanced workload strategy interface with lifecycle management.
     */
    public interface WorkloadStrategy {
        /**
         * Executes the workload strategy.
         */
        void execute(TestPlanSpec testPlanSpec);

        /**
         * Validates the strategy configuration.
         */
        default void validate(TestPlanSpec testPlanSpec) throws StrategyValidationException {
            // Default implementation - strategies can override
        }

        /**
         * Initializes the strategy with resources.
         */
        default void initialize() throws StrategyInitializationException {
            // Default implementation - strategies can override
        }

        /**
         * Cleans up strategy resources.
         */
        default void cleanup() {
            // Default implementation - strategies can override
        }

        /**
         * Returns strategy metadata.
         */
        default StrategyMetadata getMetadata() {
            return new StrategyMetadata(
                getClass().getSimpleName(), 
                "Default strategy implementation",
                TestPlanSpec.WorkLoadModel.CLOSED, // Default
                Set.of(), Set.of(), Map.of(), false
            );
        }

        /**
         * Checks if the strategy supports the given load model.
         */
        default boolean supports(TestPlanSpec.LoadModel loadModel) {
            return true; // Default - all strategies support all models
        }
    }

    /**
     * Factory for creating custom workload strategies.
     */
    @FunctionalInterface
    public interface StrategyFactory {
        WorkloadStrategy create(TestPhaseManager phaseManager, RequestExecutor requestExecutor,
                              ResourceManager resourceManager, AtomicBoolean cancelled);
    }

    // ================================
    // INSTANCE FIELDS
    // ================================

    private final FactoryConfig config;
    private final Map<TestPlanSpec.WorkLoadModel, StrategyFactory> strategyFactories;
    private final Map<String, StrategyFactory> customStrategyFactories;
    private final Map<TestPlanSpec.WorkLoadModel, StrategyMetadata> strategyMetadata;
    private final Map<String, WorkloadStrategy> strategyCache;
    private final StrategyValidator validator;
    private final FactoryMetricsCollector metricsCollector;

    // ================================
    // CONSTRUCTORS
    // ================================

    /**
     * Creates a WorkloadStrategyFactory with default configuration.
     */
    public WorkloadStrategyFactory() {
        this(FactoryConfig.defaultConfig());
    }

    /**
     * Creates a WorkloadStrategyFactory with custom configuration.
     */
    public WorkloadStrategyFactory(FactoryConfig config) {
        this.config = config != null ? config : FactoryConfig.defaultConfig();
        this.strategyFactories = new ConcurrentHashMap<>();
        this.customStrategyFactories = new ConcurrentHashMap<>();
        this.strategyMetadata = new ConcurrentHashMap<>();
        this.strategyCache = config.isCachingEnabled() ? new ConcurrentHashMap<>() : null;
        this.validator = new StrategyValidator(this.config);
        this.metricsCollector = new FactoryMetricsCollector(this.config);

        initializeBuiltInStrategies();
        
        log.info("WorkloadStrategyFactory initialized - Validation: {}, Metrics: {}, Caching: {}, Custom strategies: {}",
                config.isValidationEnabled(), config.isMetricsEnabled(), config.isCachingEnabled(), config.isCustomStrategiesAllowed());
    }

    // ================================
    // PUBLIC API
    // ================================

    /**
     * Creates a workload strategy for the given load model with enhanced error handling.
     */
    public WorkloadStrategy createStrategy(TestPlanSpec.LoadModel loadModel,
                                         TestPhaseManager phaseManager,
                                         RequestExecutor requestExecutor,
                                         ResourceManager resourceManager,
                                         AtomicBoolean cancelled) {
        if (loadModel == null) {
            throw new IllegalArgumentException("LoadModel cannot be null");
        }

        long startTime = System.nanoTime();
        
        try {
            // Validate load model if validation is enabled
            if (config.isValidationEnabled()) {
                validator.validateLoadModel(loadModel);
            }

            // Check cache first if caching is enabled
            if (config.isCachingEnabled()) {
                WorkloadStrategy cached = getCachedStrategy(loadModel);
                if (cached != null) {
                    metricsCollector.recordCacheHit();
                    return cached;
                }
            }

            // Create strategy
            WorkloadStrategy strategy = createStrategyInternal(loadModel, phaseManager, requestExecutor, resourceManager, cancelled);

            // Initialize strategy
            strategy.initialize();

            // Validate strategy supports the load model
            if (!strategy.supports(loadModel)) {
                throw new StrategyCreationException(
                    String.format("Strategy %s does not support load model type %s", 
                    strategy.getClass().getSimpleName(), loadModel.getType()));
            }

//            // Validate strategy configuration if enabled
//            if (config.isValidationEnabled()) {
//                TestPlanSpec testPlanSpec = createTestPlanSpec(loadModel);
//                strategy.validate(testPlanSpec);
//            }

            // Cache strategy if caching is enabled
            if (config.isCachingEnabled()) {
                cacheStrategy(loadModel, strategy);
            }

            // Record metrics
            long duration = System.nanoTime() - startTime;
            metricsCollector.recordStrategyCreation(loadModel.getType(), Duration.ofNanos(duration), true);

            log.debug("Created strategy: {} for load model type: {} in {}ms", 
                    strategy.getClass().getSimpleName(), loadModel.getType(), duration / 1_000_000);

            return strategy;

        } catch (Exception e) {
            long duration = System.nanoTime() - startTime;
            metricsCollector.recordStrategyCreation(loadModel.getType(), Duration.ofNanos(duration), false);
            
            log.error("Failed to create strategy for load model type: {}", loadModel.getType(), e);
            throw new StrategyCreationException("Failed to create workload strategy", e);
        }
    }

    /**
     * Registers a custom strategy factory.
     */
    public void registerCustomStrategy(String name, StrategyFactory factory, StrategyMetadata metadata) {
        if (!config.isCustomStrategiesAllowed()) {
            throw new UnsupportedOperationException("Custom strategies are not allowed");
        }

        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Strategy name cannot be null or empty");
        }

        if (factory == null) {
            throw new IllegalArgumentException("Strategy factory cannot be null");
        }

        customStrategyFactories.put(name, factory);
        if (metadata != null) {
            // Store metadata with custom prefix to avoid conflicts
            strategyMetadata.put(TestPlanSpec.WorkLoadModel.valueOf("CUSTOM_" + name.toUpperCase()), metadata);
        }

        log.info("Registered custom strategy: {}", name);
    }

    /**
     * Returns available strategy types.
     */
    public Set<TestPlanSpec.WorkLoadModel> getAvailableStrategyTypes() {
        return Set.copyOf(strategyFactories.keySet());
    }

    /**
     * Returns available custom strategy names.
     */
    public Set<String> getAvailableCustomStrategies() {
        return Set.copyOf(customStrategyFactories.keySet());
    }

    /**
     * Returns metadata for a strategy type.
     */
    public Optional<StrategyMetadata> getStrategyMetadata(TestPlanSpec.WorkLoadModel type) {
        return Optional.ofNullable(strategyMetadata.get(type));
    }

    /**
     * Returns factory metrics.
     */
    public FactoryMetrics getMetrics() {
        return metricsCollector.getMetrics();
    }

    /**
     * Validates a load model configuration.
     */
    public ValidationResult validateLoadModel(TestPlanSpec.LoadModel loadModel) {
        try {
            validator.validateLoadModel(loadModel);
            return ValidationResult.success("Load model is valid");
        } catch (Exception e) {
            return ValidationResult.failure("Load model validation failed: " + e.getMessage());
        }
    }

    /**
     * Clears the strategy cache.
     */
    public void clearCache() {
        if (strategyCache != null) {
            // Cleanup cached strategies
            strategyCache.values().forEach(strategy -> {
                try {
                    strategy.cleanup();
                } catch (Exception e) {
                    log.warn("Error cleaning up cached strategy: {}", e.getMessage());
                }
            });
            strategyCache.clear();
            log.debug("Strategy cache cleared");
        }
    }

    /**
     * Performs cleanup of factory resources.
     */
    public void cleanup() {
        clearCache();
        customStrategyFactories.clear();
        log.info("WorkloadStrategyFactory cleanup completed");
    }

    // ================================
    // PRIVATE IMPLEMENTATION
    // ================================

    private void initializeBuiltInStrategies() {
        // Register built-in CLOSED strategy
        strategyFactories.put(TestPlanSpec.WorkLoadModel.CLOSED, 
                (phaseManager, requestExecutor, resourceManager, cancelled) -> 
                        new ClosedWorkloadStrategy(phaseManager, requestExecutor, resourceManager, cancelled));

        strategyMetadata.put(TestPlanSpec.WorkLoadModel.CLOSED, new StrategyMetadata(
                "Closed Workload", 
                "Fixed number of virtual users executing iterations",
                TestPlanSpec.WorkLoadModel.CLOSED,
                Set.of("users", "iterations"),
                Set.of("rampUp", "holdFor", "thinkTime"),
                Map.of("users", 1, "iterations", 1),
                false
        ));

        // Register built-in OPEN strategy
        strategyFactories.put(TestPlanSpec.WorkLoadModel.OPEN, 
                (phaseManager, requestExecutor, resourceManager, cancelled) -> 
                        new OpenWorkloadStrategy(phaseManager, requestExecutor, resourceManager, cancelled));

        strategyMetadata.put(TestPlanSpec.WorkLoadModel.OPEN, new StrategyMetadata(
                "Open Workload",
                "Arrival rate-based request generation with concurrency limits",
                TestPlanSpec.WorkLoadModel.OPEN,
                Set.of("arrivalRatePerSec", "duration"),
                Set.of("maxConcurrent", "warmup"),
                Map.of("arrivalRatePerSec", 1, "maxConcurrent", 10),
                false
        ));
    }

    private WorkloadStrategy createStrategyInternal(TestPlanSpec.LoadModel loadModel,
                                                   TestPhaseManager phaseManager,
                                                   RequestExecutor requestExecutor,
                                                   ResourceManager resourceManager,
                                                   AtomicBoolean cancelled) {
        
        TestPlanSpec.WorkLoadModel type = loadModel.getType();
        
        // Check for built-in strategies first
        StrategyFactory factory = strategyFactories.get(type);
        if (factory != null) {
            return factory.create(phaseManager, requestExecutor, resourceManager, cancelled);
        }

        // Check for custom strategies if allowed
        if (config.isCustomStrategiesAllowed()) {
            String customName = extractCustomStrategyName(type);
            if (customName != null) {
                StrategyFactory customFactory = customStrategyFactories.get(customName);
                if (customFactory != null) {
                    return customFactory.create(phaseManager, requestExecutor, resourceManager, cancelled);
                }
            }
        }

        throw new StrategyCreationException("No factory found for load model type: " + type);
    }

    private String extractCustomStrategyName(TestPlanSpec.WorkLoadModel type) {
        String typeName = type.name();
        if (typeName.startsWith("CUSTOM_")) {
            return typeName.substring(7).toLowerCase(); // Remove "CUSTOM_" prefix
        }
        return null;
    }

    private WorkloadStrategy getCachedStrategy(TestPlanSpec.LoadModel loadModel) {
        if (strategyCache == null) return null;
        
        String cacheKey = generateCacheKey(loadModel);
        return strategyCache.get(cacheKey);
    }

    private void cacheStrategy(TestPlanSpec.LoadModel loadModel, WorkloadStrategy strategy) {
        if (strategyCache == null) return;

        String cacheKey = generateCacheKey(loadModel);
        
        // Enforce cache size limit
        if (strategyCache.size() >= config.getMaxCachedStrategies()) {
            // Remove oldest entry (simple LRU approximation)
            String oldestKey = strategyCache.keySet().iterator().next();
            WorkloadStrategy removed = strategyCache.remove(oldestKey);
            if (removed != null) {
                removed.cleanup();
            }
        }

        strategyCache.put(cacheKey, strategy);
    }

    private String generateCacheKey(TestPlanSpec.LoadModel loadModel) {
        return String.format("%s-%d-%d-%s", 
                loadModel.getType(), 
                loadModel.getUsers(),
                loadModel.getArrivalRatePerSec(),
                loadModel.getDuration());
    }

//    private TestPlanSpec createTestPlanSpec(TestPlanSpec.LoadModel loadModel) {
//        // Create a minimal TestPlanSpec for validation purposes
//        // In real implementation, this would be more comprehensive
//        return new TestPlanSpec(null, new TestPlanSpec.ExecutionSpec(loadModel, null, null));
//    }

    // ================================
    // INNER CLASSES
    // ================================

    /**
     * Validator for strategy configurations.
     */
    private static class StrategyValidator {
        private final FactoryConfig config;

        StrategyValidator(FactoryConfig config) {
            this.config = config;
        }

        void validateLoadModel(TestPlanSpec.LoadModel loadModel) throws StrategyValidationException {
            if (loadModel == null) {
                throw new StrategyValidationException("Load model cannot be null");
            }

            if (loadModel.getType() == null) {
                throw new StrategyValidationException("Load model type cannot be null");
            }

            // Type-specific validation
            switch (loadModel.getType()) {
                case CLOSED -> validateClosedModel(loadModel);
                case OPEN -> validateOpenModel(loadModel);
                default -> {
                    if (!config.isCustomStrategiesAllowed()) {
                        throw new StrategyValidationException("Unknown load model type: " + loadModel.getType());
                    }
                }
            }
        }

        private void validateClosedModel(TestPlanSpec.LoadModel loadModel) throws StrategyValidationException {
            if (loadModel.getUsers() <= 0) {
                throw new StrategyValidationException("CLOSED model requires users > 0");
            }
            if (loadModel.getIterations() <= 0) {
                throw new StrategyValidationException("CLOSED model requires iterations > 0");
            }
        }

        private void validateOpenModel(TestPlanSpec.LoadModel loadModel) throws StrategyValidationException {
            if (loadModel.getArrivalRatePerSec() <= 0) {
                throw new StrategyValidationException("OPEN model requires arrivalRatePerSec > 0");
            }
            if (loadModel.getDuration() == null || loadModel.getDuration().isEmpty()) {
                throw new StrategyValidationException("OPEN model requires duration");
            }
            if (loadModel.getMaxConcurrent() <= 0) {
                throw new StrategyValidationException("OPEN model requires maxConcurrent > 0");
            }
        }
    }

    /**
     * Metrics collector for factory operations.
     */
    private static class FactoryMetricsCollector {
        private final FactoryConfig config;
        private final AtomicLong totalCreated = new AtomicLong(0);
        private final AtomicLong cacheHits = new AtomicLong(0);
        private final AtomicLong validationFailures = new AtomicLong(0);
        private final AtomicLong creationFailures = new AtomicLong(0);
        private final Map<TestPlanSpec.WorkLoadModel, AtomicLong> typeDistribution = new ConcurrentHashMap<>();
        private final Map<String, AtomicLong> customUsage = new ConcurrentHashMap<>();
        private final List<Long> creationTimes = Collections.synchronizedList(new ArrayList<>());
        private volatile Instant lastCreated;

        FactoryMetricsCollector(FactoryConfig config) {
            this.config = config;
        }

        void recordStrategyCreation(TestPlanSpec.WorkLoadModel type, Duration duration, boolean success) {
            if (!config.isMetricsEnabled()) return;

            if (success) {
                totalCreated.incrementAndGet();
                lastCreated = Instant.now();
                
                typeDistribution.computeIfAbsent(type, k -> new AtomicLong(0)).incrementAndGet();
                
                // Keep only recent timing data
                synchronized (creationTimes) {
                    creationTimes.add(duration.toNanos() / 1_000_000); // Convert to milliseconds
                    if (creationTimes.size() > 100) {
                        creationTimes.remove(0);
                    }
                }
            } else {
                creationFailures.incrementAndGet();
            }
        }

        void recordCacheHit() {
            if (config.isMetricsEnabled()) {
                cacheHits.incrementAndGet();
            }
        }

        void recordValidationFailure() {
            if (config.isMetricsEnabled()) {
                validationFailures.incrementAndGet();
            }
        }

        FactoryMetrics getMetrics() {
            Map<TestPlanSpec.WorkLoadModel, Long> typeDistMap = new HashMap<>();
            typeDistribution.forEach((k, v) -> typeDistMap.put(k, v.get()));

            Map<String, Long> customUsageMap = new HashMap<>();
            customUsage.forEach((k, v) -> customUsageMap.put(k, v.get()));

            double avgCreationTime;
            synchronized (creationTimes) {
                avgCreationTime = creationTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
            }

            return new FactoryMetrics(
                    totalCreated.get(), cacheHits.get(), validationFailures.get(), creationFailures.get(),
                    typeDistMap, customUsageMap, avgCreationTime, lastCreated
            );
        }
    }

    /**
     * Result of validation operations.
     */
    public static class ValidationResult {
        private final boolean success;
        private final String message;
        private final List<String> errors;

        private ValidationResult(boolean success, String message, List<String> errors) {
            this.success = success;
            this.message = message;
            this.errors = List.copyOf(errors);
        }

        public static ValidationResult success(String message) {
            return new ValidationResult(true, message, List.of());
        }

        public static ValidationResult failure(String message) {
            return new ValidationResult(false, message, List.of(message));
        }

        public static ValidationResult failure(String message, List<String> errors) {
            return new ValidationResult(false, message, errors);
        }

        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
        public List<String> getErrors() { return errors; }

        @Override
        public String toString() {
            return String.format("ValidationResult{success=%s, message='%s', errors=%d}", 
                    success, message, errors.size());
        }
    }

    // ================================
    // CUSTOM EXCEPTIONS
    // ================================

    /**
     * Exception thrown when strategy creation fails.
     */
    public static class StrategyCreationException extends RuntimeException {
        public StrategyCreationException(String message) {
            super(message);
        }

        public StrategyCreationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Exception thrown when strategy validation fails.
     */
    public static class StrategyValidationException extends Exception {
        public StrategyValidationException(String message) {
            super(message);
        }

        public StrategyValidationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Exception thrown when strategy initialization fails.
     */
    public static class StrategyInitializationException extends Exception {
        public StrategyInitializationException(String message) {
            super(message);
        }

        public StrategyInitializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}