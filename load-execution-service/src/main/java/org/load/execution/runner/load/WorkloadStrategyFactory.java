// File: org/load/execution/runner/load/strategy/WorkloadStrategyFactory.java
package org.load.execution.runner.load;


import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Factory for creating workload strategies based on load model type.
 *
 * @author Load Test Framework
 * @since 1.0
 */
public class WorkloadStrategyFactory {

    public WorkloadStrategy createStrategy(TestPlanSpec.LoadModel loadModel,
                                           TestPhaseManager phaseManager,
                                           RequestExecutor requestExecutor,
                                           ResourceManager resourceManager,
                                           LoadMetrics loadMetrics,
                                           AtomicBoolean cancelled) {
        return switch (loadModel.getType()) {
            case CLOSED -> new ClosedWorkloadStrategy(phaseManager, requestExecutor, resourceManager, loadMetrics, cancelled);
            case OPEN -> new OpenWorkloadStrategy(phaseManager, requestExecutor, resourceManager, loadMetrics, cancelled);
        };
    }
}
