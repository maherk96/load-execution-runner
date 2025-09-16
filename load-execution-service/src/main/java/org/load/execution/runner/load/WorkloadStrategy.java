package org.load.execution.runner.load;

public interface WorkloadStrategy {
    void execute(TestPlanSpec testPlanSpec);
}