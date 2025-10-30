package com.mk.fx.qa.load.execution.executors.closed;

@FunctionalInterface
public interface VirtualUserIterationRunner {
  void run(int userIndex, int iteration) throws Exception;
}
