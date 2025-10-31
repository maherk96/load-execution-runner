package com.mk.fx.qa.load.execution.executors.closed;

/**
 * Callback used by {@link ClosedLoadExecutor} to execute a single iteration for a virtual user.
 * Implementations should perform the actual scenario work for the given user and iteration.
 * Throwing an exception ends the current user's execution, while other users continue.
 */
@FunctionalInterface
public interface VirtualUserIterationRunner {
  /**
   * Executes one iteration for the specified user.
   *
   * @param userIndex zero-based index of the virtual user
   * @param iteration zero-based iteration counter for that user
   * @throws Exception to abort this user's execution; other users are unaffected
   */
  void run(int userIndex, int iteration) throws Exception;
}
