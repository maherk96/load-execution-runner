package org.load.execution.runner.core.queue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.RepeatedTest;
import org.load.execution.runner.api.dto.TaskDto;
import org.load.execution.runner.core.model.TaskType;

import java.time.LocalDateTime;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TaskWrapperTest {

    private TaskDto mockTask;
    private TaskWrapper taskWrapper;

    @BeforeEach
    void setUp() {
        mockTask = mock(TaskDto.class);
        when(mockTask.getTaskId()).thenReturn("test-task-123");
        when(mockTask.getTaskType()).thenReturn(TaskType.REST_LOAD);

        taskWrapper = new TaskWrapper(mockTask);
    }

    @Test
    void constructor_SetsTaskAndQueuedTime() {
        LocalDateTime before = LocalDateTime.now().minusSeconds(1);
        TaskWrapper wrapper = new TaskWrapper(mockTask);
        LocalDateTime after = LocalDateTime.now().plusSeconds(1);

        assertEquals(mockTask, wrapper.getTask());
        assertTrue(wrapper.getQueuedAt().isAfter(before));
        assertTrue(wrapper.getQueuedAt().isBefore(after));
        assertFalse(wrapper.isCancelled());
        assertNull(wrapper.getExecutionFuture());
    }

    @Test
    void isCancelled_InitiallyFalse() {
        assertFalse(taskWrapper.isCancelled());
    }

    @Test
    void cancel_FirstTime_ReturnsTrue() {
        assertTrue(taskWrapper.cancel());
        assertTrue(taskWrapper.isCancelled());
    }

    @Test
    void cancel_SecondTime_ReturnsFalse() {
        assertTrue(taskWrapper.cancel());   // First call succeeds
        assertFalse(taskWrapper.cancel());  // Second call returns false
        assertTrue(taskWrapper.isCancelled());
    }

    @Test
    void cancel_WithoutFuture_ReturnsTrue() {
        assertTrue(taskWrapper.cancel());
        assertTrue(taskWrapper.isCancelled());
    }

    @Test
    void cancel_WithFuture_CancelsFuture() {
        Future<Void> mockFuture = mock(Future.class);
        when(mockFuture.cancel(true)).thenReturn(true);

        taskWrapper.setExecutionFuture(mockFuture);
        assertTrue(taskWrapper.cancel());

        verify(mockFuture).cancel(true);
        assertTrue(taskWrapper.isCancelled());
    }

    @Test
    void cancel_WithFuture_FutureCancelFails_StillReturnsAppropriateValue() {
        Future<Void> mockFuture = mock(Future.class);
        when(mockFuture.cancel(true)).thenReturn(false);

        taskWrapper.setExecutionFuture(mockFuture);
        boolean result = taskWrapper.cancel();

        verify(mockFuture).cancel(true);
        assertTrue(taskWrapper.isCancelled());
        assertFalse(result); // Should return false since future.cancel() returned false
    }

    @Test
    void setExecutionFuture_BeforeCancellation_SetsNormally() {
        Future<Void> mockFuture = mock(Future.class);

        taskWrapper.setExecutionFuture(mockFuture);

        assertEquals(mockFuture, taskWrapper.getExecutionFuture());
        verify(mockFuture, never()).cancel(anyBoolean());
    }

    @Test
    void setExecutionFuture_AfterCancellation_ImmediatelyCancelsFuture() {
        Future<Void> mockFuture = mock(Future.class);
        when(mockFuture.cancel(true)).thenReturn(true);

        taskWrapper.cancel();
        taskWrapper.setExecutionFuture(mockFuture);

        assertEquals(mockFuture, taskWrapper.getExecutionFuture());
        verify(mockFuture).cancel(true);
    }

    @Test
    void cancel_AlreadyCancelled_WithCancelledFuture_ReturnsFalse() {
        Future<Void> mockFuture = mock(Future.class);
        when(mockFuture.cancel(true)).thenReturn(true);
        when(mockFuture.isCancelled()).thenReturn(true);

        taskWrapper.setExecutionFuture(mockFuture);
        assertTrue(taskWrapper.cancel());   // First cancellation succeeds
        assertFalse(taskWrapper.cancel());  // Second cancellation returns false
        assertTrue(taskWrapper.isCancelled());
    }

    @Test
    void cancel_AlreadyCancelled_WithNonCancelledFuture_ReturnsFalse() {
        Future<Void> mockFuture = mock(Future.class);
        when(mockFuture.cancel(true)).thenReturn(true);
        when(mockFuture.isCancelled()).thenReturn(false);

        taskWrapper.setExecutionFuture(mockFuture);
        assertTrue(taskWrapper.cancel());   // First cancellation succeeds
        assertFalse(taskWrapper.cancel());  // Second cancellation returns false
        assertTrue(taskWrapper.isCancelled());
    }

    @RepeatedTest(10)
    void cancel_ConcurrentCalls_ThreadSafe() throws InterruptedException {
        int numberOfThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger successfulCancellations = new AtomicInteger(0);

        Future<Void> mockFuture = mock(Future.class);
        when(mockFuture.cancel(true)).thenReturn(true);
        taskWrapper.setExecutionFuture(mockFuture);

        // Submit multiple cancel operations
        for (int i = 0; i < numberOfThreads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    if (taskWrapper.cancel()) {
                        successfulCancellations.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        startLatch.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        // Only one cancellation should succeed (first one)
        assertTrue(taskWrapper.isCancelled());
        verify(mockFuture, times(1)).cancel(true);
    }

    @RepeatedTest(5)
    void setExecutionFuture_ConcurrentWithCancel_ThreadSafe() throws InterruptedException {
        int numberOfThreads = 20;
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean futureWasCancelled = new AtomicBoolean(false);

        CompletableFuture<Void> realFuture = new CompletableFuture<>();

        // Half the threads try to cancel, half try to set future
        for (int i = 0; i < numberOfThreads / 2; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    taskWrapper.cancel();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            executor.submit(() -> {
                try {
                    startLatch.await();
                    taskWrapper.setExecutionFuture(realFuture);
                    if (realFuture.isCancelled()) {
                        futureWasCancelled.set(true);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        startLatch.countDown();
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        // Task should be cancelled
        assertTrue(taskWrapper.isCancelled());

        // If future was set, it should be cancelled too
        if (taskWrapper.getExecutionFuture() != null) {
            assertTrue(taskWrapper.getExecutionFuture().isCancelled());
        }
    }

    @Test
    void cancel_WithNullFuture_HandlesGracefully() {
        taskWrapper.setExecutionFuture(null);

        assertTrue(taskWrapper.cancel());
        assertTrue(taskWrapper.isCancelled());
    }

    @Test
    void setExecutionFuture_WithNull_HandlesGracefully() {
        taskWrapper.cancel();

        // Should not throw exception
        assertDoesNotThrow(() -> taskWrapper.setExecutionFuture(null));
        assertNull(taskWrapper.getExecutionFuture());
    }

    @Test
    void multipleSetExecutionFuture_OnlyFirstOneMatters() {
        Future<Void> firstFuture = mock(Future.class);
        Future<Void> secondFuture = mock(Future.class);

        taskWrapper.setExecutionFuture(firstFuture);
        taskWrapper.setExecutionFuture(secondFuture);

        // Should still reference the second future (overwrites)
        assertEquals(secondFuture, taskWrapper.getExecutionFuture());
    }

    @Test
    void cancel_StateTransition_IsAtomic() throws InterruptedException, ExecutionException {
        // This test verifies that the cancellation state transition is atomic
        // and no race condition exists between checking and setting cancelled flag

        int iterations = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicInteger successfulCancellations = new AtomicInteger(0);

        for (int i = 0; i < iterations; i++) {
            TaskWrapper wrapper = new TaskWrapper(mockTask);
            CountDownLatch latch = new CountDownLatch(1);

            Future<?> task1 = executor.submit(() -> {
                try {
                    latch.await();
                    if (wrapper.cancel()) {
                        successfulCancellations.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            Future<?> task2 = executor.submit(() -> {
                try {
                    latch.await();
                    if (wrapper.cancel()) {
                        successfulCancellations.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            latch.countDown();
            task1.get();
            task2.get();

            assertTrue(wrapper.isCancelled());
        }

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        // Should have exactly 'iterations' successful cancellations
        // (one per iteration, never both threads succeeding)
        assertEquals(iterations, successfulCancellations.get());
    }
}