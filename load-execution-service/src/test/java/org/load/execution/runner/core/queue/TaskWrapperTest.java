package org.load.execution.runner.core.queue;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.load.execution.runner.api.dto.TaskDto;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TaskWrapperTest {

    @Mock
    private TaskDto mockTask;

    @Mock
    private Future<Void> mockFuture;

    @Test
    void constructor_SetsTaskAndQueuedTime() {
        // Given
        LocalDateTime beforeCreation = LocalDateTime.now();

        // When
        TaskWrapper wrapper = new TaskWrapper(mockTask);

        // Then
        assertThat(wrapper.getTask()).isEqualTo(mockTask);
        assertThat(wrapper.getQueuedAt()).isAfterOrEqualTo(beforeCreation);
        assertThat(wrapper.getQueuedAt()).isBeforeOrEqualTo(LocalDateTime.now());
        assertThat(wrapper.isCancelled()).isFalse();
        assertThat(wrapper.getExecutionFuture()).isNull();
    }

    @Test
    void cancel_WithNullFuture_ReturnsTrueAndSetsCancelledFlag() {
        // Given
        TaskWrapper wrapper = new TaskWrapper(mockTask);
        assertThat(wrapper.getExecutionFuture()).isNull();

        // When
        boolean result = wrapper.cancel();

        // Then
        assertThat(result).isTrue();
        assertThat(wrapper.isCancelled()).isTrue();
    }

    @Test
    void cancel_WithActiveFuture_WhenCancelSucceeds_ReturnsTrueAndSetsCancelledFlag() {
        // Given
        TaskWrapper wrapper = new TaskWrapper(mockTask);
        wrapper.setExecutionFuture(mockFuture);
        when(mockFuture.cancel(true)).thenReturn(true);

        // When
        boolean result = wrapper.cancel();

        // Then
        verify(mockFuture).cancel(true);
        assertThat(result).isTrue();
        assertThat(wrapper.isCancelled()).isTrue();
    }

    @Test
    void cancel_WithActiveFuture_WhenCancelFails_ReturnsFalseAndKeepsCancelledFalse() {
        // Given
        TaskWrapper wrapper = new TaskWrapper(mockTask);
        wrapper.setExecutionFuture(mockFuture);
        when(mockFuture.cancel(true)).thenReturn(false); // Simulate failed cancellation

        // When
        boolean cancelResult = wrapper.cancel();

        // Then - With improved implementation, state is consistent
        assertThat(cancelResult).isFalse(); // Cancel method says it failed
        assertThat(wrapper.isCancelled()).isFalse(); // Object correctly reflects failure

        // Verify the future cancellation was attempted
        verify(mockFuture).cancel(true);
    }

    @Test
    void cancel_CalledMultipleTimes_WhenAlreadyCancelledWithNullFuture_ReturnsTrue() {
        // Given
        TaskWrapper wrapper = new TaskWrapper(mockTask);
        wrapper.cancel(); // First cancellation (no future, sets cancelled=true)

        // When
        boolean secondCall = wrapper.cancel();
        boolean thirdCall = wrapper.cancel();

        // Then
        assertThat(secondCall).isTrue(); // Returns true because future is null
        assertThat(thirdCall).isTrue(); // Returns true because future is null
        assertThat(wrapper.isCancelled()).isTrue();
    }

    @Test
    void cancel_CalledMultipleTimes_WhenAlreadyCancelledWithCancelledFuture_ReturnsTrue() {
        // Given
        TaskWrapper wrapper = new TaskWrapper(mockTask);
        wrapper.setExecutionFuture(mockFuture);
        when(mockFuture.cancel(true)).thenReturn(true);
        when(mockFuture.isCancelled()).thenReturn(true);

        wrapper.cancel(); // First cancellation (succeeds, sets cancelled=true)

        // When
        boolean secondCall = wrapper.cancel();
        boolean thirdCall = wrapper.cancel();

        // Then
        assertThat(secondCall).isTrue(); // Returns true because future.isCancelled() is true
        assertThat(thirdCall).isTrue(); // Returns true because future.isCancelled() is true
        assertThat(wrapper.isCancelled()).isTrue();

        // Future.cancel() only called once, subsequent calls check isCancelled()
        verify(mockFuture, times(1)).cancel(true);
        verify(mockFuture, atLeast(2)).isCancelled();
    }

    @Test
    void cancel_CalledMultipleTimes_WhenPreviousCancelFailed_KeepsRetryingCancel() {
        // Given
        TaskWrapper wrapper = new TaskWrapper(mockTask);
        wrapper.setExecutionFuture(mockFuture);
        when(mockFuture.cancel(true)).thenReturn(false);

        wrapper.cancel(); // First cancellation (fails, cancelled remains false)

        // When
        boolean secondCall = wrapper.cancel();
        boolean thirdCall = wrapper.cancel();

        // Then
        assertThat(secondCall).isFalse(); // Still fails
        assertThat(thirdCall).isFalse(); // Still fails
        assertThat(wrapper.isCancelled()).isFalse(); // Flag remains false since cancellation never succeeded

        // Future.cancel() called multiple times because cancelled flag is still false
        verify(mockFuture, times(3)).cancel(true);
    }

    @Test
    void cancel_WhenCancelledWithActiveFuture_ReturnsCurrentFutureState() {
        // Given
        TaskWrapper wrapper = new TaskWrapper(mockTask);
        wrapper.setExecutionFuture(mockFuture);
        when(mockFuture.cancel(true)).thenReturn(true);
        when(mockFuture.isCancelled()).thenReturn(true);

        wrapper.cancel(); // First call succeeds, sets cancelled=true

        // Now simulate that future state might change
        when(mockFuture.isCancelled()).thenReturn(false); // Hypothetical scenario

        // When
        boolean result = wrapper.cancel();

        // Then
        assertThat(result).isFalse(); // Returns current future state
        assertThat(wrapper.isCancelled()).isTrue(); // Wrapper flag remains true
        verify(mockFuture, times(1)).cancel(true); // Only one actual cancel call
        verify(mockFuture, times(1)).isCancelled(); // Checked current state
    }

    @Test
    void cancel_SetFutureAfterCancellation_SubsequentCancelChecksNewFuture() {
        // Given
        TaskWrapper wrapper = new TaskWrapper(mockTask);
        wrapper.cancel(); // Cancel with no future (sets cancelled=true)

        // Later, someone sets a future
        wrapper.setExecutionFuture(mockFuture);
        when(mockFuture.isCancelled()).thenReturn(true);

        // When
        boolean result = wrapper.cancel();

        // Then
        assertThat(result).isTrue(); // Returns future.isCancelled()
        assertThat(wrapper.isCancelled()).isTrue();
        verify(mockFuture, never()).cancel(true); // No cancel call since already cancelled
        verify(mockFuture).isCancelled(); // Checks the future state
    }

    @Test
    @Disabled
    void cancel_ConcurrentAccess_ThreadSafe() throws InterruptedException {
        // Given
        TaskWrapper wrapper = new TaskWrapper(mockTask);
        when(mockFuture.cancel(true)).thenReturn(true);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(2);
        AtomicInteger successCount = new AtomicInteger(0);

        // When - Simulate concurrent access
        Thread t1 = new Thread(() -> {
            try {
                startLatch.await();
                wrapper.setExecutionFuture(mockFuture);
                if (wrapper.cancel()) {
                    successCount.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                finishLatch.countDown();
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                startLatch.await();
                if (wrapper.cancel()) {
                    successCount.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                finishLatch.countDown();
            }
        });

        t1.start();
        t2.start();

        // Release both threads simultaneously
        startLatch.countDown();

        // Wait for completion
        boolean completed = finishLatch.await(1, TimeUnit.SECONDS);

        // Then
        assertThat(completed).isTrue();

        // At least one cancellation should succeed
        // The exact number depends on timing - if t2 executes first (no future), both succeed
        // If t1 executes first (sets future and cancels), both succeed
        assertThat(successCount.get()).isGreaterThanOrEqualTo(1);
        assertThat(successCount.get()).isLessThanOrEqualTo(2);
    }

    @Test
    void queuedAt_IsImmutableAfterConstruction() {
        // Given
        TaskWrapper wrapper = new TaskWrapper(mockTask);
        LocalDateTime originalQueuedAt = wrapper.getQueuedAt();

        // When - Various operations
        wrapper.setExecutionFuture(mockFuture);
        wrapper.cancel();

        // Then
        assertThat(wrapper.getQueuedAt()).isEqualTo(originalQueuedAt);
    }
}