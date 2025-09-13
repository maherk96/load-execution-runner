package org.load.execution.runner.core.history;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.load.execution.runner.api.dto.TaskDto;
import org.load.execution.runner.api.dto.TaskExecution;
import org.load.execution.runner.config.TaskQueueConfig;
import org.load.execution.runner.core.model.TaskStatus;
import org.load.execution.runner.core.model.TaskType;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TaskHistoryServiceTest {

    private TaskQueueConfig config;

    @Mock
    private TaskDto taskDto;

    private TaskHistoryService historyService;

    @BeforeEach
    void setUp() {

        config = new TaskQueueConfig();
        config.setTaskHistoryRetentionHours(2);

        historyService = new TaskHistoryService(config);
    }

    @Test
    void recordQueued_ShouldAddTaskToHistoryAndActiveTasks() {
        when(taskDto.getTaskId()).thenReturn("t1");
        when(taskDto.getTaskType()).thenReturn(TaskType.LOAD_TEST);

        historyService.recordQueued(taskDto, 1);

        TaskExecution execution = historyService.getTaskExecution("t1");
        assertThat(execution).isNotNull();
        assertThat(execution.getStatus()).isEqualTo(TaskStatus.QUEUED);
        assertThat(historyService.isTaskActive("t1")).isTrue();
        assertThat(historyService.getActiveTaskCount()).isEqualTo(1);
    }

    @Test
    void markProcessing_ShouldUpdateTaskStatusToProcessing() {
        setupQueuedTask("t2");
        historyService.markProcessing("t2");

        TaskExecution execution = historyService.getTaskExecution("t2");
        assertThat(execution.getStatus()).isEqualTo(TaskStatus.PROCESSING);
        assertThat(execution.getStartedAt()).isNotNull();
    }

    @Test
    void markCompleted_ShouldUpdateTaskStatusAndRemoveFromActiveTasks() {
        setupQueuedTask("t3");
        historyService.markCompleted("t3", 123);

        TaskExecution execution = historyService.getTaskExecution("t3");
        assertThat(execution.getStatus()).isEqualTo(TaskStatus.COMPLETED);
        assertThat(execution.getCompletedAt()).isNotNull();
        assertThat(execution.getProcessingTimeMs()).isEqualTo(123);
        assertThat(historyService.isTaskActive("t3")).isFalse();
    }

    @Test
    void markFailed_ShouldUpdateTaskStatusToFailed() {
        setupQueuedTask("t4");
        historyService.markFailed("t4", 200, "Some error");

        TaskExecution execution = historyService.getTaskExecution("t4");
        assertThat(execution.getStatus()).isEqualTo(TaskStatus.FAILED);
        assertThat(execution.getErrorMessage()).contains("Some error");
    }

    @Test
    void markFailed_WhenErrorContainsTimeout_ShouldMarkAsTimeout() {
        setupQueuedTask("t5");
        historyService.markFailed("t5", 200, "timeout while processing");

        TaskExecution execution = historyService.getTaskExecution("t5");
        assertThat(execution.getStatus()).isEqualTo(TaskStatus.TIMEOUT);
    }

    @Test
    void markCancelled_ShouldUpdateTaskStatusAndRemoveFromActiveTasks() {
        setupQueuedTask("t6");
        historyService.markCancelled("t6", 50, "Cancelled by user");

        TaskExecution execution = historyService.getTaskExecution("t6");
        assertThat(execution.getStatus()).isEqualTo(TaskStatus.CANCELLED);
        assertThat(execution.getErrorMessage()).contains("Cancelled by user");
        assertThat(historyService.isTaskActive("t6")).isFalse();
    }

    @Test
    void getTaskHistory_ShouldReturnTasksInDescendingOrder() {
        setupQueuedTask("t7");
        sleep(10);
        setupQueuedTask("t8");

        List<TaskExecution> history = historyService.getTaskHistory(10);
        assertThat(history.get(0).getTaskId()).isEqualTo("t8");
        assertThat(history.get(1).getTaskId()).isEqualTo("t7");
    }

    @Test
    void getTasksByStatus_ShouldFilterCorrectly() {
        setupQueuedTask("t9");
        historyService.markProcessing("t9");
        setupQueuedTask("t10");

        List<TaskExecution> processingTasks = historyService.getTasksByStatus(TaskStatus.PROCESSING, 10);
        assertThat(processingTasks).hasSize(1);
        assertThat(processingTasks.get(0).getTaskId()).isEqualTo("t9");
    }

    @Test
    void getStatusBreakdown_ShouldReturnCountsPerStatus() {
        setupQueuedTask("t11");
        setupQueuedTask("t12");
        historyService.markProcessing("t12");

        Map<TaskStatus, Long> breakdown = historyService.getStatusBreakdown();
        assertThat(breakdown.get(TaskStatus.QUEUED)).isEqualTo(1);
        assertThat(breakdown.get(TaskStatus.PROCESSING)).isEqualTo(1);
    }

    @Test
    void cleanupTaskHistory_ShouldRemoveOldTasks() throws Exception {
        setupQueuedTask("t13");

        // Get the internal taskHistory map via reflection
        var field = TaskHistoryService.class.getDeclaredField("taskHistory");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, TaskExecution> taskHistory =
                (ConcurrentHashMap<String, TaskExecution>) field.get(historyService);

        TaskExecution existing = taskHistory.get("t13");

        // Replace with a backdated COMPLETED execution
        TaskExecution modified = new TaskExecution(
                existing.getTaskId(),
                existing.getTaskType(),
                TaskStatus.COMPLETED,
                existing.getQueuedAt().minusHours(3), // backdate by 3 hours
                existing.getStartedAt(),
                LocalDateTime.now().minusHours(3),    // completed > 2h ago
                123,
                null,
                existing.getQueuePosition()
        );

        taskHistory.put("t13", modified); // override entry

        // Run cleanup (retention = 2 hours in @BeforeEach)
        historyService.cleanupTaskHistory();

        assertThat(historyService.getHistorySize()).isZero();
    }

    // ===== Helper =====

    private void setupQueuedTask(String taskId) {
        when(taskDto.getTaskId()).thenReturn(taskId);
        when(taskDto.getTaskType()).thenReturn(TaskType.LOAD_TEST);
        historyService.recordQueued(taskDto, 1);
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
        }
    }
}