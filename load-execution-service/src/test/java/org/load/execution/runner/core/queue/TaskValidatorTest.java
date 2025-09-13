package org.load.execution.runner.core.queue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.load.execution.runner.api.dto.TaskDto;
import org.load.execution.runner.api.dto.TaskExecution;
import org.load.execution.runner.api.exception.DuplicateTaskException;
import org.load.execution.runner.api.exception.InvalidTaskException;
import org.load.execution.runner.api.exception.NoProcessorException;
import org.load.execution.runner.api.exception.NoProcessorsException;
import org.load.execution.runner.api.exception.QueueCapacityException;
import org.load.execution.runner.api.exception.ServiceShutdownException;
import org.load.execution.runner.api.exception.TaskValidationException;
import org.load.execution.runner.config.TaskQueueConfig;
import org.load.execution.runner.core.history.TaskHistoryService;
import org.load.execution.runner.core.model.TaskType;
import org.load.execution.runner.core.processor.TaskProcessorManager;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TaskValidatorTest {

    @Mock
    private TaskQueueConfig config;

    @Mock
    private TaskProcessorManager processorManager;

    @Mock
    private TaskHistoryService historyService;

    @Mock
    private TaskDto validTask;

    @Mock
    private TaskExecution taskExecution;

    private TaskValidator validator;

    @BeforeEach
    void setUp() {
        validator = new TaskValidator(config, processorManager, historyService);
    }
    // ========== validateServiceState() Tests ==========

    @Test
    void validateServiceState_WhenShuttingDown_ShouldThrowServiceShutdownException() {
        // Given
        validator.setShuttingDown(true);

        // When/Then
        assertThatThrownBy(() -> validator.validateServiceState())
                .isInstanceOf(ServiceShutdownException.class)
                .hasMessage("Service is shutting down");
    }

    @Test
    void validateServiceState_WhenNoProcessorsAvailable_ShouldThrowNoProcessorsException() {
        // Given
        validator.setShuttingDown(false);
        when(processorManager.hasProcessors()).thenReturn(false);

        // When/Then
        assertThatThrownBy(() -> validator.validateServiceState())
                .isInstanceOf(NoProcessorsException.class)
                .hasMessage("No task processors are available");
    }

    @Test
    void validateServiceState_WhenServiceRunningAndProcessorsAvailable_ShouldPass() {
        // Given
        validator.setShuttingDown(false);
        when(processorManager.hasProcessors()).thenReturn(true);

        // When/Then
        assertDoesNotThrow(() -> validator.validateServiceState());
    }

    // ========== validateTask() Tests ==========

    @Test
    void validateTask_WhenTaskIsNull_ShouldThrowInvalidTaskException() {
        // When/Then
        assertThatThrownBy(() -> validator.validateTask(null))
                .isInstanceOf(InvalidTaskException.class)
                .hasMessage("Task cannot be null");
    }

    @Test
    void validateTask_WhenTaskIdIsNull_ShouldThrowInvalidTaskException() {
        // Given
        when(validTask.getTaskId()).thenReturn(null);

        // When/Then
        assertThatThrownBy(() -> validator.validateTask(validTask))
                .isInstanceOf(InvalidTaskException.class)
                .hasMessage("Task ID is required");
    }

    @Test
    void validateTask_WhenTaskIdIsEmpty_ShouldThrowInvalidTaskException() {
        // Given
        when(validTask.getTaskId()).thenReturn("   ");

        // When/Then
        assertThatThrownBy(() -> validator.validateTask(validTask))
                .isInstanceOf(InvalidTaskException.class)
                .hasMessage("Task ID is required");
    }

    @Test
    void validateTask_WhenTaskIdTooLong_ShouldThrowInvalidTaskException() {
        // Given
        String longTaskId = "a".repeat(101); // Exceeds maxTaskIdLength of 100
        when(validTask.getTaskId()).thenReturn(longTaskId);
        when(config.getMaxTaskIdLength()).thenReturn(100);

        // When/Then
        assertThatThrownBy(() -> validator.validateTask(validTask))
                .isInstanceOf(InvalidTaskException.class)
                .hasMessage("Task ID too long (max 100 characters)");
    }

    @Test
    void validateTask_WhenTaskTypeIsNull_ShouldThrowInvalidTaskException() {
        when(config.getMaxTaskIdLength()).thenReturn(100);
        when(validTask.getTaskType()).thenReturn(null);
        when(validTask.getTaskId()).thenReturn("valid-id");

        assertThatThrownBy(() -> validator.validateTask(validTask))
                .isInstanceOf(InvalidTaskException.class)
                .hasMessage("Task type is required");
    }

    @Test
    void validateTask_WhenCreatedAtIsNull_ShouldThrowInvalidTaskException() {
        // Given
        when(validTask.getCreatedAt()).thenReturn(null);
        when(validTask.getTaskId()).thenReturn("valid-id");
        when(validTask.getTaskType()).thenReturn(TaskType.LOAD_TEST);
        when(config.getMaxTaskIdLength()).thenReturn(100);

        // When/Then
        assertThatThrownBy(() -> validator.validateTask(validTask))
                .isInstanceOf(InvalidTaskException.class)
                .hasMessage("Task creation timestamp is required");
    }

    @Test
    void validateTask_WhenTaskTooOld_ShouldThrowInvalidTaskException() {
        // Given
        LocalDateTime oldTimestamp = LocalDateTime.now().minusHours(25); // Exceeds maxTaskAgeHours of 24
        when(validTask.getCreatedAt()).thenReturn(oldTimestamp);
        when(config.getMaxTaskAgeHours()).thenReturn(24);
        when(validTask.getTaskId()).thenReturn("valid-id");
        when(validTask.getTaskType()).thenReturn(TaskType.LOAD_TEST);
        when(validTask.getTaskId()).thenReturn("valid-id");
        when(config.getMaxTaskIdLength()).thenReturn(100);
        // When/Then
        assertThatThrownBy(() -> validator.validateTask(validTask))
                .isInstanceOf(InvalidTaskException.class)
                .hasMessageContaining("Task too old")
                .hasMessageContaining("Maximum age is 24 hours");
    }

    @Test
    void validateTask_WhenDataIsNull_ShouldThrowInvalidTaskException() {
        // Given
        when(validTask.getData()).thenReturn(null);
        when(validTask.getTaskId()).thenReturn("valid-id");
        when(validTask.getTaskType()).thenReturn(TaskType.LOAD_TEST);
        when(validTask.getCreatedAt()).thenReturn(LocalDateTime.now().minusHours(1));
        when(config.getMaxTaskIdLength()).thenReturn(100);
        when(config.getMaxTaskAgeHours()).thenReturn(24);

        // When/Then
        assertThatThrownBy(() -> validator.validateTask(validTask))
                .isInstanceOf(InvalidTaskException.class)
                .hasMessage("Task data cannot be null");
    }

    @Test
    void validateTask_WhenAllFieldsValid_ShouldPass() {
        // When/Then
        when(config.getMaxTaskIdLength()).thenReturn(100);
        when(config.getMaxTaskAgeHours()).thenReturn(24);
        when(validTask.getTaskId()).thenReturn("valid-id");
        when(validTask.getTaskType()).thenReturn(TaskType.LOAD_TEST);
        when(validTask.getCreatedAt()).thenReturn(LocalDateTime.now().minusHours(1));
        when(validTask.getData()).thenReturn(new HashMap<>());
        assertDoesNotThrow(() -> validator.validateTask(validTask));
    }


    // ========== checkDuplicate() Tests ==========

    @Test
    void checkDuplicate_WhenTaskIsActive_ShouldThrowDuplicateTaskException() {
        // Given
        String taskId = "duplicate-task";
        when(validTask.getTaskId()).thenReturn(taskId);
        when(historyService.isTaskActive(taskId)).thenReturn(true);

        // When/Then
        assertThatThrownBy(() -> validator.checkDuplicate(validTask))
                .isInstanceOf(DuplicateTaskException.class)
                .hasMessage("Task with this ID is already active");
    }

    @Test
    void checkDuplicate_WhenTaskCompletedRecentlyWithinOneHour_ShouldThrowDuplicateTaskException() {
        // Given
        String taskId = "recent-task";
        when(validTask.getTaskId()).thenReturn(taskId);
        when(historyService.isTaskActive(taskId)).thenReturn(false);

        LocalDateTime recentCompletion = LocalDateTime.now().minusMinutes(30);
        when(taskExecution.getCompletedAt()).thenReturn(recentCompletion);
        when(historyService.getTaskExecution(taskId)).thenReturn(taskExecution);

        // When/Then
        assertThatThrownBy(() -> validator.checkDuplicate(validTask))
                .isInstanceOf(DuplicateTaskException.class)
                .hasMessageContaining("Task with this ID was completed")
                .hasMessageContaining("hours ago");
    }

    @Test
    void checkDuplicate_WhenTaskCompletedMoreThanOneHourAgo_ShouldPass() {
        // Given
        String taskId = "old-task";
        when(validTask.getTaskId()).thenReturn(taskId);
        when(historyService.isTaskActive(taskId)).thenReturn(false);

        LocalDateTime oldCompletion = LocalDateTime.now().minusHours(2);
        when(taskExecution.getCompletedAt()).thenReturn(oldCompletion);
        when(historyService.getTaskExecution(taskId)).thenReturn(taskExecution);

        // When/Then
        assertDoesNotThrow(() -> validator.checkDuplicate(validTask));
    }

    @Test
    void checkDuplicate_WhenTaskExecutionNotFound_ShouldPass() {
        // Given
        String taskId = "new-task";
        when(validTask.getTaskId()).thenReturn(taskId);
        when(historyService.isTaskActive(taskId)).thenReturn(false);
        when(historyService.getTaskExecution(taskId)).thenReturn(null);

        // When/Then
        assertDoesNotThrow(() -> validator.checkDuplicate(validTask));
    }

    @Test
    void checkDuplicate_WhenTaskExecutionHasNoCompletionTime_ShouldPass() {
        // Given
        String taskId = "incomplete-task";
        when(validTask.getTaskId()).thenReturn(taskId);
        when(historyService.isTaskActive(taskId)).thenReturn(false);
        when(taskExecution.getCompletedAt()).thenReturn(null);
        when(historyService.getTaskExecution(taskId)).thenReturn(taskExecution);

        // When/Then
        assertDoesNotThrow(() -> validator.checkDuplicate(validTask));
    }

    // ========== validateBusinessRules() Tests ==========

    @Test
    void validateBusinessRules_WhenNoProcessorForTaskType_ShouldThrowNoProcessorException() {
        // Given
        TaskType taskType = TaskType.LOAD_TEST;
        when(validTask.getTaskType()).thenReturn(taskType);
        when(processorManager.hasProcessor(taskType)).thenReturn(false);
        when(processorManager.getAvailableTaskTypes()).thenReturn(Set.of(TaskType.LOAD_TEST));

        // When/Then
        assertThatThrownBy(() -> validator.validateBusinessRules(validTask, 10))
                .isInstanceOf(NoProcessorException.class)
                .hasMessageContaining("No processor available for task type: LOAD_TEST. Available types: [LOAD_TEST]");
    }

    @Test
    void validateBusinessRules_WhenQueueAtCapacity_ShouldThrowQueueCapacityException() {
        // Given
        when(validTask.getTaskType()).thenReturn(TaskType.LOAD_TEST);
        when(processorManager.hasProcessor(TaskType.LOAD_TEST)).thenReturn(true);
        int queueSize = 1000; // Equals maxQueueSize

        // When/Then
        assertThatThrownBy(() -> validator.validateBusinessRules(validTask, queueSize))
                .isInstanceOf(QueueCapacityException.class)
                .hasMessage("Queue capacity exceeded (1000 tasks). Please try again later.");
    }

    @Test
    void validateBusinessRules_WhenProcessorValidationFails_ShouldThrowTaskValidationException() {
        // Given
        when(validTask.getTaskType()).thenReturn(TaskType.LOAD_TEST);
        when(processorManager.hasProcessor(TaskType.LOAD_TEST)).thenReturn(true);
        when(config.getMaxQueueSize()).thenReturn(1000);
        doThrow(new IllegalArgumentException("Invalid load test configuration"))
                .when(processorManager).validateTask(validTask);

        // When/Then
        assertThatThrownBy(() -> validator.validateBusinessRules(validTask, 10))
                .isInstanceOf(TaskValidationException.class)
                .hasMessage("Task validation failed: Invalid load test configuration");
    }

    @Test
    void validateBusinessRules_WhenAllRulesPass_ShouldPass() {
        // Given
        when(validTask.getTaskType()).thenReturn(TaskType.LOAD_TEST);
        when(processorManager.hasProcessor(TaskType.LOAD_TEST)).thenReturn(true);
        when(config.getMaxQueueSize()).thenReturn(1000);
        doNothing().when(processorManager).validateTask(validTask);

        // When/Then
        assertDoesNotThrow(() -> validator.validateBusinessRules(validTask, 10));

        // Verify all validations were called
        verify(processorManager).hasProcessor(TaskType.LOAD_TEST);
        verify(processorManager).validateTask(validTask);
    }

    @Test
    void validateBusinessRules_WhenQueueSizeAtBoundary_ShouldPass() {
        // Given
        when(config.getMaxQueueSize()).thenReturn(1000);
        when(validTask.getTaskType()).thenReturn(TaskType.LOAD_TEST);
        when(processorManager.hasProcessor(TaskType.LOAD_TEST)).thenReturn(true);
        doNothing().when(processorManager).validateTask(validTask);
        int queueSize = 999; // Just under maxQueueSize of 1000

        // When/Then
        assertDoesNotThrow(() -> validator.validateBusinessRules(validTask, queueSize));
    }

    // ========== Integration Tests ==========

    @Test
    void validateTask_WithRealTaskDto_ShouldValidateAllFields() {
        // Given
        when(config.getMaxTaskIdLength()).thenReturn(100);
        when(config.getMaxTaskAgeHours()).thenReturn(24);

        TaskDto realTask = new TaskDto();
        realTask.setTaskId("real-task-123");
        realTask.setTaskType(TaskType.LOAD_TEST);
        realTask.setCreatedAt(LocalDateTime.now().minusHours(1));

        Map<String, Object> data = new HashMap<>();
        data.put("url", "https://example.com");
        data.put("users", 10);
        realTask.setData(data);

        // When/Then
        assertDoesNotThrow(() -> validator.validateTask(realTask));
    }

    @Test
    void checkDuplicate_WithBoundaryTimingOneHour_ShouldPass() {
        // Given
        String taskId = "boundary-task";
        when(validTask.getTaskId()).thenReturn(taskId);
        when(historyService.isTaskActive(taskId)).thenReturn(false);

        LocalDateTime exactlyOneHourAgo = LocalDateTime.now().minusHours(1).minusSeconds(1);
        when(taskExecution.getCompletedAt()).thenReturn(exactlyOneHourAgo);
        when(historyService.getTaskExecution(taskId)).thenReturn(taskExecution);

        // When/Then
        assertDoesNotThrow(() -> validator.checkDuplicate(validTask));
    }

    // ========== Constructor and Field Tests ==========

    @Test
    void constructor_ShouldSetAllFields() {
        // When
        TaskValidator newValidator = new TaskValidator(config, processorManager, historyService);

        // Then
        assertThat(newValidator.getConfig()).isEqualTo(config);
        assertThat(newValidator.getProcessorManager()).isEqualTo(processorManager);
        assertThat(newValidator.getHistoryService()).isEqualTo(historyService);
        assertThat(newValidator.isShuttingDown()).isFalse();
    }

    @Test
    void setShuttingDown_ShouldUpdateFlag() {
        // Given
        assertThat(validator.isShuttingDown()).isFalse();

        // When
        validator.setShuttingDown(true);

        // Then
        assertThat(validator.isShuttingDown()).isTrue();
    }
}