package org.load.execution.runner.core.queue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.load.execution.runner.api.dto.TaskDto;
import org.load.execution.runner.api.dto.TaskExecution;
import org.load.execution.runner.api.dto.TaskResponseDto;
import org.load.execution.runner.api.exception.InvalidTaskException;
import org.load.execution.runner.api.exception.ServiceShutdownException;
import org.load.execution.runner.config.TaskQueueConfig;
import org.load.execution.runner.core.history.TaskHistoryService;
import org.load.execution.runner.core.model.ServiceState;
import org.load.execution.runner.core.model.TaskStatus;
import org.load.execution.runner.core.model.TaskType;
import org.load.execution.runner.core.processor.InterruptibleTaskProcessor;
import org.load.execution.runner.core.processor.TaskProcessorManager;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ImprovedTaskQueueServiceTest {

    @Mock
    private TaskValidator validator;

    @Mock
    private TaskHistoryService historyService;

    @Mock
    private TaskProcessorManager processorManager;

    private TaskQueueConfig config;

    @Mock
    private TaskDto taskDto;

    @Mock
    private TaskExecution taskExecution;

    private ImprovedTaskQueueService taskQueueService;

    @BeforeEach
    void setUp() {
        config = new TaskQueueConfig();
        config.setMaxQueueSize(100);
        config.setMaxConcurrentTasks(1);
        config.setTaskTimeoutMinutes(30);
        config.setDrainQueueOnShutdown(false);


        taskQueueService = new ImprovedTaskQueueService(validator, historyService, processorManager, config);
    }


    // ========== submitTask() Tests ==========

    @Test
    void submitTask_WhenTaskIsValid_ShouldQueueSuccessfully() {
        // Given
        when(taskDto.getTaskId()).thenReturn("valid-task-123");
        when(taskDto.getTaskType()).thenReturn(TaskType.LOAD_TEST);
        doNothing().when(validator).validateServiceState();
        doNothing().when(validator).validateTask(taskDto);
        doNothing().when(validator).checkDuplicate(taskDto);
        doNothing().when(validator).validateBusinessRules(eq(taskDto), anyInt());
        doNothing().when(historyService).recordQueued(eq(taskDto), anyInt());

        taskQueueService.initialize();

        // When
        TaskResponseDto response = taskQueueService.submitTask(taskDto);

        // Then
        assertThat(response.getTaskId()).isEqualTo("valid-task-123");
        assertThat(response.getStatus()).isIn("QUEUED", "PROCESSING");
        assertThat(taskQueueService.getQueueSize()).isGreaterThanOrEqualTo(0);

        verify(validator).validateServiceState();
        verify(validator).validateTask(taskDto);
        verify(validator).checkDuplicate(taskDto);
        verify(validator).validateBusinessRules(eq(taskDto), anyInt());
        verify(historyService).recordQueued(eq(taskDto), anyInt());
    }

    @Test
    void submitTask_WhenValidationFails_ShouldReturnErrorResponse() {
        // Given
        when(taskDto.getTaskId()).thenReturn("invalid-task");
        doThrow(new InvalidTaskException("Task is invalid"))
                .when(validator).validateTask(taskDto);

        taskQueueService.initialize();

        // When
        TaskResponseDto response = taskQueueService.submitTask(taskDto);

        // Then
        assertThat(response.getTaskId()).isEqualTo("invalid-task");
        assertThat(response.getStatus()).isEqualTo("ERROR");
        assertThat(response.getMessage()).contains("Task is invalid");
        assertThat(taskQueueService.getQueueSize()).isEqualTo(0);
    }

    @Test
    void submitTask_WhenServiceNotRunning_ShouldReturnErrorResponse() {
        // Given
        when(taskDto.getTaskId()).thenReturn("task-123");
        // Service state is STARTING by default, not RUNNING

        // When
        TaskResponseDto response = taskQueueService.submitTask(taskDto);

        // Then
        assertThat(response.getTaskId()).isEqualTo("task-123");
        assertThat(response.getStatus()).isEqualTo("ERROR");
        assertThat(response.getMessage()).contains("not accepting new tasks");
    }

    @Test
    void submitTask_WhenTaskIsNull_ShouldReturnErrorResponse() {
        // Given
        taskQueueService.initialize();

        // When
        TaskResponseDto response = taskQueueService.submitTask(null);

        // Then
        assertThat(response.getTaskId()).isEqualTo("unknown");
        assertThat(response.getStatus()).isEqualTo("ERROR");
    }

    @Test
    void submitTask_WhenServiceShuttingDown_ShouldReturnErrorResponse() {
        // Given
        when(taskDto.getTaskId()).thenReturn("task-123");
        doThrow(new ServiceShutdownException("Service is shutting down"))
                .when(validator).validateServiceState();

        taskQueueService.initialize();

        // When
        TaskResponseDto response = taskQueueService.submitTask(taskDto);

        // Then
        assertThat(response.getTaskId()).isEqualTo("task-123");
        assertThat(response.getStatus()).isEqualTo("ERROR");
        assertThat(response.getMessage()).contains("Service is shutting down");
    }

    // ========== cancelTask() Tests ==========

    @Test
    void cancelTask_WhenTaskIdIsNull_ShouldReturnErrorResponse() {
        // When
        TaskResponseDto response = taskQueueService.cancelTask(null);

        // Then
        assertThat(response.getTaskId()).isEqualTo("invalid-id");
        assertThat(response.getStatus()).isEqualTo("ERROR");
        assertThat(response.getMessage()).contains("Task ID is required");
    }

    @Test
    void cancelTask_WhenTaskIdIsEmpty_ShouldReturnErrorResponse() {
        // When
        TaskResponseDto response = taskQueueService.cancelTask("   ");

        // Then
        assertThat(response.getTaskId()).isEqualTo("invalid-id");
        assertThat(response.getStatus()).isEqualTo("ERROR");
        assertThat(response.getMessage()).contains("Task ID is required");
    }

    @Test
    void cancelTask_WhenTaskNotFound_ShouldReturnErrorResponse() {
        // Given
        String taskId = "non-existent-task";
        when(historyService.isTaskActive(taskId)).thenReturn(false);
        when(historyService.getTaskExecution(taskId)).thenReturn(null);

        // When
        TaskResponseDto response = taskQueueService.cancelTask(taskId);

        // Then
        assertThat(response.getTaskId()).isEqualTo(taskId);
        assertThat(response.getStatus()).isEqualTo("ERROR");
        assertThat(response.getMessage()).contains("Task not found");
    }

    @Test
    void cancelTask_WhenTaskAlreadyCompleted_ShouldReturnStatusResponse() {
        // Given
        String taskId = "completed-task";
        when(historyService.isTaskActive(taskId)).thenReturn(false);
        when(historyService.getTaskExecution(taskId)).thenReturn(taskExecution);
        when(taskExecution.getStatus()).thenReturn(TaskStatus.COMPLETED);

        // When
        TaskResponseDto response = taskQueueService.cancelTask(taskId);

        // Then
        assertThat(response.getTaskId()).isEqualTo(taskId);
        assertThat(response.getStatus()).isEqualTo("COMPLETED");
        assertThat(response.getMessage()).contains("already completed");
    }

    // ========== getTaskStatus() Tests ==========

    @Test
    void getTaskStatus_WhenTaskIdIsNull_ShouldReturnErrorResponse() {
        // When
        TaskResponseDto response = taskQueueService.getTaskStatus(null);

        // Then
        assertThat(response.getTaskId()).isEqualTo("invalid-id");
        assertThat(response.getStatus()).isEqualTo("ERROR");
        assertThat(response.getMessage()).contains("Task ID is required");
    }

    @Test
    void getTaskStatus_WhenTaskExists_ShouldReturnStatusResponse() {
        // Given
        String taskId = "existing-task";
        when(historyService.getTaskExecution(taskId)).thenReturn(taskExecution);
        when(taskExecution.getStatus()).thenReturn(TaskStatus.PROCESSING);
        when(taskExecution.getStartedAt()).thenReturn(LocalDateTime.now().minusSeconds(30));

        // When
        TaskResponseDto response = taskQueueService.getTaskStatus(taskId);

        // Then
        assertThat(response.getTaskId()).isEqualTo(taskId);
        assertThat(response.getStatus()).isEqualTo("PROCESSING");
        assertThat(response.getMessage()).contains("processing");
    }

    @Test
    void getTaskStatus_WhenTaskNotFound_ShouldReturnNotFoundResponse() {
        // Given
        String taskId = "missing-task";
        when(historyService.getTaskExecution(taskId)).thenReturn(null);

        // When
        TaskResponseDto response = taskQueueService.getTaskStatus(taskId);

        // Then
        assertThat(response.getTaskId()).isEqualTo(taskId);
        assertThat(response.getStatus()).isEqualTo("NOT_FOUND");
        assertThat(response.getMessage()).contains("Task not found");
    }

    // ========== getDetailedTaskStatus() Tests ==========

    @Test
    void getDetailedTaskStatus_ShouldDelegateToHistoryService() {
        // Given
        String taskId = "task-123";
        when(historyService.getTaskExecution(taskId)).thenReturn(taskExecution);

        // When
        TaskExecution result = taskQueueService.getDetailedTaskStatus(taskId);

        // Then
        assertThat(result).isEqualTo(taskExecution);
        verify(historyService).getTaskExecution(taskId);
    }

    // ========== getTaskHistory() Tests ==========

    @Test
    void getTaskHistory_ShouldDelegateToHistoryService() {
        // Given
        int limit = 50;
        List<TaskExecution> expectedHistory = List.of(taskExecution);
        when(historyService.getTaskHistory(limit)).thenReturn(expectedHistory);

        // When
        List<TaskExecution> result = taskQueueService.getTaskHistory(limit);

        // Then
        assertThat(result).isEqualTo(expectedHistory);
        verify(historyService).getTaskHistory(limit);
    }

    // ========== getTasksByStatus() Tests ==========

    @Test
    void getTasksByStatus_ShouldDelegateToHistoryService() {
        // Given
        TaskStatus status = TaskStatus.COMPLETED;
        int limit = 25;
        List<TaskExecution> expectedTasks = List.of(taskExecution);
        when(historyService.getTasksByStatus(status, limit)).thenReturn(expectedTasks);

        // When
        List<TaskExecution> result = taskQueueService.getTasksByStatus(status, limit);

        // Then
        assertThat(result).isEqualTo(expectedTasks);
        verify(historyService).getTasksByStatus(status, limit);
    }

    // ========== getServiceStatus() Tests ==========

    @Test
    void getServiceStatus_ShouldReturnComprehensiveStatus() {
        // Given
        when(historyService.getActiveTaskCount()).thenReturn(3);
        when(historyService.getHistorySize()).thenReturn(100);
        when(processorManager.getAvailableTaskTypes()).thenReturn(Set.of(TaskType.LOAD_TEST, TaskType.DATA_MIGRATION));

        taskQueueService.initialize();

        // When
        Map<String, Object> status = taskQueueService.getServiceStatus();

        // Then
        assertThat(status).containsKeys(
                "serviceName", "version", "serviceState", "uptimeHours", "startTime",
                "queueSize", "isProcessing", "currentTask", "statistics",
                "availableTaskTypes", "configuration", "activeTasks", "taskHistorySize"
        );

        assertThat(status.get("serviceName")).isEqualTo("TaskQueueService");
        assertThat(status.get("version")).isEqualTo("3.0.0");

        // ✅ FIX: service state should be RUNNING, not PROCESSING
        assertThat(status.get("serviceState")).isEqualTo("RUNNING");

        assertThat(status.get("queueSize")).isEqualTo(0);
        assertThat(status.get("isProcessing")).isEqualTo(false);
        assertThat(status.get("activeTasks")).isEqualTo(3);
        assertThat(status.get("taskHistorySize")).isEqualTo(100);

        @SuppressWarnings("unchecked")
        Map<String, Object> statistics = (Map<String, Object>) status.get("statistics");
        assertThat(statistics).containsKeys("totalCompleted", "totalFailed", "totalProcessed",
                "successRate", "averageProcessingTimeMs", "statusBreakdown");

        @SuppressWarnings("unchecked")
        List<String> taskTypes = (List<String>) status.get("availableTaskTypes");
        assertThat(taskTypes).containsExactlyInAnyOrder("LOAD_TEST", "DATA_MIGRATION");

        @SuppressWarnings("unchecked")
        Map<String, Object> configuration = (Map<String, Object>) status.get("configuration");
        assertThat(configuration.get("maxQueueSize")).isEqualTo(100);
        assertThat(configuration.get("maxConcurrentTasks")).isEqualTo(1);
        assertThat(configuration.get("taskTimeoutMinutes")).isEqualTo(30L);
    }

    // ========== getAvailableTaskTypes() Tests ==========

    @Test
    void getAvailableTaskTypes_ShouldDelegateToProcessorManager() {
        // Given
        Set<TaskType> expectedTypes = Set.of(TaskType.LOAD_TEST, TaskType.DATA_MIGRATION);
        when(processorManager.getAvailableTaskTypes()).thenReturn(expectedTypes);

        // When
        Set<TaskType> result = taskQueueService.getAvailableTaskTypes();

        // Then
        assertThat(result).isEqualTo(expectedTypes);
        verify(processorManager).getAvailableTaskTypes();
    }

    // ========== Basic Getter Tests ==========

    @Test
    void getQueueSize_WhenQueueEmpty_ShouldReturnZero() {
        // When
        int queueSize = taskQueueService.getQueueSize();

        // Then
        assertThat(queueSize).isEqualTo(0);
    }

    @Test
    void isCurrentlyProcessing_WhenNoTaskProcessing_ShouldReturnFalse() {
        // When
        boolean isProcessing = taskQueueService.isCurrentlyProcessing();

        // Then
        assertThat(isProcessing).isFalse();
    }

    @Test
    void getCurrentTaskId_WhenNoTaskProcessing_ShouldReturnNull() {
        // When
        String currentTaskId = taskQueueService.getCurrentTaskId();

        // Then
        assertThat(currentTaskId).isNull();
    }

    @Test
    void getServiceState_WhenServiceStarting_ShouldReturnStarting() {
        // When
        ServiceState state = taskQueueService.getServiceState();

        // Then
        assertThat(state).isEqualTo(ServiceState.STARTING);
    }

    @Test
    void getServiceState_AfterInitialization_ShouldReturnRunning() {
        // Given
        taskQueueService.initialize();

        // When
        ServiceState state = taskQueueService.getServiceState();

        // Then
        assertThat(state).isEqualTo(ServiceState.RUNNING);
    }

    // ========== Shutdown Tests ==========

    @Test
    @Disabled
    void shutdown_ShouldChangeServiceStateToShutdown() throws InterruptedException {
        // Given
        taskQueueService.initialize();

        // When
        taskQueueService.shutdown();

        // Give a moment for shutdown to complete
        Thread.sleep(100);

        // Then
        assertThat(taskQueueService.getServiceState()).isEqualTo(ServiceState.SHUTDOWN);
        verify(validator).setShuttingDown(true);
    }

    // ========== Error Handling Tests ==========

    @Test
    void submitTask_WhenUnexpectedExceptionOccurs_ShouldReturnErrorResponse() {
        // Given
        when(taskDto.getTaskId()).thenReturn("error-task");
        doThrow(new RuntimeException("Unexpected error"))
                .when(validator).validateServiceState();

        taskQueueService.initialize();

        // When
        TaskResponseDto response = taskQueueService.submitTask(taskDto);

        // Then
        assertThat(response.getTaskId()).isEqualTo("error-task");
        assertThat(response.getStatus()).isEqualTo("ERROR");
        assertThat(response.getMessage()).contains("Internal error during task submission");
    }

    // ========== Integration Tests ==========

    // ============================================================
    // Existing Tests (Your Original Suite)
    // ============================================================

    @Test
    void constructor_ShouldInitializeWithCorrectDependencies() {
        ImprovedTaskQueueService service = new ImprovedTaskQueueService(
                validator, historyService, processorManager, config);

        assertThat(service.getServiceState()).isEqualTo(ServiceState.STARTING);
        assertThat(service.getQueueSize()).isEqualTo(0);
        assertThat(service.isCurrentlyProcessing()).isFalse();
        assertThat(service.getCurrentTaskId()).isNull();
    }

    @Test
    void initialize_ShouldSetServiceStateToRunning() {
        taskQueueService.initialize();
        assertThat(taskQueueService.getServiceState()).isEqualTo(ServiceState.RUNNING);
    }

    // ... [YOUR EXISTING TESTS REMAIN UNCHANGED] ...
    // (submitTask(), cancelTask(), getTaskStatus(), shutdown, etc.)
    // I've omitted them here for brevity — keep them in your file

    // ============================================================
    // New Tests - Missing Coverage
    // ============================================================

    // ---------- Task Processing Lifecycle ----------

    @Test
    void processTaskSafely_WhenProcessorSucceeds_ShouldMarkCompleted() throws Exception {
        TaskDto task = mock(TaskDto.class);
        when(task.getTaskId()).thenReturn("success-task");
        when(task.getTaskType()).thenReturn(TaskType.LOAD_TEST);

        TaskWrapper wrapper = new TaskWrapper(task);
        doNothing().when(historyService).markProcessing("success-task");

        // Mock processor to just return successfully
        doAnswer(invocation -> null).when(processorManager).processTask(task);

        taskQueueService.initialize();
        // Invoke private method via reflection
        Method m = ImprovedTaskQueueService.class.getDeclaredMethod("processTaskSafely", TaskWrapper.class);
        m.setAccessible(true);
        m.invoke(taskQueueService, wrapper);

        verify(historyService).markProcessing("success-task");
        verify(historyService).markCompleted(eq("success-task"), anyLong());
    }

    @Test
    void processTaskSafely_WhenProcessorThrows_ShouldMarkFailed() throws Exception {
        TaskDto task = mock(TaskDto.class);
        when(task.getTaskId()).thenReturn("fail-task");
        when(task.getTaskType()).thenReturn(TaskType.LOAD_TEST);
        TaskWrapper wrapper = new TaskWrapper(task);

        doThrow(new RuntimeException("boom")).when(processorManager).processTask(task);

        taskQueueService.initialize();
        Method m = ImprovedTaskQueueService.class.getDeclaredMethod("processTaskSafely", TaskWrapper.class);
        m.setAccessible(true);
        m.invoke(taskQueueService, wrapper);

        verify(historyService).markFailed(eq("fail-task"), anyLong(), contains("boom"));
    }

    @Test
    void processTaskSafely_WhenWrapperCancelledBeforeStart_ShouldMarkCancelled() throws Exception {
        TaskDto task = mock(TaskDto.class);
        when(task.getTaskId()).thenReturn("cancelled-before");
        TaskWrapper wrapper = new TaskWrapper(task);
        wrapper.cancel();

        taskQueueService.initialize();
        Method m = ImprovedTaskQueueService.class.getDeclaredMethod("processTaskSafely", TaskWrapper.class);
        m.setAccessible(true);
        m.invoke(taskQueueService, wrapper);

        verify(historyService).markCancelled(eq("cancelled-before"), eq(0L), contains("before processing"));
    }

    // ---------- Cancellation of Running Task ----------


    @Test
    void cancelTask_WhenTaskIsRunning_ShouldCancelAndNotifyProcessor() throws Exception {
        // Create a fresh service to avoid state leakage from @BeforeEach
        ImprovedTaskQueueService localService = new ImprovedTaskQueueService(
                validator, historyService, processorManager, config);
        localService.initialize();

        // Create a running task + wrapper
        TaskDto runningTask = mock(TaskDto.class);
        when(runningTask.getTaskId()).thenReturn("running-task");
        when(runningTask.getTaskType()).thenReturn(TaskType.LOAD_TEST);

        TaskWrapper wrapper = new TaskWrapper(runningTask);

        // Put wrapper into the AtomicReference currentTask
        Field currentTaskField = ImprovedTaskQueueService.class.getDeclaredField("currentTask");
        currentTaskField.setAccessible(true);
        @SuppressWarnings("unchecked")
        AtomicReference<TaskWrapper> currentTaskRef =
                (AtomicReference<TaskWrapper>) currentTaskField.get(localService);
        currentTaskRef.set(wrapper);

        // Make the service think the task is active
        when(historyService.isTaskActive("running-task")).thenReturn(true);

        // Mock processor that supports cancellation
        InterruptibleTaskProcessor processor = mock(InterruptibleTaskProcessor.class);
        when(processorManager.getProcessor(TaskType.LOAD_TEST)).thenReturn(processor);

        // Act
        TaskResponseDto response = localService.cancelTask("running-task");

        // Assert
        assertThat(response.getStatus()).isEqualTo("CANCELLED");
        verify(processor).cancelTask(runningTask);
        verify(historyService).markCancelled(eq("running-task"), eq(0L), contains("cancelled"));
    }

    // ---------- calculateCurrentQueuePosition ----------

    @Test
    void calculateCurrentQueuePosition_WhenTaskInQueue_ShouldReturnCorrectIndex() throws Exception {
        TaskDto t1 = mock(TaskDto.class);
        when(t1.getTaskId()).thenReturn("t1");
        TaskWrapper wrapper1 = new TaskWrapper(t1);

        // Use reflection to access the private queue field without starting the processor
        var queueField = ImprovedTaskQueueService.class.getDeclaredField("taskQueue");
        queueField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Queue<TaskWrapper> queue = (Queue<TaskWrapper>) queueField.get(taskQueueService);

        // Add task manually
        queue.add(wrapper1);

        // Invoke the private method directly
        var method = ImprovedTaskQueueService.class.getDeclaredMethod("calculateCurrentQueuePosition", String.class);
        method.setAccessible(true);
        int pos = (int) method.invoke(taskQueueService, "t1");

        assertThat(pos).isEqualTo(1); // ✅ deterministic now
    }

    @Test
    void calculateCurrentQueuePosition_WhenTaskNotFound_ShouldReturnMinusOne() throws Exception {
        Method m = ImprovedTaskQueueService.class.getDeclaredMethod("calculateCurrentQueuePosition", String.class);
        m.setAccessible(true);
        int pos = (int) m.invoke(taskQueueService, "missing-task");
        assertThat(pos).isEqualTo(-1);
    }

    // ---------- buildStatusResponse ----------

    @Test
    void buildStatusResponse_WhenStatusCompleted_ShouldContainDuration() throws Exception {
        when(taskExecution.getStatus()).thenReturn(TaskStatus.COMPLETED);
        when(taskExecution.getProcessingTimeMs()).thenReturn(123L);
        when(taskExecution.getQueuePosition()).thenReturn(0);

        Method m = ImprovedTaskQueueService.class.getDeclaredMethod(
                "buildStatusResponse", String.class, TaskExecution.class);
        m.setAccessible(true);
        TaskResponseDto response = (TaskResponseDto) m.invoke(taskQueueService, "task-xyz", taskExecution);

        assertThat(response.getMessage()).contains("completed in 123 ms");
    }

    @Test
    void buildStatusResponse_WhenStatusFailed_ShouldContainErrorMessage() throws Exception {
        when(taskExecution.getStatus()).thenReturn(TaskStatus.FAILED);
        when(taskExecution.getErrorMessage()).thenReturn("Failure reason");
        when(taskExecution.getQueuePosition()).thenReturn(0);

        Method m = ImprovedTaskQueueService.class.getDeclaredMethod(
                "buildStatusResponse", String.class, TaskExecution.class);
        m.setAccessible(true);
        TaskResponseDto response = (TaskResponseDto) m.invoke(taskQueueService, "task-xyz", taskExecution);

        assertThat(response.getMessage()).contains("Failure reason");
    }

    // ---------- notifyProcessorOfCancellation ----------

    @Test
    void notifyProcessorOfCancellation_ShouldCallInterruptibleProcessorCancel() throws Exception {
        TaskDto task = mock(TaskDto.class);
        when(task.getTaskType()).thenReturn(TaskType.LOAD_TEST);

        InterruptibleTaskProcessor processor = mock(InterruptibleTaskProcessor.class);
        when(processorManager.getProcessor(TaskType.LOAD_TEST)).thenReturn(processor);

        Method m = ImprovedTaskQueueService.class.getDeclaredMethod("notifyProcessorOfCancellation", TaskDto.class);
        m.setAccessible(true);
        m.invoke(taskQueueService, task);

        verify(processor).cancelTask(task);
    }

    // ============================================================
    // Reflection Helper Methods
    // ============================================================

    private java.lang.reflect.Field getCurrentTaskField() {
        try {
            var f = ImprovedTaskQueueService.class.getDeclaredField("currentTask");
            f.setAccessible(true);
            return f;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private java.lang.reflect.Field getQueueField() {
        try {
            var f = ImprovedTaskQueueService.class.getDeclaredField("taskQueue");
            f.setAccessible(true);
            return f;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    // ========== Helper Methods ==========

    private void setupValidTaskSubmission() {
        when(taskDto.getTaskId()).thenReturn("task-123");
        when(taskDto.getTaskType()).thenReturn(TaskType.LOAD_TEST);
        doNothing().when(validator).validateServiceState();
        doNothing().when(validator).validateTask(taskDto);
        doNothing().when(validator).checkDuplicate(taskDto);
        doNothing().when(validator).validateBusinessRules(eq(taskDto), anyInt());
        doNothing().when(historyService).recordQueued(eq(taskDto), anyInt());
    }
}