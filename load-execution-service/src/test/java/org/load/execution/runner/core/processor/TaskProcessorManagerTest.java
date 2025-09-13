package org.load.execution.runner.core.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.load.execution.runner.api.dto.TaskDto;
import org.load.execution.runner.config.TaskQueueConfig;
import org.load.execution.runner.core.model.TaskType;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class TaskProcessorManagerTest {

    private TaskQueueConfig config;
    private TaskProcessor mockProcessor;
    private TaskProcessorManager manager;

    @BeforeEach
    void setUp() {
        config = mock(TaskQueueConfig.class);
        mockProcessor = mock(TaskProcessor.class);
        when(mockProcessor.getTaskType()).thenReturn(TaskType.LOAD_TEST);

        manager = new TaskProcessorManager(config, List.of(mockProcessor));
    }

    @Test
    void constructor_ShouldRegisterProcessors() {
        assertThat(manager.hasProcessors()).isTrue();
        assertThat(manager.hasProcessor(TaskType.LOAD_TEST)).isTrue();
        assertThat(manager.getAvailableTaskTypes()).containsExactly(TaskType.LOAD_TEST);
    }

    @Test
    void constructor_WhenNoProcessors_ShouldWarnAndBeEmpty() {
        TaskProcessorManager emptyManager = new TaskProcessorManager(config, List.of());
        assertThat(emptyManager.hasProcessors()).isFalse();
        assertThat(emptyManager.getAvailableTaskTypes()).isEmpty();
    }

    @Test
    void getAvailableTaskTypesString_ShouldReturnSortedCommaSeparatedString() {
        TaskProcessor p2 = mock(TaskProcessor.class);
        when(p2.getTaskType()).thenReturn(TaskType.DATA_MIGRATION);
        TaskProcessorManager mgr = new TaskProcessorManager(config, List.of(mockProcessor, p2));

        String result = mgr.getAvailableTaskTypesString();

        assertThat(result).isEqualTo("DATA_MIGRATION, LOAD_TEST"); // alphabetically sorted
    }

    @Test
    void validateTask_WhenProcessorImplementsValidatable_ShouldDelegate() {
        // Create a mock that is both a TaskProcessor and ValidatableTaskProcessor
        class ValidatableProcessor implements TaskProcessor, ValidatableTaskProcessor {
            @Override
            public TaskType getTaskType() {
                return TaskType.LOAD_TEST;
            }

            @Override
            public void processTask(TaskDto task) {
                // no-op
            }

            @Override
            public void validateTask(TaskDto task) {

            }
        }

        // Track validation call
        final boolean[] called = {false};
        ValidatableProcessor processor = new ValidatableProcessor() {
            @Override
            public void validateTask(TaskDto task) {
                called[0] = true;
            }
        };

        TaskDto task = mock(TaskDto.class);
        when(task.getTaskType()).thenReturn(TaskType.LOAD_TEST);

        TaskProcessorManager mgr = new TaskProcessorManager(config, List.of(processor));

        mgr.validateTask(task);

        assertThat(called[0]).isTrue();
    }

    @Test
    void validateTask_WhenProcessorNotFound_ShouldDoNothing() {
        TaskDto task = mock(TaskDto.class);
        when(task.getTaskType()).thenReturn(TaskType.DATA_MIGRATION);

        assertThatCode(() -> manager.validateTask(task)).doesNotThrowAnyException();
    }

    @Test
    void processTask_WhenProcessorExists_ShouldInvokeProcessTask() throws Exception {
        TaskDto task = mock(TaskDto.class);
        when(task.getTaskType()).thenReturn(TaskType.LOAD_TEST);

        manager.processTask(task);

        verify(mockProcessor).processTask(task);
    }

    @Test
    void processTask_WhenProcessorNotFound_ShouldThrow() {
        TaskDto task = mock(TaskDto.class);
        when(task.getTaskType()).thenReturn(TaskType.DATA_MIGRATION);

        assertThatThrownBy(() -> manager.processTask(task))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Processor not found for type");
    }
}
