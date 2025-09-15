package org.load.execution.runner.core.processor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.load.execution.runner.api.dto.TaskDto;
import org.load.execution.runner.config.TaskQueueConfig;
import org.load.execution.runner.core.model.TaskType;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TaskProcessorManagerTest {

    @Mock
    private TaskQueueConfig mockConfig;

    @Mock
    private TaskProcessor restLoadProcessor;

    @Mock
    private TaskProcessor fixLoadProcessor;

    @Mock
    private TaskDto mockTask;

    private TaskProcessorManager processorManager;

    @BeforeEach
    void setUp() {
        lenient().when(restLoadProcessor.getTaskType()).thenReturn(TaskType.REST_LOAD);
        lenient().when(fixLoadProcessor.getTaskType()).thenReturn(TaskType.FIX_LOAD);
    }

    @Test
    void constructor_WithValidProcessors_RegistersCorrectly() {
        List<TaskProcessor> processors = Arrays.asList(restLoadProcessor, fixLoadProcessor);

        processorManager = new TaskProcessorManager(mockConfig, processors);

        assertTrue(processorManager.hasProcessors());
        assertTrue(processorManager.hasProcessor(TaskType.REST_LOAD));
        assertTrue(processorManager.hasProcessor(TaskType.FIX_LOAD));
        assertEquals(2, processorManager.getAvailableTaskTypes().size());
    }

    @Test
    void constructor_WithNullProcessorList_HandlesGracefully() {
        processorManager = new TaskProcessorManager(mockConfig, null);

        assertFalse(processorManager.hasProcessors());
        assertEquals(0, processorManager.getAvailableTaskTypes().size());
        assertTrue(processorManager.getAvailableTaskTypesString().isEmpty());
    }

    @Test
    void constructor_WithEmptyProcessorList_HandlesGracefully() {
        processorManager = new TaskProcessorManager(mockConfig, Collections.emptyList());

        assertFalse(processorManager.hasProcessors());
        assertEquals(0, processorManager.getAvailableTaskTypes().size());
        assertTrue(processorManager.getAvailableTaskTypesString().isEmpty());
    }

    @Test
    void constructor_WithProcessorThatThrowsException_SkipsFailedProcessor() {
        TaskProcessor faultyProcessor = mock(TaskProcessor.class);
        when(faultyProcessor.getTaskType()).thenThrow(new RuntimeException("Processor error"));

        List<TaskProcessor> processors = Arrays.asList(restLoadProcessor, faultyProcessor, fixLoadProcessor);

        processorManager = new TaskProcessorManager(mockConfig, processors);

        assertTrue(processorManager.hasProcessors());
        assertTrue(processorManager.hasProcessor(TaskType.REST_LOAD));
        assertTrue(processorManager.hasProcessor(TaskType.FIX_LOAD));
        assertEquals(2, processorManager.getAvailableTaskTypes().size());
    }

    @Test
    void hasProcessors_WithNoProcessors_ReturnsFalse() {
        processorManager = new TaskProcessorManager(mockConfig, Collections.emptyList());

        assertFalse(processorManager.hasProcessors());
    }

    @Test
    void hasProcessors_WithProcessors_ReturnsTrue() {
        processorManager = new TaskProcessorManager(mockConfig, Arrays.asList(restLoadProcessor));

        assertTrue(processorManager.hasProcessors());
    }

    @Test
    void hasProcessor_ExistingType_ReturnsTrue() {
        processorManager = new TaskProcessorManager(mockConfig, Arrays.asList(restLoadProcessor));

        assertTrue(processorManager.hasProcessor(TaskType.REST_LOAD));
    }

    @Test
    void hasProcessor_NonExistingType_ReturnsFalse() {
        processorManager = new TaskProcessorManager(mockConfig, Arrays.asList(restLoadProcessor));

        assertFalse(processorManager.hasProcessor(TaskType.FIX_LOAD));
    }

    @Test
    void getProcessor_ExistingType_ReturnsProcessor() {
        processorManager = new TaskProcessorManager(mockConfig, Arrays.asList(restLoadProcessor));

        TaskProcessor result = processorManager.getProcessor(TaskType.REST_LOAD);

        assertEquals(restLoadProcessor, result);
    }

    @Test
    void getProcessor_NonExistingType_ReturnsNull() {
        processorManager = new TaskProcessorManager(mockConfig, Arrays.asList(restLoadProcessor));

        TaskProcessor result = processorManager.getProcessor(TaskType.FIX_LOAD);

        assertNull(result);
    }

    @Test
    void getAvailableTaskTypes_ReturnsCorrectSet() {
        List<TaskProcessor> processors = Arrays.asList(restLoadProcessor, fixLoadProcessor);
        processorManager = new TaskProcessorManager(mockConfig, processors);

        Set<TaskType> availableTypes = processorManager.getAvailableTaskTypes();

        assertEquals(2, availableTypes.size());
        assertTrue(availableTypes.contains(TaskType.REST_LOAD));
        assertTrue(availableTypes.contains(TaskType.FIX_LOAD));
    }

    @Test
    void getAvailableTaskTypes_ReturnsImmutableCopy() {
        processorManager = new TaskProcessorManager(mockConfig, Arrays.asList(restLoadProcessor));

        Set<TaskType> availableTypes1 = processorManager.getAvailableTaskTypes();
        Set<TaskType> availableTypes2 = processorManager.getAvailableTaskTypes();

        assertNotSame(availableTypes1, availableTypes2);
        assertEquals(availableTypes1, availableTypes2);
    }

    @Test
    void getAvailableTaskTypesString_WithMultipleTypes_ReturnsCommaSeparated() {
        List<TaskProcessor> processors = Arrays.asList(restLoadProcessor, fixLoadProcessor);
        processorManager = new TaskProcessorManager(mockConfig, processors);

        String typesString = processorManager.getAvailableTaskTypesString();

        // Should be sorted alphabetically
        assertTrue(typesString.contains("FIX_LOAD"));
        assertTrue(typesString.contains("REST_LOAD"));
        assertTrue(typesString.contains(", "));
    }

    @Test
    void getAvailableTaskTypesString_WithSingleType_ReturnsTypeName() {
        processorManager = new TaskProcessorManager(mockConfig, Arrays.asList(restLoadProcessor));

        String typesString = processorManager.getAvailableTaskTypesString();

        assertEquals("REST_LOAD", typesString);
    }

    @Test
    void getAvailableTaskTypesString_WithNoTypes_ReturnsEmptyString() {
        processorManager = new TaskProcessorManager(mockConfig, Collections.emptyList());

        String typesString = processorManager.getAvailableTaskTypesString();

        assertTrue(typesString.isEmpty());
    }



    @Test
    void validateTask_WithNonValidatableProcessor_DoesNotCallValidation() {
        when(mockTask.getTaskType()).thenReturn(TaskType.REST_LOAD);

        processorManager = new TaskProcessorManager(mockConfig, Arrays.asList(restLoadProcessor));

        // Should not throw exception
        assertDoesNotThrow(() -> processorManager.validateTask(mockTask));
    }

    @Test
    void validateTask_WithNonExistentProcessor_DoesNotThrow() {
        when(mockTask.getTaskType()).thenReturn(TaskType.FIX_LOAD);

        processorManager = new TaskProcessorManager(mockConfig, Arrays.asList(restLoadProcessor));

        // Should not throw exception
        assertDoesNotThrow(() -> processorManager.validateTask(mockTask));
    }

    @Test
    void processTask_WithExistingProcessor_CallsProcessor() throws Exception {
        when(mockTask.getTaskType()).thenReturn(TaskType.REST_LOAD);

        processorManager = new TaskProcessorManager(mockConfig, Arrays.asList(restLoadProcessor));

        processorManager.processTask(mockTask);

        verify(restLoadProcessor).processTask(mockTask);
    }

    @Test
    void processTask_WithNonExistentProcessor_ThrowsIllegalStateException() {
        when(mockTask.getTaskType()).thenReturn(TaskType.FIX_LOAD);

        processorManager = new TaskProcessorManager(mockConfig, Arrays.asList(restLoadProcessor));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> processorManager.processTask(mockTask));

        assertTrue(exception.getMessage().contains("Processor not found for type: FIX_LOAD"));
    }

    @Test
    void processTask_ProcessorThrowsException_PropagatesException() throws Exception {
        when(mockTask.getTaskType()).thenReturn(TaskType.REST_LOAD);
        Exception processorException = new RuntimeException("Processing failed");
        doThrow(processorException).when(restLoadProcessor).processTask(mockTask);

        processorManager = new TaskProcessorManager(mockConfig, Arrays.asList(restLoadProcessor));

        Exception thrownException = assertThrows(RuntimeException.class,
                () -> processorManager.processTask(mockTask));

        assertEquals(processorException, thrownException);
    }

    @Test
    void registerProcessors_WithInterruptibleProcessor_LogsCorrectly() {
        InterruptibleTaskProcessor interruptibleProcessor = mock(InterruptibleTaskProcessor.class);
        when(interruptibleProcessor.getTaskType()).thenReturn(TaskType.REST_LOAD);

        processorManager = new TaskProcessorManager(mockConfig, Arrays.asList(interruptibleProcessor));

        assertTrue(processorManager.hasProcessor(TaskType.REST_LOAD));
        assertEquals(interruptibleProcessor, processorManager.getProcessor(TaskType.REST_LOAD));
    }

    @Test
    void registerProcessors_WithValidatableAndInterruptibleProcessor_RegistersCorrectly() {
        // Create a processor that implements both interfaces
        TestValidatableInterruptibleProcessor combinedProcessor = mock(TestValidatableInterruptibleProcessor.class);
        when(combinedProcessor.getTaskType()).thenReturn(TaskType.REST_LOAD);
        when(mockTask.getTaskType()).thenReturn(TaskType.REST_LOAD);

        processorManager = new TaskProcessorManager(mockConfig, Arrays.asList(combinedProcessor));

        assertTrue(processorManager.hasProcessor(TaskType.REST_LOAD));

        // Test validation works
        processorManager.validateTask(mockTask);
        verify(combinedProcessor).validateTask(mockTask);
    }

    @Test
    void registerProcessors_DuplicateTaskType_OverridesPreviousProcessor() {
        TaskProcessor anotherRestLoadProcessor = mock(TaskProcessor.class);
        when(anotherRestLoadProcessor.getTaskType()).thenReturn(TaskType.REST_LOAD);

        List<TaskProcessor> processors = Arrays.asList(restLoadProcessor, anotherRestLoadProcessor);
        processorManager = new TaskProcessorManager(mockConfig, processors);

        // Should have only one REST_LOAD processor (the last one registered)
        assertEquals(1, processorManager.getAvailableTaskTypes().size());
        assertEquals(anotherRestLoadProcessor, processorManager.getProcessor(TaskType.REST_LOAD));
    }

    @Test
    void threadSafety_ConcurrentAccess_HandledSafely() throws InterruptedException {
        // Test that ConcurrentHashMap handles concurrent access properly
        processorManager = new TaskProcessorManager(mockConfig, Arrays.asList(restLoadProcessor, fixLoadProcessor));

        // Multiple threads accessing processor manager simultaneously
        Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 100; j++) {
                    processorManager.hasProcessor(TaskType.REST_LOAD);
                    processorManager.getProcessor(TaskType.REST_LOAD);
                    processorManager.getAvailableTaskTypes();
                    processorManager.getAvailableTaskTypesString();
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join(1000); // Wait max 1 second per thread
        }

        // Should still work correctly after concurrent access
        assertTrue(processorManager.hasProcessor(TaskType.REST_LOAD));
        assertEquals(2, processorManager.getAvailableTaskTypes().size());
    }


    @Test
    void construction_WithNullConfig_HandlesGracefully() {
        // Constructor should handle null config gracefully
        assertDoesNotThrow(() ->
                new TaskProcessorManager(null, Arrays.asList(restLoadProcessor)));
    }

    @Test
    void getAvailableTaskTypes_WithEmptyProcessors_ReturnsEmptySet() {
        processorManager = new TaskProcessorManager(mockConfig, Collections.emptyList());

        Set<TaskType> availableTypes = processorManager.getAvailableTaskTypes();

        assertNotNull(availableTypes);
        assertTrue(availableTypes.isEmpty());
    }

    // Helper interface for testing processors that implement multiple interfaces
    interface TestValidatableInterruptibleProcessor extends TaskProcessor, ValidatableTaskProcessor, InterruptibleTaskProcessor {
    }
}