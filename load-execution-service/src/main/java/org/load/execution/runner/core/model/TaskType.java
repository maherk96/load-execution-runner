// ===============================
// Improved TaskQueueService.java
// ===============================
package org.load.execution.runner.core.model;

// ===============================
// ENUMS AND DATA CLASSES
// ===============================

public enum TaskType {
    LOAD_TEST,
    REST_API_TEST,
    QUICKFIX_TEST,
    PERFORMANCE_TEST,
    DATA_MIGRATION,
    HEALTH_CHECK
}



// ===============================
// CONFIGURATION
// ===============================



// ===============================
// IMPROVED TASK QUEUE SERVICE
// ===============================


// ===============================
// ENHANCED TASK CONTROLLER
// ===============================



/*
===============================
USAGE EXAMPLES
===============================

1. Submit a task:
POST /api/tasks/submit
{
  "taskId": "load-test-001",
  "taskType": "LOAD_TEST",
  "data": {
    "endpoint": "https://api.example.com",
    "duration": 300
  },
  "createdAt": "2024-01-15T10:30:00"
}

Response:
{
  "taskId": "load-test-001",
  "status": "QUEUED",
  "queuePosition": 3,
  "message": "Task queued at position 3"
}

2. Check task status:
GET /api/tasks/load-test-001/status

Response:
{
  "taskId": "load-test-001",
  "status": "PROCESSING",
  "message": "Task processing for 45 seconds"
}

3. Get detailed task information:
GET /api/tasks/load-test-001/details

Response:
{
  "taskId": "load-test-001",
  "taskType": "LOAD_TEST",
  "status": "PROCESSING",
  "queuedAt": "2024-01-15T10:30:00",
  "startedAt": "2024-01-15T10:32:15",
  "processingTimeMs": 45000,
  "queuePosition": 3
}

4. Cancel a task:
DELETE /api/tasks/load-test-001

Response:
{
  "taskId": "load-test-001",
  "status": "CANCELLED",
  "message": "Task was cancelled successfully"
}

5. Get queue status:
GET /api/tasks/queue/status

Response:
{
  "serviceName": "TaskQueueService",
  "status": "RUNNING",
  "queueSize": 2,
  "isProcessing": true,
  "currentTask": {
    "taskId": "load-test-002",
    "taskType": "LOAD_TEST",
    "processingTimeMs": 30000
  },
  "statistics": {
    "totalCompleted": 15,
    "totalFailed": 2,
    "successRate": "88.24%",
    "statusBreakdown": {
      "COMPLETED": 15,
      "FAILED": 2,
      "PROCESSING": 1,
      "QUEUED": 2
    }
  }
}

6. Get task history:
GET /api/tasks/history?limit=10

7. Get tasks by status:
GET /api/tasks/status/FAILED?limit=5

8. Health check:
GET /api/tasks/health

===============================
CONFIGURATION
===============================

application.yml:
task:
  queue:
    max-queue-size: 1000
    max-task-age-hours: 24
    task-timeout-minutes: 60
    task-history-retention-hours: 168
    shutdown-timeout-seconds: 30

===============================
KEY FEATURES ADDED
===============================

✅ Task Cancellation:
  - Cancel queued tasks (removes from queue)
  - Cancel running tasks (interrupts execution)
  - Proper status tracking for cancelled tasks

✅ Enhanced Status Tracking:
  - QUEUED, PROCESSING, COMPLETED, FAILED, CANCELLED, TIMEOUT
  - Real-time queue position updates
  - Processing time tracking
  - Error message storage

✅ Comprehensive API:
  - Submit, status, cancel, history endpoints
  - Detailed task information
  - Queue monitoring and metrics
  - Health checks

✅ Thread Safety:
  - Proper interrupt handling
  - Cancellation during execution
  - Atomic status updates

✅ Production Ready:
  - Configurable timeouts
  - Memory management
  - Comprehensive logging
  - Error handling
*/