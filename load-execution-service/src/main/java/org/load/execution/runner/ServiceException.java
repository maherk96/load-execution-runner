package org.load.execution.runner;

abstract class ServiceException extends RuntimeException {
    public ServiceException(String message) {
        super(message);
    }
}

class ServiceShutdownException extends ServiceException {
    public ServiceShutdownException(String message) {
        super(message);
    }
}

class NoProcessorsException extends ServiceException {
    public NoProcessorsException(String message) {
        super(message);
    }
}

class InvalidTaskException extends ServiceException {
    public InvalidTaskException(String message) {
        super(message);
    }
}

class DuplicateTaskException extends ServiceException {
    public DuplicateTaskException(String message) {
        super(message);
    }
}

class NoProcessorException extends ServiceException {
    public NoProcessorException(String message) {
        super(message);
    }
}

class QueueCapacityException extends ServiceException {
    public QueueCapacityException(String message) {
        super(message);
    }
}

class TaskValidationException extends ServiceException {
    public TaskValidationException(String message) {
        super(message);
    }
}