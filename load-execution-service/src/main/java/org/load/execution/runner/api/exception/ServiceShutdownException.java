package org.load.execution.runner.api.exception;

public class ServiceShutdownException extends ServiceException {
    public ServiceShutdownException(String message) {
        super(message);
    }
}