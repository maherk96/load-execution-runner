package org.load.execution.runner.api.exception;

abstract class ServiceException extends RuntimeException {
    public ServiceException(String message) {
        super(message);
    }
}
