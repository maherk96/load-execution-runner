package com.mk.fx.qa.load.execution.cfg;

/**
 * Represents an error response with an error message and additional details.
 *
 * @param error the error message
 * @param details additional details about the error
 */
public record ErrorResponse(String error, String details) {}
