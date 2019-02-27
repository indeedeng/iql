package com.indeed.iql.exceptions;

/**
 * Thrown in cases where we fail to flush the outputstream.
 */
public class OutputStreamFlushException extends RuntimeException {
    public OutputStreamFlushException(final String message) {
        super(message);
    }
}
