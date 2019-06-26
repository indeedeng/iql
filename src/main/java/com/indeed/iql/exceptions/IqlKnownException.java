package com.indeed.iql.exceptions;

/**
 * Exceptions for user errors like incorrect query or too many groups after regrouping.
 *
 * @author aibragimov
 */
public class IqlKnownException extends RuntimeException {

    // All constructors are protected.
    // Create subclasses of IqlKnownException.
    protected IqlKnownException(final String message) {
        super(message);
    }

    protected IqlKnownException(final String message, final Throwable cause) {
        super(message, cause);
    }

    protected IqlKnownException(final Throwable cause) {
        super(cause);
    }

    protected IqlKnownException(
            final String message,
            final Throwable cause,
            final boolean enableSuppression,
            final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    // Incorrect query
    public static class ParseErrorException extends IqlKnownException {
        public ParseErrorException(final String message) {
            super(message);
        }

        public ParseErrorException(final Throwable cause) {
            super(cause);
        }

        public ParseErrorException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    // String field used as int string or vice versa
    public static class FieldTypeMismatchException extends IqlKnownException {
        public FieldTypeMismatchException(final String message) {
            super(message);
        }
    }

    public static class OptionsErrorException extends IqlKnownException {
        public OptionsErrorException(final String message, final Throwable cause) {
            super(message, cause);
        }

        public OptionsErrorException(final String message) {
            super(message);
        }
    }

    public static class UnknownDatasetException extends IqlKnownException {
        public UnknownDatasetException(final String message) {
            super(message);
        }
    }

    public static class UnknownFieldException extends IqlKnownException {
        public UnknownFieldException(final String message) {
            super(message);
        }
    }

    public static class GroupLimitExceededException extends IqlKnownException {
        public GroupLimitExceededException(final String message) {
            super(message);
        }
    }

    public static class DocumentsLimitExceededException extends IqlKnownException {
        public DocumentsLimitExceededException(final String message) {
            super(message);
        }
    }

    public static class NoDataException extends IqlKnownException {
        public NoDataException(final String message) {
            super(message);
        }
    }

    // Error that occur during execution but not system error.
    // For example, group by multivalued field
    public static class ExecutionException extends IqlKnownException {
        public ExecutionException(final String message) {
            super(message);
        }
    }

    public static class IdentificationRequiredException extends IqlKnownException {
        public IdentificationRequiredException(final String message) {
            super(message);
        }
    }

    public static class RowLimitErrorException extends IqlKnownException {
        public RowLimitErrorException(final String message) {
            super(message);
        }
    }

    public static class AccessDeniedException extends IqlKnownException {
        public AccessDeniedException(String message) {
            super(message);
        }
    }

    public static class TooManyPendingQueriesException extends IqlKnownException {
        public TooManyPendingQueriesException(final String message) {
            super(message);
        }
    }

    public static class ClientHungUpException extends IqlKnownException {
        public ClientHungUpException(final String message) {
            super(message);
        }
    }
}
