package com.indeed.iql.exceptions;

import com.google.common.base.Throwables;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public static class UnknownHostException extends IqlKnownException {
        public UnknownHostException(final String message, final Throwable cause) {
            super(message, cause);
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

    // Legacy parsing exception from Iql 1.
    // Renamed, previously was IQLParseException
    public static class StatementParseException extends IqlKnownException {
        private static final Pattern COLUMN_ERROR_PATTERN = Pattern.compile("line 1, column (\\d+)[\\n:]");

        // which clause the exception occurred in
        private final String clause;
        private final int offsetInClause;

        public StatementParseException(final Throwable cause, final String clause) {
            super(getSummaryMessage(Throwables.getRootCause(cause)), cause);

            this.clause = clause;
            this.setStackTrace(cause.getStackTrace());


            offsetInClause = getOffset(cause);
        }

        private static int getOffset(final Throwable cause) {
            if(cause != null) {
                final String causeMessage = cause.getMessage();
                final Matcher columnMatcher = COLUMN_ERROR_PATTERN.matcher(causeMessage);
                if(columnMatcher.find()) {
                    return Integer.valueOf(columnMatcher.group(1));
                }
            }
            return -1;
        }

        public String getClause() {
            return clause;
        }

        public int getOffsetInClause() {
            return offsetInClause;
        }

        private static String getSummaryMessage(final Throwable e) {
            String message = e.getMessage();
            final Matcher columnErrorMatcher = COLUMN_ERROR_PATTERN.matcher(message);
            message = columnErrorMatcher.replaceAll("");
            return message.trim();
        }
    }

}
