package com.indeed.imhotep.web;

/**
 * @author vladimir
 */

public class ErrorResult {
    String exceptionType;
    String message;
    String stackTrace;
    String clause;
    int offsetInClause;

    public ErrorResult(String exceptionType, String message, String stackTrace, String clause, int offsetInClause) {
        this.exceptionType = exceptionType;
        this.message = message;
        this.stackTrace = stackTrace;
        this.clause = clause;
        this.offsetInClause = offsetInClause;
    }

    public String getExceptionType() {
        return exceptionType;
    }

    public String getMessage() {
        return message;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    public String getClause() {
        return clause;
    }

    public int getOffsetInClause() {
        return offsetInClause;
    }
}
