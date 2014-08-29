package com.indeed.imhotep.web;

import com.google.common.base.Throwables;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author vladimir
 */

public class IQLParseException extends RuntimeException {
    private final static Pattern columnErrorPattern = Pattern.compile("line 1, column (\\d+)[\\n:]");

    // which clause the exception occurred in
    private final String clause;
    private final int offsetInClause;

    public IQLParseException(Throwable cause, String clause) {
        super(getSummaryMessage(Throwables.getRootCause(cause)), cause);

        this.clause = clause;
        this.setStackTrace(cause.getStackTrace());


        offsetInClause = getOffset(cause);
    }

    private static int getOffset(Throwable cause) {
        if(cause != null) {
            final String causeMessage = cause.getMessage();
            final Matcher columnMatcher = columnErrorPattern.matcher(causeMessage);
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

    private static String getSummaryMessage(Throwable e) {
        String message = e.getMessage();
        final Matcher columnErrorMatcher = columnErrorPattern.matcher(message);
        message = columnErrorMatcher.replaceAll("");
        return message.trim();
    }
}
