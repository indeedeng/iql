package com.indeed.squall.iql2.language;

import java.util.regex.Pattern;

public class Identifiers {
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_-]*");

    public static Positioned<String> parseIdentifier(JQLParser.IdentifierContext identifierContext) {
        final String result;
        if (identifierContext.BACKQUOTED_ID() != null) {
            result = identifierContext.getText().substring(1, identifierContext.getText().length() - 1).toUpperCase();
        } else {
            result = identifierContext.getText().toUpperCase();
        }
        if (!result.isEmpty() && !IDENTIFIER_PATTERN.matcher(result).matches()) {
            throw new IllegalArgumentException("identifier " + result +" doesn't match pattern : " + IDENTIFIER_PATTERN.toString());
        }
        return Positioned.from(result, identifierContext);
    }
}
