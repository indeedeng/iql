package com.indeed.squall.iql2.language;

public class Identifiers {
    private static final String IDENTIFIER_PATTERN = "[a-zA-Z][a-zA-Z0-9_-]*";

    public static Positioned<String> parseIdentifier(JQLParser.IdentifierContext identifierContext) {
        final String result;
        if (identifierContext.BACKQUOTED_ID() != null) {
            result = identifierContext.getText().substring(1, identifierContext.getText().length() - 1).toUpperCase();
        } else {
            result = identifierContext.getText().toUpperCase();
        }
        if (!result.isEmpty() && !result.matches(IDENTIFIER_PATTERN)) {
            throw new IllegalArgumentException("identifier " + result +" doesn't match pattern : " + IDENTIFIER_PATTERN);
        }
        return Positioned.from(result, identifierContext);
    }
}
