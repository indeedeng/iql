package com.indeed.squall.iql2.language;

public class Identifiers {
    public static Positioned<String> parseIdentifier(JQLParser.IdentifierContext identifierContext) {
        final String result;
        if (identifierContext.BACKQUOTED_ID() != null) {
            result = identifierContext.getText().substring(1, identifierContext.getText().length() - 1).toUpperCase();
        } else {
            result = identifierContext.getText().toUpperCase();
        }
        if (!result.isEmpty() && Character.isDigit(result.charAt(0))) {
            throw new IllegalArgumentException("identifier starts with digit is not allowed");
        }
        return Positioned.from(result, identifierContext);
    }
}
