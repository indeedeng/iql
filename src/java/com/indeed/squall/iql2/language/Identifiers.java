package com.indeed.squall.iql2.language;

public class Identifiers {
    public static String parseIdentifier(JQLParser.IdentifierContext identifierContext) {
        if (identifierContext.BACKQUOTED_ID() != null) {
            return identifierContext.getText().substring(1, identifierContext.getText().length() - 1).toUpperCase();
        } else {
            return identifierContext.getText().toUpperCase();
        }
    }
}
