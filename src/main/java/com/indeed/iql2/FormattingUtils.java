package com.indeed.iql2;

import javax.annotation.Nullable;
import java.util.regex.Pattern;

public class FormattingUtils {
    private static final Pattern TSV_ESCAPE_PATTERN = Pattern.compile("[\t\r\n]");

    public static String tsvEscape(@Nullable final String s) {
        if (s == null) {
            return null;
        }
        return TSV_ESCAPE_PATTERN.matcher(s).replaceAll("\ufffd");
    }
}
