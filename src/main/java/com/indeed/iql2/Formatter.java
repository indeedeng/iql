package com.indeed.iql2;

import com.indeed.iql2.execution.ResultFormat;
import org.apache.commons.lang.StringEscapeUtils;

import javax.annotation.Nullable;
import java.util.regex.Pattern;

public enum Formatter {
    CSV(ResultFormat.CSV, ','), TSV(ResultFormat.TSV, '\t');

    private static final Pattern TSV_ESCAPE_PATTERN = Pattern.compile("[\t\r\n]");

    private final ResultFormat resultFormat;
    private final char separator;

    Formatter(final ResultFormat resultFormat, final char separator) {
        this.resultFormat = resultFormat;
        this.separator = separator;
    }

    public static Formatter forFormat(final ResultFormat format) {
        switch (format) {
            case CSV:
                return Formatter.CSV;
            case TSV:
                return Formatter.TSV;
            default:
                throw new IllegalArgumentException("Unknown ResultFormat: " + format);
        }
    }

    public String escape(@Nullable final String s) {
        switch (resultFormat) {
            case CSV:
                return csvEscape(s);
            case TSV:
                return tsvEscape(s);
            default:
                throw new IllegalStateException("Unknown result format: " + resultFormat);
        }
    }

    public void appendEscapedString(final String groupString, final StringBuilder sb) {
        switch (resultFormat) {
            case CSV:
                sb.append(csvEscape(groupString));
                break;
            case TSV:
                for (int i = 0; i < groupString.length(); i++) {
                    final char groupChar = groupString.charAt(i);
                    if (groupChar != '\t' && groupChar != '\r' && groupChar != '\n') {
                        sb.append(groupChar);
                    } else {
                        sb.append('\ufffd');
                    }
                }
                break;
        }
    }

    public char getSeparator() {
        return separator;
    }

    private static String tsvEscape(@Nullable final String s) {
        if (s == null) {
            return null;
        }
        return TSV_ESCAPE_PATTERN.matcher(s).replaceAll("\ufffd");
    }

    private static String csvEscape(@Nullable final String s) {
        return StringEscapeUtils.escapeCsv(s);
    }
}
