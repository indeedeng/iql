package com.indeed.iql2.language.util;

import org.apache.commons.lang.StringEscapeUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;

public class ToStringEscapingUtil {
    private ToStringEscapingUtil() {
    }

    public static String escape(final Collection<String> strings) {
        try (final StringWriter stringWriter = new StringWriter()) {
            stringWriter.append("[\"");
            boolean isFirst = true;
            for (final String str : strings) {
                if (!isFirst) {
                    stringWriter.append("\", \"");
                }
                isFirst = false;
                StringEscapeUtils.escapeJava(stringWriter, str);
            }
            stringWriter.append("\"]");
            return stringWriter.toString();
        } catch (final IOException e) {
            throw new IllegalStateException("StringWriter shouldn't throw an IO Exception", e);
        }
    }
}
