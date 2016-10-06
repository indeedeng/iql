package com.indeed.squall.iql2.language;

import org.apache.commons.lang.StringEscapeUtils;
import org.joda.time.DateTimeZone;

public class ParserCommon {
    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    // TODO: Shouldn't this unescape things like \n and whatnot..?
    // TODO: Should this really be used for regexes?
    public static String unquote(String text) {
        if (!((text.startsWith("\"") && text.endsWith("\"")) || (text.startsWith("\'") && text.endsWith("\'")))) {
            return text;
        }
        return StringEscapeUtils.unescapeJava(text.substring(1, text.length() - 1));
    }
}
