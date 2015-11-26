package com.indeed.squall.iql2.language;

import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.util.core.Pair;
import org.antlr.v4.runtime.Token;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ParserCommon {
    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    public static String unquote(String text) {
        if (!((text.startsWith("\"") && text.endsWith("\"")) || (text.startsWith("\'") && text.endsWith("\'")))) {
            return text;
        }
        final StringBuilder sb = new StringBuilder();
        boolean isEscaping = false;
        for (int i = 1; i < text.length() - 1; i++) {
            final char c = text.charAt(i);
            if (c == '\\') {
                if (isEscaping) {
                    sb.append('\\');
                    isEscaping = false;
                } else {
                    isEscaping = true;
                }
            } else {
                sb.append(c);
                isEscaping = false;
            }
        }
        return sb.toString();
    }
}
