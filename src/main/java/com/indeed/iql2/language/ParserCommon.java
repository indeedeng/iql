/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.iql2.language;

import com.google.common.base.Preconditions;
import com.indeed.iql.exceptions.IqlKnownException;
import it.unimi.dsi.fastutil.chars.CharAVLTreeSet;
import it.unimi.dsi.fastutil.chars.CharOpenHashSet;
import it.unimi.dsi.fastutil.chars.CharSet;
import it.unimi.dsi.fastutil.chars.CharSortedSet;
import org.apache.commons.lang.StringEscapeUtils;
import org.joda.time.DateTimeZone;

import java.util.function.Consumer;

public class ParserCommon {
    private ParserCommon() {
    }

    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    public static String unquote(final String text, final boolean useLegacy, final Consumer<String> warn) {
        return useLegacy ? unquoteLegacy(text, warn) : unquote(text, warn);
    }

    // TODO: Shouldn't this unescape things like \n and whatnot..?
    // TODO: Should this really be used for regexes?
    public static String unquote(final String text, final Consumer<String> warn) {
        if (!((text.startsWith("\"") && text.endsWith("\"")) || (text.startsWith("\'") && text.endsWith("\'")))) {
            return text;
        }
        try {
            checkForUnnecessaryEscapes(text, warn);
            return StringEscapeUtils.unescapeJava(text.substring(1, text.length() - 1));
        } catch (final Throwable t) {
            throw new IqlKnownException.ParseErrorException("Can't process string value " + text, t);
        }
    }

    private static final String REGEXP_RESERVED_CHARS = "|?*+{}[].\"()^$\\";
    private static final CharSet REGEXP_ESCAPED_CHARS = new CharOpenHashSet(REGEXP_RESERVED_CHARS.toCharArray());

    public static void checkForUnnecessaryRegexEscapes(final String regex, final Consumer<String> warn) {
        final CharSortedSet unnecessaryEscapes = getUnnecessaryRegexEscapes(regex);
        if (!unnecessaryEscapes.isEmpty()) {
            warn.accept("Regex contains unnecessary escapes (for characters " + unnecessaryEscapes + "): " + regex);
        }
    }

    public static CharSortedSet getUnnecessaryRegexEscapes(final String regex) {
        final CharSortedSet unnecessaryEscapes = new CharAVLTreeSet();
        for (int i = 0; i < regex.length(); i++) {
            if (regex.charAt(i) == '\\') {
                i += 1;
                final char escapedChar = regex.charAt(i);
                if (!REGEXP_ESCAPED_CHARS.contains(escapedChar)) {
                    unnecessaryEscapes.add(escapedChar);
                }
            }
        }
        return unnecessaryEscapes;
    }

    // deliberately exclude b and f for lack of utility in the context of IQL
    private static final CharSet ESCAPED_CHARS = new CharOpenHashSet(new char[]{'\\', '\'', '\"', 'r', 't', 'n', 'u'});

    public static void checkForUnnecessaryEscapes(final String text, final Consumer<String> warn) {
        final CharSortedSet unnecessaryEscapes = getUnnecessaryEscapes(text);
        if (!unnecessaryEscapes.isEmpty()) {
            warn.accept("String contains unnecessary escapes (for characters " + unnecessaryEscapes + "): " + text);
        }
    }

    @SuppressWarnings("AssignmentToForLoopParameter")
    public static CharSortedSet getUnnecessaryEscapes(final String text) {
        final CharSortedSet unnecessaryEscapes = new CharAVLTreeSet();
        final char quoteUsed = text.charAt(0);
        Preconditions.checkArgument((text.startsWith("\"") && text.endsWith("\"")) || (text.startsWith("\'") && text.endsWith("\'")));
        final char quoteNotUsed = (quoteUsed == '\'') ? '\"' : '\'';
        for (int i = 1; i < (text.length() - 1); i++) {
            if (text.charAt(i) == '\\') {
                i += 1;
                if (i == text.length()) {
                    break;
                }
                final char escapedChar = text.charAt(i);
                if (!ESCAPED_CHARS.contains(escapedChar)) {
                    unnecessaryEscapes.add(escapedChar);
                }
                if (escapedChar == quoteNotUsed) {
                    // if using " we don't need to unescape '
                    // if using ' we don't need to unescape "
                    unnecessaryEscapes.add(quoteNotUsed);
                }
                if (escapedChar == 'u') {
                    // unicode. skip the 4 hex unicode chars.
                    i += 4;
                }
            }
        }

        return unnecessaryEscapes;
    }

    public static String unquoteLegacy(final String text, final Consumer<String> warn) {
        if (text.startsWith("\'") && text.endsWith("\'")) {
            // for string with single quotes we just delete quotes
            return text.substring(1, text.length() - 1);
        }

        if (text.startsWith("\"") && text.endsWith("\"")) {
            // for string with double quotes we do un-escaping
            try {
                checkForUnnecessaryEscapes(text, warn);
                return StringEscapeUtils.unescapeJava(text.substring(1, text.length() - 1));
            } catch (final Throwable t) {
                throw new IqlKnownException.ParseErrorException("Can't process string value " + text, t);
            }
        }

        // quotes not found
        return text;
    }
}
