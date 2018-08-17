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

import com.indeed.iql.exceptions.IqlKnownException;
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
        try {
            return StringEscapeUtils.unescapeJava(text.substring(1, text.length() - 1));
        } catch (final Throwable t) {
            throw new IqlKnownException.ParseErrorException("Can't process string value " + text, t);
        }
    }
}
