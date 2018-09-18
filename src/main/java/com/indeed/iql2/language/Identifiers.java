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
import com.indeed.iql2.language.JQLParser;

import java.util.regex.Pattern;

public class Identifiers {
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");

    public static Positioned<String> parseIdentifier(JQLParser.IdentifierContext identifierContext) {
        final String result;
        if (identifierContext.BACKQUOTED_ID() != null) {
            result = identifierContext.getText().substring(1, identifierContext.getText().length() - 1).toUpperCase();
        } else {
            result = identifierContext.getText().toUpperCase();
            if (!IDENTIFIER_PATTERN.matcher(result).matches()) {
                throw new IqlKnownException.ParseErrorException("identifier " + result +" doesn't match pattern : " + IDENTIFIER_PATTERN.toString());
            }
        }
        return Positioned.from(result, identifierContext);
    }
}
