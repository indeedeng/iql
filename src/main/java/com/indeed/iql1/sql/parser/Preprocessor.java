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
 package com.indeed.iql1.sql.parser;

import com.google.common.base.Strings;
import org.codehaus.jparsec.Parser;
import org.codehaus.jparsec.Parsers;
import org.codehaus.jparsec.Scanners;
import org.codehaus.jparsec.Terminals;
import org.codehaus.jparsec.Token;
import org.codehaus.jparsec.pattern.Patterns;

import java.util.List;
import java.util.Map;

/**
 * Takes care of applying field aliases / expansions before the main parser runs.
 * @author vladimir
 */
public class Preprocessor {
    private Preprocessor() {
    }

    public static final Parser<String> wordParser = Scanners.IDENTIFIER;
    public static final Parser<String> nonWordParser = Scanners.pattern(Patterns.regex("[^a-zA-Z]"), "nonword").source();
    public static final Parser<String> termsTokenizer = Parsers.or(Terminals.StringLiteral.SINGLE_QUOTE_TOKENIZER,
            Terminals.StringLiteral.DOUBLE_QUOTE_TOKENIZER, wordParser, nonWordParser).source();
    static final Parser<List<Token>> tokenizer = termsTokenizer.lexer(Scanners.SQL_DELIMITER); // could use Scanners.WHITESPACES instead of SQL_DELIMITER

    public static String applyAliases(String clause, Map<String, String> aliases) {
        if(Strings.isNullOrEmpty(clause)) {
            return clause;
        }
        // tokenize while ignoring quoted strings
        final List<Token> tokens = tokenizer.parse(clause);

        // for each token, look up the NameExpression in a map and if found replace with the provided replacement expression
        for(int i = tokens.size()-1; i >= 0; i--) {
            final Token token = tokens.get(i);
            final String tokenStr = tokenAsString(token);
            final String replacement = aliases.get(tokenStr);
            if(replacement != null) {
                // TODO: optimize?
                clause = clause.substring(0, token.index()) + replacement + clause.substring(token.index() + token.length());
            }
        }

        return clause;
    }

    private static String tokenAsString(Token token) {
        return token != null ? (String) token.value() : "";
    }
}
