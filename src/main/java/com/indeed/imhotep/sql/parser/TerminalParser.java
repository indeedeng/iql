/*
 * Copyright (C) 2014 Indeed Inc.
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
 /*****************************************************************************
 * Copyright (C) Codehaus.org                                                *
 * ------------------------------------------------------------------------- *
 * Licensed under the Apache License, Version 2.0 (the "License");           *
 * you may not use this file except in compliance with the License.          *
 * You may obtain a copy of the License at                                   *
 *                                                                           *
 * http://www.apache.org/licenses/LICENSE-2.0                                *
 *                                                                           *
 * Unless required by applicable law or agreed to in writing, software       *
 * distributed under the License is distributed on an "AS IS" BASIS,         *
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 * See the License for the specific language governing permissions and       *
 * limitations under the License.                                            *
 *****************************************************************************/
package com.indeed.imhotep.sql.parser;

import com.google.common.base.Throwables;
import org.codehaus.jparsec.*;
import org.codehaus.jparsec.misc.Mapper;

import java.util.Arrays;
import java.util.List;

/**
 * Lexers and terminal level parsers for IQL.
 *
 * @author Ben Yu
 */
public final class TerminalParser {

    private static final String[] WHERE_OPERATORS = {
            "=", ":", "!=", "-",
            ",", "(", ")" // for IN
    };

    private static final String[] OPERATORS = {
            "+", "-", "*", "/", "%", ",", "(", ")", "\\",   // general expression operators
            "[", "]",   // for top terms in 'group by'
            "<", ">", "<=", ">=",   // where inequalities
            "=", "!=", ":", "=~", "!=~", // where equalities
    };

    private static final String[] WHERE_KEYWORDS = {
            "and", "in", "not"
    };
    private static final String[] KEYWORDS = WHERE_KEYWORDS;

    private static final Terminals TERMS = Terminals.caseInsensitive(OPERATORS, KEYWORDS);

    private static final Parser<?> BASE_TOKENIZER = Parsers.or(Terminals.IntegerLiteral.TOKENIZER, Terminals.StringLiteral.SINGLE_QUOTE_TOKENIZER, Terminals.StringLiteral.DOUBLE_QUOTE_TOKENIZER);
    private static final Parser<?> TOKENIZER = Parsers.or(BASE_TOKENIZER, TERMS.tokenizer());


    static final Parser<String> NUMBER = Terminals.IntegerLiteral.PARSER;
    static final Parser<String> STRING = Terminals.StringLiteral.PARSER;
    static final Parser<String> NAME = Terminals.Identifier.PARSER;

    public static <T> T parse(Parser<T> parser, String source) {
        return parser.from(TOKENIZER, Scanners.SQL_DELIMITER).parse(source);
    }

    public static Parser<List<Token>> LEXER = TOKENIZER.lexer(Scanners.SQL_DELIMITER);

    public static Parser<?> term(String term) {
        try {
            return Mapper._(TERMS.token(term));
        } catch (Throwable t) {
            System.out.println(term);
            throw Throwables.propagate(t);
        }
    }

    /**
     * Run parser if next token is not "term"
     */
    public static <T> Parser<T> notTerm(String term, Parser<T> parser) {
        return Parsers.sequence(term(term).not(), parser);
    }

    public static Parser<?> terms(String... terms) {
        try {
            return Mapper._(TERMS.token(terms));
        } catch (Throwable t) {
            System.out.println(Arrays.toString(terms));
            throw Throwables.propagate(t);
        }
    }

    public static Parser<?> phrase(String phrase) {
        return Mapper._(TERMS.phrase(phrase.split("\\s")));
    }
}
