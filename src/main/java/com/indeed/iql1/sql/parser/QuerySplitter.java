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

import com.google.common.collect.Lists;
import com.indeed.iql1.sql.ast2.QueryParts;
import org.codehaus.jparsec.*;
import org.codehaus.jparsec.functors.Map5;
import org.codehaus.jparsec.misc.Mapper;
import org.codehaus.jparsec.pattern.Patterns;

import java.util.List;

/**
 * @author vladimir
 */

public class QuerySplitter {
    private static final String[] KEYWORDS = new String[] {
            "select", "from", "where", "group", "by", "limit", "=", ":", //query

    };

    public static final Parser<String> wordParser = Scanners.pattern(Patterns.regex("[^\\s]+"), "word").source();
    public static final Parser<String> tokensParser = Parsers.or(Terminals.StringLiteral.SINGLE_QUOTE_TOKENIZER,
            Terminals.StringLiteral.DOUBLE_QUOTE_TOKENIZER, wordParser).many1().source();
    private static final Terminals TERMS = Terminals.caseInsensitive(tokensParser, new String[0], KEYWORDS);

    public static QueryParts splitQuery(String query) {
        Parser<?> termsTokenizer = TERMS.tokenizer();
        Parser<Void> nonTokensParser = Scanners.WHITESPACES;
        Parser<List<Token>> tokenizer = termsTokenizer.lexer(nonTokensParser);

        Parser<QueryParts> querySQLParser = getQuerySQLParser();
        Parser<QueryParts> queryLINQParser = getQueryLINQParser();

        Parser<QueryParts> queryFragmentsParser = Parsers.or(queryLINQParser, querySQLParser);

        // combine Tokenizer with the token/fragment level Parser
        Parser<QueryParts> queryParser = queryFragmentsParser.from(tokenizer);
        // we probably could store this parser statically instead of recreating on each call

        // finally parse the query string
        return queryParser.parse(query);
    }

    private static Parser<QueryParts> getQueryLINQParser() {
        Parser<Token> selectLINQParser = TERMS.token("select").next(getContentParser("limit"));
        Parser<Token> fromLINQParser = TERMS.token("from").next(getContentParser("where", "group", "select", "limit"));
        Parser<Token> whereLINQParser = TERMS.token("where").next(getContentParser(true, "group", "select", "limit"));
        Parser<Token> groupByLINQParser = TERMS.phrase("group", "by").next(getContentParser("select", "limit"));
        Parser<Token> limitParser = TERMS.token("limit").next(getContentParser());

        return Parsers.sequence(fromLINQParser, whereLINQParser.optional(), groupByLINQParser.optional(), selectLINQParser.optional(), limitParser.optional(),
                new Map5<Token, Token, Token, Token, Token, QueryParts>() {
            @Override
            public QueryParts map(Token from, Token where, Token groupBy, Token select, Token limit) {
                return new QueryParts(from, where, groupBy, select, limit);
            }
        });
    }

    private static Parser<QueryParts> getQuerySQLParser() {
        Parser<Token> selectSQLParser = TERMS.token("select").next(getContentParser("from"));
        Parser<Token> fromSQLParser = TERMS.token("from").next(getContentParser("where", "group", "limit"));
        Parser<Token> whereSQLParser = TERMS.token("where").next(getContentParser(true, "group", "limit"));
        Parser<Token> groupBySQLParser = TERMS.phrase("group", "by").next(getContentParser("limit"));
        Parser<Token> limitParser = TERMS.token("limit").next(getContentParser());

        return Parsers.sequence(selectSQLParser.optional(), fromSQLParser, whereSQLParser.optional(), groupBySQLParser.optional(), limitParser.optional(),
                new Map5<Token, Token, Token, Token, Token, QueryParts>() {
                    @Override
                    public QueryParts map(Token select, Token from, Token where, Token groupBy, Token limit) {
                        return new QueryParts(from, where, groupBy, select, limit);
                    }
                });
    }

    private static Parser<Token> getContentParser(String... excludedTerms) {
        return getContentParser(false, excludedTerms);
    }
    private static Parser<Token> getContentParser(boolean allowExcludedAfterEq, String... excludedTerms) {
        List<Parser<?>> alternatives = Lists.newArrayList();
        if(allowExcludedAfterEq) {
            for(String term : excludedTerms) {
                alternatives.add(Mapper._(TERMS.phrase("=", term)));
                alternatives.add(Mapper._(TERMS.phrase(":", term)));
                alternatives.add(Mapper._(TERMS.phrase(term, "=")));
                alternatives.add(Mapper._(TERMS.phrase(term, ":")));
            }
        }
        // consume any token as long as it is not one of the excluded ones
        alternatives.add(TERMS.token(excludedTerms).not().next(Parsers.ANY_TOKEN));

        return Parsers.or(alternatives).many().source().token();
    }

    static void runBenchmark() {
        runBenchmarkJParsec();
        System.out.println("Warm up done");
        long start = System.currentTimeMillis();
        runBenchmarkJParsec();
        // 5.3s this jparsec implementation, 3.7s custom implementation
        System.out.println("done in " + (System.currentTimeMillis()-start));
    }
    private static void runBenchmarkJParsec() {
        for (int i = 0; i < 100000; i++) {
            splitQuery("select count() from ramsaccess \"2012-02-01T00:00:00\" \"2013-02-09T00:00:00\" where searches=1 and fields in (\"affiliate\", \"affchan\", \"affchannel\", \"affshr\", \"affshare\", \"agent\", \"asecmp\", \"ascompany\", \"asettl\", \"astitle\", \"emailuser\", \"emaildomain\", \"adschn\", \"adschan\", \"adschannel\", \"language\", \"ch\", \"chn\", \"spon\", \"clkcnt\", \"clk\", \"rq\", \"conversion\") group by fields");
        }
    }
}
