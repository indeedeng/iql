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
import com.google.common.collect.Lists;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import com.indeed.iql1.sql.ast.Expression;
import com.indeed.iql1.sql.ast.FunctionExpression;
import com.indeed.iql1.sql.ast2.FromClause;
import com.indeed.iql1.sql.ast2.GroupByClause;
import com.indeed.iql1.sql.ast2.IQL1SelectStatement;
import com.indeed.iql1.sql.ast2.QueryParts;
import com.indeed.iql1.sql.ast2.SelectClause;
import com.indeed.iql1.sql.ast2.WhereClause;
import org.codehaus.jparsec.Parser;
import org.codehaus.jparsec.Parsers;
import org.codehaus.jparsec.Scanners;
import org.codehaus.jparsec.Terminals;
import org.codehaus.jparsec.functors.Map;
import org.codehaus.jparsec.misc.Mapper;
import org.joda.time.DateTime;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * @author vladimir
 */

public class SelectStatementParser {
    private SelectStatementParser() {
    }

    public static int LOWEST_YEAR_ALLOWED = 0;

    public static IQL1SelectStatement parseSelectStatement(
            final String selectQuery,
            final DateTime querySubmitTime,
            final ImhotepMetadataCache metadata) {
        final QueryParts parts;
        try {
            parts = QuerySplitter.splitQuery(selectQuery);
        } catch (Exception e) {
            throw new IqlKnownException.StatementParseException(e, "splitter");
        }
        return parseSelectStatement(parts, querySubmitTime, metadata);
    }

    static IQL1SelectStatement parseSelectStatement(
            final QueryParts parts,
            final DateTime querySubmitTime,
            final ImhotepMetadataCache metadata) {
        final SelectClause select;
        final FromClause from;
        final WhereClause where;
        final GroupByClause groupBy;

        try {
            from = parseFromClause(parts.from, querySubmitTime, false);
        } catch (Exception e) {
            throw new IqlKnownException.StatementParseException(e, "from");
        }
        final String dataset = from.getDataset();
        final java.util.Map<String, String> aliases = metadata != null ? metadata.getDataset(dataset).getIql1ExpressionAliases() : Collections.<String, String>emptyMap();

        try {
            select = parseSelectClause(parts.select, aliases);
        } catch (Exception e) {
            throw new IqlKnownException.StatementParseException(e, "select");
        }

        try {
            where = parseWhereClause(parts.where, aliases);
        } catch (Exception e) {
            throw new IqlKnownException.StatementParseException(e, "where");
        }
        try {
            groupBy = parseGroupByClause(parts.groupBy, aliases);
        } catch (Exception e) {
            throw new IqlKnownException.StatementParseException(e, "groupBy");
        }
        int limit = parseLimit(parts.limit);

        return new IQL1SelectStatement(select, from, where, groupBy, limit);
    }

    private static int parseLimit(String limit) {
        if (limit.isEmpty()) {
            return Integer.MAX_VALUE - 1;
        }
        try {
            int limitInt = Integer.valueOf(limit);
            if( limitInt > 0 && limitInt <= Integer.MAX_VALUE - 1) {
                return limitInt;
            }
        } catch (NumberFormatException ignored) {}
        throw new IqlKnownException.RowLimitErrorException("Query Limit should be a positive, not exceeding " + (Integer.MAX_VALUE - 1));
    }

    static GroupByClause parseGroupByClause(String text) {
        return parseGroupByClause(text, Collections.<String, String>emptyMap());
    }
    static GroupByClause parseGroupByClause(String text, java.util.Map<String, String> aliases) {
        if(Strings.isNullOrEmpty(text)) {
            return null;
        }
        text = Preprocessor.applyAliases(text, aliases);

        Parser<Expression> expr = ExpressionParser.groupByExpression();
        Parser<GroupByClause> groupByParser = Mapper.curry(GroupByClause.class).sequence(expr.sepBy1(TerminalParser.term(",")));
        return TerminalParser.parse(groupByParser, text);
    }

    public static WhereClause parseWhereClause(String text) {
        return parseWhereClause(text, Collections.<String, String>emptyMap());
    }
    public static WhereClause parseWhereClause(String text, java.util.Map<String, String> aliases) {
        if(Strings.isNullOrEmpty(text)) {
            return null;
        }
        text = Preprocessor.applyAliases(text, aliases);

        final Expression whereExpression = ExpressionParser.parseWhereExpression(text);
        return new WhereClause(whereExpression);
    }

    public static FromClause parseFromClause(final String text, final DateTime querySubmitTime, final boolean allowIllegalDates) {
        Parser<String> tokenizer = Parsers.or(Terminals.StringLiteral.SINGLE_QUOTE_TOKENIZER,
                Terminals.StringLiteral.DOUBLE_QUOTE_TOKENIZER, QuerySplitter.wordParser);
        Parser<FromClause> fromParser = TerminalParser.STRING.atLeast(3).map(new Map<List<String>, FromClause>() {
            @Override
            public FromClause map(List<String> parts) {
                String dataset = parts.get(0);
                String start = "";
                String end = "";
                if(parts.size() == 3) {
                    start = parts.get(1);
                    end = parts.get(2);
                } else if(parts.size() == 5) {
                    start = parts.get(1) + " " + parts.get(2);
                    end = parts.get(3) + " " + parts.get(4);
                } else if(parts.size() == 4) {
                    if(parts.get(2).contains("-")) {
                        // start is 1 word date, end is 2 word date+time
                        start = parts.get(1);
                        end = parts.get(2) + " " + parts.get(3);
                    } else {
                        // start is 2 word date+time, end is 1 word date
                        start = parts.get(1) + " " + parts.get(2);
                        end = parts.get(3);
                    }
                }

                DateTime startTime;
                DateTime endTime;

                try {
                    startTime = new DateTime(start.replace(' ', 'T'));
                } catch (IllegalArgumentException ignored) {
                    if(Strings.isNullOrEmpty(start)) {
                        startTime = querySubmitTime.withTimeAtStartOfDay();
                    } else {
                        startTime = tryParseRelativeDate(start, querySubmitTime);
                    }
                }

                try {
                    endTime = new DateTime(end.replace(' ', 'T'));
                } catch (IllegalArgumentException ignored) {
                    if(Strings.isNullOrEmpty(end)) {
                        endTime = querySubmitTime.plusDays(1).withTimeAtStartOfDay();
                    } else {
                        endTime = tryParseRelativeDate(end, querySubmitTime);
                    }
                }

                // check if it's a unix timestamp
                if(startTime == null) {
                    startTime = tryParseUnixTimestamp(start);
                }
                if(endTime == null) {
                    endTime = tryParseUnixTimestamp(end);
                }


                if(!allowIllegalDates) {
                    if (startTime == null) {
                        throw new IqlKnownException.ParseErrorException("Start date parsing failed: " + start);
                    }
                    if (endTime == null) {
                        throw new IqlKnownException.ParseErrorException("End date parsing failed: " + end);
                    }
                    if (!startTime.isBefore(endTime)) {
                        throw new IqlKnownException.ParseErrorException("Start date has to be before the end date. start: " + startTime + ", end: " + endTime);
                    }
                    if (startTime.isBefore(new DateTime(LOWEST_YEAR_ALLOWED, 1, 1, 0, 0))) {
                        throw new IqlKnownException.ParseErrorException("The start date appears to be too low. Check for a typo: " + startTime);
                    }
                }
                return new FromClause(dataset, startTime, endTime, start, end);
            }
        });
        return fromParser.from(tokenizer, Scanners.SQL_DELIMITER).parse(text);
    }

    @Nullable
    private static DateTime tryParseRelativeDate(final String value, final DateTime querySubmitTime) {
        if(Strings.isNullOrEmpty(value)) {
            return querySubmitTime.withTimeAtStartOfDay();
        }
        final String lowercasedValue = value.toLowerCase();
        if("yesterday".startsWith(lowercasedValue)) {
            return querySubmitTime.minusDays(1).withTimeAtStartOfDay();
        } else if ("today".startsWith(lowercasedValue)) {
            return querySubmitTime.withTimeAtStartOfDay();
        } else if("tomorrow".startsWith(lowercasedValue)) {
            return querySubmitTime.plusDays(1).withTimeAtStartOfDay();
        }

        Period period = PeriodParser.parseString(value);
        if(period == null) {
            return null;
        }
        return querySubmitTime.withTimeAtStartOfDay().minus(period);
    }

    @Nullable
    private static DateTime tryParseUnixTimestamp(String value) {
        try {
            long timestamp = Long.valueOf(value);
            if(timestamp < Integer.MAX_VALUE) {
                timestamp *= 1000;  // seconds to milliseconds
            }
            return new DateTime(timestamp);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    public static SelectClause parseSelectClause(String text) {
        return parseSelectClause(text, Collections.<String, String>emptyMap());
    }

    public static SelectClause parseSelectClause(String text, java.util.Map<String, String> aliases) {
        if(Strings.isNullOrEmpty(text)) {
            return defaultSelect();
        }
        text = Preprocessor.applyAliases(text, aliases);

        Parser<Expression> expr = ExpressionParser.expression();
        Parser<List<Expression>> selectParser = expr.sepBy1(TerminalParser.term(","));
        List<Expression> result = TerminalParser.parse(selectParser, text);
        if(result == null || result.isEmpty()) {
            return defaultSelect();
        }
        return new SelectClause(result);
    }

    private static SelectClause defaultSelect() {
        // default to counts()
        return new SelectClause(Lists.newArrayList((Expression)new FunctionExpression("count", Collections.<Expression>emptyList())));
    }
}
