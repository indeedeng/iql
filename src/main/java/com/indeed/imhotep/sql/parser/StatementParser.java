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
 package com.indeed.imhotep.sql.parser;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.indeed.imhotep.sql.ast.*;
import com.indeed.imhotep.sql.ast2.*;
import com.indeed.imhotep.web.IQLParseException;
import com.indeed.imhotep.web.ImhotepMetadataCache;
import org.codehaus.jparsec.*;
import org.codehaus.jparsec.functors.Map;
import org.codehaus.jparsec.misc.Mapper;
import org.joda.time.DateTime;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.indeed.imhotep.sql.parser.TerminalParser.term;

/**
 * @author vladimir
 */

public class StatementParser {
    private static final Pattern selectPattern = Pattern.compile("(?i)\\s*(?:select|from) .*");
    private static final Pattern showPattern = Pattern.compile("(?i)\\s*show\\s+(?:tables|datasets).*");
    private static final Pattern describePattern = Pattern.compile("(?i)\\s*(?:describe|explain|desc)\\s+(\\w+)(?:(?:\\s+|\\.)(\\w+))?.*");
    public static int LOWEST_YEAR_ALLOWED = 0;


    public static IQLStatement parse(String statement) {
        return parse(statement, null);
    }
    public static IQLStatement parse(String statement, ImhotepMetadataCache metadata) {
        if(selectPattern.matcher(statement).matches()) {
            final QueryParts parts;
            try {
                parts = QuerySplitter.splitQuery(statement);
            } catch (Exception e) {
                throw new IQLParseException(e, "splitter");
            }
            return parseSelectStatement(parts, metadata);
        } else if(showPattern.matcher(statement).matches()) {
            return new ShowStatement();
        } else {
            Matcher matcher = describePattern.matcher(statement);
            if(matcher.matches()) {
                return new DescribeStatement(matcher.group(1), matcher.group(2));
            }
        }

        return null;
    }

    public static SelectStatement parseSelectStatement(QueryParts parts, ImhotepMetadataCache metadata) {
        final SelectClause select;
        final FromClause from;
        final WhereClause where;
        final GroupByClause groupBy;

        try {
            from = parseFromClause(parts.from, false);
        } catch (Exception e) {
            throw new IQLParseException(e, "from");
        }
        final String dataset = from.getDataset();
        final java.util.Map<String, String> aliases = metadata != null ? metadata.getDataset(dataset).getAliases() : Collections.<String, String>emptyMap();

        try {
            select = parseSelectClause(parts.select, aliases);
        } catch (Exception e) {
            throw new IQLParseException(e, "select");
        }

        try {
            where = parseWhereClause(parts.where, aliases);
        } catch (Exception e) {
            throw new IQLParseException(e, "where");
        }
        try {
            groupBy = parseGroupByClause(parts.groupBy, aliases);
        } catch (Exception e) {
            throw new IQLParseException(e, "groupBy");
        }
        int limit = parseLimit(parts.limit);

        return new SelectStatement(select, from, where, groupBy, limit);
    }

    private static int parseLimit(String limit) {
        try {
            int limitInt = Integer.valueOf(limit);
            if(limitInt > 0) {
                return limitInt;
            }
        } catch (Exception ignored) { }
        return Integer.MAX_VALUE;
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
        Parser<GroupByClause> groupByParser = Mapper.curry(GroupByClause.class).sequence(expr.sepBy1(term(",")));
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

    public static FromClause parseFromClause(String text, final boolean allowMacros) {
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
                        startTime = DateTime.now().withTimeAtStartOfDay();
                    } else {
                        startTime = tryParseRelativeDate(start);
                    }
                }

                try {
                    endTime = new DateTime(end.replace(' ', 'T'));
                } catch (IllegalArgumentException ignored) {
                    if(Strings.isNullOrEmpty(end)) {
                        endTime = DateTime.now().plusDays(1).withTimeAtStartOfDay();
                    } else {
                        endTime = tryParseRelativeDate(end);
                    }
                }

                // check if it's a unix timestamp
                if(startTime == null) {
                    startTime = tryParseUnixTimestamp(start);
                }
                if(endTime == null) {
                    endTime = tryParseUnixTimestamp(end);
                }


                if(startTime == null && (!allowMacros || !start.startsWith("$"))) {
                    throw new IllegalArgumentException("Start date parsing failed: " + start);
                }
                if(endTime == null && (!allowMacros || !end.startsWith("$"))) {
                    throw new IllegalArgumentException("End date parsing failed: " + end);
                }
                if(startTime != null && endTime != null && !startTime.isBefore(endTime)) {
                    throw new IllegalArgumentException("Start date has to be before the end date. start: " + startTime + ", end: " + endTime);
                }
                if(startTime != null && startTime.isBefore(new DateTime(LOWEST_YEAR_ALLOWED, 1, 1, 0, 0))) {
                    throw new IllegalArgumentException("The start date appears to be too low. Check for a typo: " + startTime);
                }
                return new FromClause(dataset, startTime, endTime, start, end);
            }
        });
        return fromParser.from(tokenizer, Scanners.SQL_DELIMITER).parse(text);
    }

    @Nullable
    private static DateTime tryParseRelativeDate(String value) {
        if(Strings.isNullOrEmpty(value)) {
            return new DateTime().withTimeAtStartOfDay();
        }
        final String lowercasedValue = value.toLowerCase();
        if("yesterday".startsWith(lowercasedValue)) {
            return DateTime.now().minusDays(1).withTimeAtStartOfDay();
        } else if ("today".startsWith(lowercasedValue)) {
            return new DateTime().withTimeAtStartOfDay();
        } else if("tomorrow".startsWith(lowercasedValue)) {
            return new DateTime().plusDays(1).withTimeAtStartOfDay();
        }

        Period period = PeriodParser.parseString(value);
        if(period == null) {
            return null;
        }
        return DateTime.now().withTimeAtStartOfDay().minus(period);
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
        Parser<List<Expression>> selectParser = expr.sepBy1(term(","));
        List<Expression> result = TerminalParser.parse(selectParser, text);
        if(result == null || result.size() == 0) {
            return defaultSelect();
        }
        return new SelectClause(result);
    }

    private static SelectClause defaultSelect() {
        // default to counts()
        return new SelectClause(Lists.newArrayList((Expression)new FunctionExpression("count", Collections.<Expression>emptyList())));
    }
}
