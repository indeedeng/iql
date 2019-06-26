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
import com.indeed.iql.Constants;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql1.sql.ast2.FromClause;
import org.codehaus.jparsec.Parser;
import org.codehaus.jparsec.Parsers;
import org.codehaus.jparsec.Scanners;
import org.codehaus.jparsec.Terminals;
import org.codehaus.jparsec.functors.Map;
import org.joda.time.DateTime;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.List;

/**
 * @author vladimir
 */

public class SelectStatementParser {
    private SelectStatementParser() {
    }

    public static int LOWEST_YEAR_ALLOWED = 0;

    public static FromClause parseFromClause(final String text, final DateTime querySubmitTime, final boolean allowIllegalDates) {
        Parser<String> tokenizer = Parsers.or(Terminals.StringLiteral.SINGLE_QUOTE_TOKENIZER,
                Terminals.StringLiteral.DOUBLE_QUOTE_TOKENIZER, QuerySplitter.wordParser);
        Parser<FromClause> fromParser = TerminalParser.STRING.atLeast(3).map(new Map<List<String>, FromClause>() {
            @Override
            public FromClause map(List<String> parts) {
                String dataset = parts.get(0);
                final String start;
                final String end;
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
                } else {
                    throw new IqlKnownException.ParseErrorException("Cannot parse from clause: " + text);
                }

                DateTime startTime;
                DateTime endTime;

                try {
                    startTime = new DateTime(start.replace(' ', 'T'), Constants.DEFAULT_IQL_TIME_ZONE);
                } catch (IllegalArgumentException ignored) {
                    if(Strings.isNullOrEmpty(start)) {
                        startTime = querySubmitTime.withTimeAtStartOfDay();
                    } else {
                        startTime = tryParseRelativeDate(start, querySubmitTime);
                    }
                }

                try {
                    endTime = new DateTime(end.replace(' ', 'T'), Constants.DEFAULT_IQL_TIME_ZONE);
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
                    if (startTime.isBefore(new DateTime(LOWEST_YEAR_ALLOWED, 1, 1, 0, 0, Constants.DEFAULT_IQL_TIME_ZONE))) {
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
            return new DateTime(timestamp, Constants.DEFAULT_IQL_TIME_ZONE);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }
}
