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

import com.google.common.collect.ImmutableList;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.JQLParser;
import com.indeed.iql2.language.util.ValidationUtil;
import com.indeed.util.core.Pair;
import com.indeed.util.core.time.WallClock;
import org.antlr.v4.runtime.Token;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TimePeriods {

    private static final int MAX_RECOMMENDED_BUCKETS = 1000;
    private static final List<TimeUnit> inferenceBucketSizeOptions = ImmutableList.of(TimeUnit.SECOND, TimeUnit.MINUTE, TimeUnit.HOUR, TimeUnit.DAY, TimeUnit.WEEK);

    public static List<Pair<Integer, TimeUnit>> parseTimePeriod(JQLParser.TimePeriodContext timePeriodContext, boolean useLegacy) {
        if (timePeriodContext == null) {
            return Collections.singletonList(Pair.of(1, TimeUnit.HOUR));
        } else if (timePeriodContext instanceof JQLParser.TimePeriodParseableContext) {
            final JQLParser.TimePeriodParseableContext periodContext = (JQLParser.TimePeriodParseableContext) timePeriodContext;
            final List<Pair<Integer, TimeUnit>> result = new ArrayList<>();
            for (JQLParser.TimeUnitContext timeunit : periodContext.timeunits) {
                final int coeff = (timeunit.coeff == null) ? 1 : Integer.parseInt(timeunit.coeff.getText());
                final TimeUnit unit = TimeUnit.fromString(timeunit.unit.getText(), useLegacy);
                result.add(Pair.of(coeff, unit));
            }
            for (final Token atom : periodContext.atoms) {
                int start = 0;
                final String raw = atom.getText();
                while (start < raw.length()) {
                    final int numberStart = start;
                    int current = start;
                    while (Character.isDigit(raw.charAt(current))) {
                        current++;
                    }
                    final int numberEndExcl = current;

                    final int periodStart = current;
                    while (current < raw.length() && Character.isAlphabetic(raw.charAt(current))) {
                        current++;
                    }
                    final int periodEndExcl = current;

                    final String coeffString = raw.substring(numberStart, numberEndExcl);
                    final int coeff = coeffString.isEmpty() ? 1 : Integer.parseInt(coeffString);
                    final TimeUnit unit = TimeUnit.fromString(raw.substring(periodStart, periodEndExcl), useLegacy);
                    result.add(Pair.of(coeff, unit));

                    start = current;
                }
            }
            return result;
        } else if (timePeriodContext instanceof JQLParser.TimePeriodStringLiteralContext) {
            final String unquoted = ParserCommon.unquote(((JQLParser.TimePeriodStringLiteralContext) timePeriodContext).STRING_LITERAL().getText());
            final JQLParser parser = Queries.parserForString(unquoted);
            final List<Pair<Integer, TimeUnit>> result = parseTimePeriod(parser.timePeriod(), useLegacy);
            if (parser.getNumberOfSyntaxErrors() > 0) {
                throw new IqlKnownException.ParseErrorException("Syntax errors encountered parsing quoted time period: [" + unquoted + "]");
            }
            return result;
        } else {
            throw new IqlKnownException.ParseErrorException("Failed to handle time period context: [" + timePeriodContext.getText() + "]");
        }

    }

    public static DateTime timePeriodDateTime(JQLParser.TimePeriodContext timePeriodContext, WallClock clock, boolean useLegacy) {
        final List<Pair<Integer, TimeUnit>> pairs = parseTimePeriod(timePeriodContext, useLegacy);
        DateTime dt = new DateTime(clock.currentTimeMillis()).withTimeAtStartOfDay();
        for (final Pair<Integer, TimeUnit> pair : pairs) {
            dt = TimeUnit.subtract(dt, pair.getFirst(), pair.getSecond());
        }
        return dt;
    }

    public static long inferTimeBucketSize(final long earliestStart, final long latestEnd) {
        for (final TimeUnit timeUnit: inferenceBucketSizeOptions) {
            if ((latestEnd - earliestStart)/timeUnit.millis < MAX_RECOMMENDED_BUCKETS) {
                return timeUnit.millis;
            }
        }
        return TimeUnit.WEEK.millis;
    }

    public static String inferTimeBucketSizeString(final DateTime start,final DateTime end) {
        final long timeBucketSizeMillis = inferTimeBucketSize(start.getMillis(), end.getMillis());
        final StringBuilder inferedTimeStringBuilder = new StringBuilder();
        ValidationUtil.appendTimePeriod(timeBucketSizeMillis/TimeUnit.SECOND.millis, inferedTimeStringBuilder);
        return inferedTimeStringBuilder.toString();
    }
}
