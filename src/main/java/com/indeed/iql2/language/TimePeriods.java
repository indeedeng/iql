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
import com.indeed.util.core.Pair;
import com.indeed.util.core.time.WallClock;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TimePeriods {
    private TimePeriods() {
    }

    private static final int MAX_RECOMMENDED_BUCKETS = 1000;
    private static final List<TimeUnit> inferenceBucketSizeOptions = ImmutableList.of(TimeUnit.SECOND, TimeUnit.MINUTE, TimeUnit.HOUR, TimeUnit.DAY, TimeUnit.WEEK);

    public static List<Pair<Integer, TimeUnit>> parseTimeBuckets(final JQLParser.TimeBucketContext timeBucketContext, final boolean useLegacy) {
        if (timeBucketContext.STRING_LITERAL() != null) {
            final String unquoted = ParserCommon.unquote(timeBucketContext.STRING_LITERAL().getText(), useLegacy);
            final JQLParser.TimeBucketTerminalContext bucketTerminal = Queries.tryRunParser(unquoted, JQLParser::timeBucketTerminal);
            if (bucketTerminal == null) {
                throw new IqlKnownException.ParseErrorException("Syntax errors encountered parsing bucket: [" + unquoted + "]");
            }
            // recursive call after unquoting
            return parseTimeBuckets(bucketTerminal.timeBucket(), useLegacy);
        }

        if (timeBucketContext.timeInterval() != null) {
            return parseTimeIntervals(timeBucketContext.timeInterval().getText(), useLegacy);
        }

        if (timeBucketContext.bucket() != null) {
            final JQLParser.BucketContext bucket = timeBucketContext.bucket();
            final int coeff;
            if (bucket.BUCKET_ATOM() != null) {
                final String raw = bucket.BUCKET_ATOM().getText();
                int current = 0;
                while (Character.isDigit(raw.charAt(current))) {
                    current++;
                }

                final String coeffString = raw.substring(0, current);
                coeff = coeffString.isEmpty() ? 1 : Integer.parseInt(coeffString);
            } else {
                coeff = (bucket.NAT() == null) ? 1 : Integer.parseInt(bucket.NAT().getText());
            }
            return Collections.singletonList(Pair.of(coeff, TimeUnit.BUCKETS));
        }

        throw new IqlKnownException.ParseErrorException("Syntax errors encountered parsing bucket: [" + timeBucketContext.getText() + "]");
    }

    // parse time interval string.
    public static List<Pair<Integer, TimeUnit>> parseTimeIntervals(
            final String interval,
            final boolean useLegacy) {
        final List<Pair<Integer, TimeUnit>> result = new ArrayList<>();
        int current = 0;
        while (current < interval.length()) {

            // skip spaces
            while (current < interval.length() && Character.isWhitespace(interval.charAt(current))) {
                current++;
            }

            // find coeff
            final int numberStart = current;
            while (Character.isDigit(interval.charAt(current))) {
                current++;
            }
            final int numberEndExcl = current;

            // skip spaces
            while (current < interval.length() && Character.isWhitespace(interval.charAt(current))) {
                current++;
            }

            // find TimeUnit
            final int periodStart = current;
            while (current < interval.length() && Character.isAlphabetic(interval.charAt(current))) {
                current++;
            }
            final int periodEndExcl = current;

            final String coeffString = interval.substring(numberStart, numberEndExcl);
            final int coeff = coeffString.isEmpty() ? 1 : Integer.parseInt(coeffString);
            final TimeUnit unit = TimeUnit.fromString(interval.substring(periodStart, periodEndExcl), useLegacy);
            result.add(Pair.of(coeff, unit));
        }
        return result;
    }

    public static DateTime subtract(final WallClock clock, final List<Pair<Integer, TimeUnit>> intervals) {
        DateTime dt = new DateTime(clock.currentTimeMillis()).withTimeAtStartOfDay();
        for (final Pair<Integer, TimeUnit> interval : intervals) {
            dt = TimeUnit.subtract(dt, interval.getFirst(), interval.getSecond());
        }
        return dt;
    }

    public static void appendTimePeriod(long timePeriodSeconds, StringBuilder builder) {
        if (timePeriodSeconds % TimeUnit.WEEK.toSeconds() == 0) {
            builder.append(timePeriodSeconds / TimeUnit.WEEK.toSeconds());
            builder.append(" weeks");
        } else if ((timePeriodSeconds % TimeUnit.DAY.toSeconds()) == 0) {
            builder.append(timePeriodSeconds / TimeUnit.DAY.toSeconds());
            builder.append(" days");
        } else if (timePeriodSeconds % TimeUnit.HOUR.toSeconds() == 0) {
            builder.append(timePeriodSeconds / TimeUnit.HOUR.toSeconds());
            builder.append(" hours");
        } else if (timePeriodSeconds % TimeUnit.MINUTE.toSeconds() == 0) {
            builder.append(timePeriodSeconds / TimeUnit.MINUTE.toSeconds());
            builder.append(" minutes");
        } else {
            builder.append(timePeriodSeconds);
            builder.append(" seconds");
        }
    }

    public static long inferTimeBucketSize(final long earliestStart, final long latestEnd, final long longestRange, final boolean isRelative) {
        final long timeRange = isRelative ? longestRange: latestEnd - earliestStart;
        for (final TimeUnit timeUnit : inferenceBucketSizeOptions) {
            if (timeRange / timeUnit.millis < MAX_RECOMMENDED_BUCKETS) {
                return timeUnit.millis;
            }
        }
        return TimeUnit.WEEK.millis;
    }

    public static String inferTimeBucketSizeString(final DateTime start,final DateTime end) {
        final long timeBucketSizeMillis = inferTimeBucketSize(start.getMillis(), end.getMillis(), end.getMillis() - start.getMillis(), false);
        final StringBuilder inferedTimeStringBuilder = new StringBuilder();
        appendTimePeriod(timeBucketSizeMillis/TimeUnit.SECOND.millis, inferedTimeStringBuilder);
        return inferedTimeStringBuilder.toString();
    }

    public static long getTimePeriodFromBucket(final long earliestStart, final long latestEnd, final long longestRange, final int numBuckets, final boolean isRelative) {
        final long timeRange = isRelative ? longestRange: latestEnd - earliestStart;
        return timeRange/numBuckets;
    }
}
