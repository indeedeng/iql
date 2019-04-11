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

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.indeed.iql2.language.query.Queries;
import com.indeed.util.core.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

@SuppressWarnings("unchecked")
public class TimePeriodsTest {
    private static class TestDef {
        public final List<Pair<Integer, TimeUnit>> expected;
        public final String withSpaces;
        public final Optional<Boolean> useLegacy;

        private TestDef(String withSpaces, Pair<Integer, TimeUnit>... expected) {
            this.expected = Arrays.asList(expected);
            this.withSpaces = withSpaces;
            useLegacy = Optional.empty();
        }


        private TestDef(String withSpaces, boolean useLegacy, Pair<Integer, TimeUnit>... expected) {
            this.expected = Arrays.asList(expected);
            this.withSpaces = withSpaces;
            this.useLegacy = Optional.of(useLegacy);
        }

    }

    // TODO: test relative time like 'today', 'yesterday', 'tomorrow' or '5days ago' somehow

    private static final List<TestDef> BUCKETS = Arrays.asList(
            new TestDef("1 b", Pair.of(1, TimeUnit.BUCKETS)),
            new TestDef("1 bucket", Pair.of(1, TimeUnit.BUCKETS)),
            new TestDef("100 buckets", Pair.of(100, TimeUnit.BUCKETS)),
            new TestDef("BUCKET", Pair.of(1, TimeUnit.BUCKETS))
    );

    private static final List<TestDef> TIME_INTERVAL = Arrays.asList(
            new TestDef("1 y", Pair.of(1, TimeUnit.YEAR)),
            new TestDef("1 year", Pair.of(1, TimeUnit.YEAR)),
            new TestDef("100 years", Pair.of(100, TimeUnit.YEAR)),
            new TestDef("1 M", false, Pair.of(1, TimeUnit.MONTH)),
            new TestDef("1 M", true, Pair.of(1, TimeUnit.MINUTE)),
            new TestDef("1 month", Pair.of(1, TimeUnit.MONTH)),
            new TestDef("100 months", Pair.of(100, TimeUnit.MONTH)),
            new TestDef("1 w", Pair.of(1, TimeUnit.WEEK)),
            new TestDef("1 week", Pair.of(1, TimeUnit.WEEK)),
            new TestDef("100 weeks", Pair.of(100, TimeUnit.WEEK)),
            new TestDef("1 d", Pair.of(1, TimeUnit.DAY)),
            new TestDef("1 day", Pair.of(1, TimeUnit.DAY)),
            new TestDef("100 days", Pair.of(100, TimeUnit.DAY)),
            new TestDef("0h", Pair.of(0, TimeUnit.HOUR)),
            new TestDef("1 hour", Pair.of(1, TimeUnit.HOUR)),
            new TestDef("100 hours", Pair.of(100, TimeUnit.HOUR)),
            new TestDef("1 m", Pair.of(1, TimeUnit.MINUTE)),
            new TestDef("1 minute", Pair.of(1, TimeUnit.MINUTE)),
            new TestDef("100 minutes", Pair.of(100, TimeUnit.MINUTE)),
            new TestDef("1 s", Pair.of(1, TimeUnit.SECOND)),
            new TestDef("1 second", Pair.of(1, TimeUnit.SECOND)),
            new TestDef("100 seconds", Pair.of(100, TimeUnit.SECOND)),
            new TestDef("1234567890h", Pair.of(1234567890, TimeUnit.HOUR)),
            new TestDef("minute", Pair.of(1, TimeUnit.MINUTE)),
            new TestDef("HOUR", Pair.of(1, TimeUnit.HOUR)),
            new TestDef("d", Pair.of(1, TimeUnit.DAY)),
            new TestDef(
                    "y 3d",
                    Pair.of(1, TimeUnit.YEAR),
                    Pair.of(3, TimeUnit.DAY)
            ),
            new TestDef(
                    "3d 5h 10m",
                    Pair.of(3, TimeUnit.DAY),
                    Pair.of(5, TimeUnit.HOUR),
                    Pair.of(10, TimeUnit.MINUTE)
            ),
            new TestDef(
                    "1s 15m 200h 50d 2w 1M 5y",
                    false,
                    Pair.of(1, TimeUnit.SECOND),
                    Pair.of(15, TimeUnit.MINUTE),
                    Pair.of(200, TimeUnit.HOUR),
                    Pair.of(50, TimeUnit.DAY),
                    Pair.of(2, TimeUnit.WEEK),
                    Pair.of(1, TimeUnit.MONTH),
                    Pair.of(5, TimeUnit.YEAR)
            ),
            new TestDef(
                    "1M 2d",
                    true,
                    Pair.of(1, TimeUnit.MINUTE),
                    Pair.of(2, TimeUnit.DAY)
            )
    );

    @Test
    public void testTimeBucket() throws Exception {
        // bucket is real bucket or time interval
        for (final TestDef testCase : Iterables.concat(BUCKETS, TIME_INTERVAL)) {
            verifyTestDef(PARSE_TIME_BUCKET, testCase.expected, testCase.withSpaces, testCase.useLegacy);
            verifyTestDef(PARSE_TIME_BUCKET, testCase.expected, testCase.withSpaces.replace(" ", ""), testCase.useLegacy);
        }
    }

    @Test
    public void testTimeInterval() throws Exception {
        for (final TestDef testCase : TIME_INTERVAL) {
            verifyTestDef(PARSE_TIME_INTERVAL, testCase.expected, testCase.withSpaces, testCase.useLegacy);
            verifyTestDef(PARSE_TIME_INTERVAL, testCase.expected, testCase.withSpaces.replace(" ", ""), testCase.useLegacy);
        }
    }

    private void verifyTestDef(
            final Function<Pair<String, Boolean>, List<Pair<Integer, TimeUnit>>> stringToUnits,
            final List<Pair<Integer, TimeUnit>> expected,
            final String timeStr,
            final Optional<Boolean> useLegacy) {
        if (useLegacy.isPresent()) {
            checkOrderInvariant(expected, stringToUnits.apply(Pair.of(timeStr, useLegacy.get())));
        } else {
            checkOrderInvariant(expected, stringToUnits.apply(Pair.of(timeStr, true)));
            checkOrderInvariant(expected, stringToUnits.apply(Pair.of(timeStr, false)));
        }
    }

    private static final Function<Pair<String, Boolean>, List<Pair<Integer, TimeUnit>>> PARSE_TIME_BUCKET =
            pair -> {
                final JQLParser.TimeBucketContext ctx = Queries.runParser(pair.getFirst(), JQLParser::timeBucketTerminal).timeBucket();
                return TimePeriods.parseTimeBuckets(ctx, pair.getSecond());
            };

    private static final Function<Pair<String, Boolean>, List<Pair<Integer, TimeUnit>>> PARSE_TIME_INTERVAL =
            pair -> {
                final JQLParser.RelativeTimeTerminalContext ctx = Queries.runParser(pair.getFirst(), JQLParser::relativeTimeTerminal);
                if (ctx.timeInterval() != null) {
                    return TimePeriods.parseTimeIntervals(ctx.timeInterval().getText(), pair.getSecond());
                }
                if (ctx.relativeTime() != null) {
                    return TimePeriods.parseTimeIntervals(ctx.relativeTime().getText(), pair.getSecond());
                }
                throw new IllegalStateException();
            };

    private static <T> void checkOrderInvariant(List<T> expected, List<T> actual) {
        final Set<T> expectedSet = Sets.newHashSet(expected);
        Assert.assertEquals("Must have unique expected elements", expected.size(), expectedSet.size());
        final Set<T> actualSet = Sets.newHashSet(actual);
        Assert.assertEquals("Must have unique actual elements", actual.size(), actualSet.size());
        Assert.assertEquals(expectedSet, actualSet);
    }
}