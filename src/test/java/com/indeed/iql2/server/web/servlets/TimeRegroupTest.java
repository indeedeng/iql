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

package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.iql.Constants;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class TimeRegroupTest extends BasicTest {
    @Test
    public void testTimeRegroup() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "1180", "45", "3"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "600", "60", "1"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "600", "180", "1"));
        for (int i = 3; i < 23; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", String.valueOf(i), "1", "1"));
        }
        expected.add(ImmutableList.of("[2015-01-01 23:00:00, 2015-01-02 00:00:00)", "1", "23", "1", "1"));

        QueryServletTestUtils.testAll(expected, "from organic yesterday today group by time(1h) select count(), oji, ojc, distinct(tk)");
        QueryServletTestUtils.testAll(expected, "from organic yesterday today group by time(24b) select count(), oji, ojc, distinct(tk)");
        QueryServletTestUtils.testIQL2AndLegacy(expected, "from organic yesterday today group by time(1h, DEFAULT, unixtime) select count(), oji, ojc, distinct(tk)");
        QueryServletTestUtils.testIQL2AndLegacy(expected, "from organic yesterday today group by time(1h, DEFAULT, 2 * unixtime - unixtime) select count(), oji, ojc, distinct(tk)");
        // Remove DISTINCT to allow streaming, rather than regroup.
        QueryServletTestUtils.testAll(QueryServletTestUtils.withoutLastColumn(expected), "from organic yesterday today group by time(1h) select count(), oji, ojc");
        QueryServletTestUtils.testAll(QueryServletTestUtils.withoutLastColumn(expected), "from organic yesterday today group by time(24b) select count(), oji, ojc");

        // IQL1 filters out empty groups, so 2 tests here
        QueryServletTestUtils.testIQL1(ImmutableList.of(
                ImmutableList.of("[2015-01-01 23:00:00, 2015-01-01 23:30:00)", "1", "23", "1")),
                "from organic h today group by time(30minute) select count(), oji, ojc");
        QueryServletTestUtils.testIQL2(ImmutableList.of(
                ImmutableList.of("[2015-01-01 23:00:00, 2015-01-01 23:30:00)", "1", "23", "1"),
                ImmutableList.of("[2015-01-01 23:30:00, 2015-01-02 00:00:00)", "0", "0", "0")),
                "from organic h today group by time(30minute) select count(), oji, ojc");

        QueryServletTestUtils.testAll(ImmutableList.of(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "151", "2653", "306")), "from organic yesterday today group by time(d) select count(), oji, ojc");
        QueryServletTestUtils.testAll(ImmutableList.of(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "151", "2653", "306")), "from organic yesterday today group by time(1d) select count(), oji, ojc");
        QueryServletTestUtils.testAll(ImmutableList.of(ImmutableList.of("[2014-12-26 00:00:00, 2015-01-02 00:00:00)", "157", "2659", "312")), "from organic 7d today group by time(1W) select count(), oji, ojc");
    }

    @Test
    public void testTimeRegroupMultipleDatasource() throws Exception {
        // count flow: 10, 60, 60, 1, 1, ..., 1
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "0"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "0"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "0"));
        for (int i = 3; i < 12; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", "0"));
        }
        for (int i = 12; i < 23; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "0", "1"));
        }

        expected.add(ImmutableList.of("[2015-01-01 23:00:00, 2015-01-02 00:00:00)", "0", "1"));

        QueryServletTestUtils.testIQL2(expected, "from organic 24h 12h as o1, organic 12h today as o2 group by time(1h) select o1.count(), o2.count()");
        QueryServletTestUtils.testIQL2(expected, "from organic 24h 12h as o1, organic 12h today as o2 group by time(24b) select o1.count(), o2.count()");
    }

    @Test
    public void testTimeRegroupMultipleDatasourceHasHole() throws Exception {
        // count flow: 10, 60, 60, 1, 1, ..., 1
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "0"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "0"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "0"));
        for (int i = 3; i < 6; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", "0"));
        }
        for (int i = 6; i < 18; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "0", "0"));
        }
        for (int i = 18; i < 23; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "0", "1"));
        }

        expected.add(ImmutableList.of("[2015-01-01 23:00:00, 2015-01-02 00:00:00)", "0", "1"));

        QueryServletTestUtils.testIQL2(expected, "from organic 24h 18h as o1, organic 6h today as o2 group by time(1h) select o1.count(), o2.count()");
        QueryServletTestUtils.testIQL2(expected, "from organic 24h 18h as o1, organic 6h today as o2 group by time(24b) select o1.count(), o2.count()");
    }

    @Test
    public void testTimeRelativeEqualRange() throws Exception {
        // count flow: 10, 60, 60, 1, 1, ..., 1
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "1"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "1"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "1"));
        for (int i = 3; i < 12; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", "1"));
        }

        QueryServletTestUtils.testIQL2(expected, "from organic 24h 12h as o1, organic 12h today as o2 group by time(1h relative) select o1.count(), o2.count()");
    }

    @Test
    public void testTimeRelativeEqualRangeHasHole() throws Exception {
        // count flow: 10, 60, 60, 1, 1, ..., 1
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "1"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "1"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "1"));
        for (int i = 3; i < 6; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", "1"));
        }

        QueryServletTestUtils.testIQL2(expected, "from organic 24h 18h as o1, organic 6h today as o2 group by time(1h relative) select o1.count(), o2.count()");
    }

    @Test
    public void testTimeRelativeNonEqualRangeHasHole() throws Exception {
        // count flow: 10, 60, 60, 1, 1, ..., 1
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "1"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "1"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "1"));
        for (int i = 3; i < 6; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", "1"));
        }
        expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", 6, 7), "1", "0"));

        QueryServletTestUtils.testIQL2(expected, "from organic 24h 17h as o1, organic 6h today as o2 group by time(1h relative) select o1.count(), o2.count()");
    }

    @Test
    public void testTimeRelativeMultipleEqualRange() throws Exception {
        // count flow: 10, 60, 60, 1, 1, ..., 1
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "1", "1"));
        for (int i = 3; i < 8; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", "1", "1"));
        }

        QueryServletTestUtils.testIQL2(expected, "from organic 24h 16h as o1, organic 16h 8h as o2, organic 8h today as o3 group by time(1h relative) select o1.count(), o2.count(), o3.count()");
    }

    @Test
    public void testTimeRelativeAfterRangeLarger() throws Exception {
        // count flow: 10, 60, 60, 1, 1, ..., 1
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "1"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "1"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "1"));
        for (int i = 3; i < 11; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", "1"));
        }
        expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", 11, 12), "0", "1"));
        expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", 12, 13), "0", "1"));

        QueryServletTestUtils.testIQL2(expected, "from organic 24h 13h as o1, organic 13h today as o2 group by time(1h relative) select o1.count(), o2.count()");
    }

    @Test
    public void testTimeRelativePrevRangeLarger() throws Exception {
        // count flow: 10, 60, 60, 1, 1, ..., 1
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "1"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "1"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "1"));

        for (int i = 3; i < 11; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", "1"));
        }
        expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", 11, 12), "1", "0"));
        expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", 12, 13), "1", "0"));

        QueryServletTestUtils.testIQL2(expected, "from organic 24h 11h as o1, organic 11h today as o2 group by time(1h relative) select o1.count(), o2.count()");
    }

    @Test
    public void testMonthRegroup() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-02-01 00:00:00)", "10", "10"));
        expected.add(ImmutableList.of("[2015-02-01 00:00:00, 2015-03-01 00:00:00)", "100", "200"));
        expected.add(ImmutableList.of("[2015-03-01 00:00:00, 2015-04-01 00:00:00)", "1", "3"));
        QueryServletTestUtils.testIQL2(expected, "from multiMonth 2015-01-01 2015-04-01 group by time(1M) select count(), month");
        QueryServletTestUtils.testIQL2AndLegacy(expected, "from multiMonth 2015-01-01 2015-04-01 group by time(1month, default, 2 * unixtime - unixtime) select count(), month");
    }

    @Test
    public void testMonthRegroup2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "[2015-01-01 00:00:00, 2015-02-01 00:00:00)", "10", "10"));
        expected.add(ImmutableList.of("1", "[2015-02-01 00:00:00, 2015-03-01 00:00:00)", "0", "0"));
        expected.add(ImmutableList.of("1", "[2015-03-01 00:00:00, 2015-04-01 00:00:00)", "0", "0"));
        expected.add(ImmutableList.of("2", "[2015-01-01 00:00:00, 2015-02-01 00:00:00)", "0", "0"));
        expected.add(ImmutableList.of("2", "[2015-02-01 00:00:00, 2015-03-01 00:00:00)", "100", "200"));
        expected.add(ImmutableList.of("2", "[2015-03-01 00:00:00, 2015-04-01 00:00:00)", "0", "0"));
        expected.add(ImmutableList.of("3", "[2015-01-01 00:00:00, 2015-02-01 00:00:00)", "0", "0"));
        expected.add(ImmutableList.of("3", "[2015-02-01 00:00:00, 2015-03-01 00:00:00)", "0", "0"));
        expected.add(ImmutableList.of("3", "[2015-03-01 00:00:00, 2015-04-01 00:00:00)", "1", "3"));
        QueryServletTestUtils.testIQL2(expected, "from multiMonth 2015-01-01 2015-04-01 group by month, time(1M) select count(), month", true);
    }

    @Test
    public void testConsistentMonthRegroupCustomFormat() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01, 2015-02-01)", "10", "10"));
        expected.add(ImmutableList.of("[2015-02-01, 2015-03-01)", "100", "200"));
        expected.add(ImmutableList.of("[2015-03-01, 2015-04-01)", "1", "3"));
        QueryServletTestUtils.testIQL2(expected, "from multiMonth 2015-01-01 2015-04-01 group by time(1M, 'yyyy-MM-dd') select count(), month");
    }

    @Test
    public void testQuarterRegroup() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-04-01 00:00:00)", "3"));
        expected.add(ImmutableList.of("[2015-04-01 00:00:00, 2015-07-01 00:00:00)", "3"));
        expected.add(ImmutableList.of("[2015-07-01 00:00:00, 2015-10-01 00:00:00)", "3"));
        expected.add(ImmutableList.of("[2015-10-01 00:00:00, 2016-01-01 00:00:00)", "3"));
        expected.add(ImmutableList.of("[2016-01-01 00:00:00, 2016-04-01 00:00:00)", "3"));
        expected.add(ImmutableList.of("[2016-04-01 00:00:00, 2016-07-01 00:00:00)", "2"));
        QueryServletTestUtils.testIQL2AndLegacy(expected, "from multiYear 2015-01-01 2016-07-01  group by time(1q) select count()");
        QueryServletTestUtils.testIQL2AndLegacy(expected, "from multiYear 2015-01-01 2016-07-01  group by time(1 quarter) select count()");
    }

    @Test
    public void testYearRegroup() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2016-01-01 00:00:00)", "12"));
        expected.add(ImmutableList.of("[2016-01-01 00:00:00, 2017-01-01 00:00:00)", "5"));
        QueryServletTestUtils.testIQL2AndLegacy(expected, "from multiYear 2015-01-01 2017-01-01  group by time(1y) select count()");
        QueryServletTestUtils.testIQL2AndLegacy(expected, "from multiYear 2015-01-01 2017-01-01  group by time(1 year) select count()");
    }

    @Test
    public void testGroupByDayOfWeek() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("Monday", "18", "146"));
        expected.add(ImmutableList.of("Tuesday", "107", "691"));
        expected.add(ImmutableList.of("Wednesday", "6", "49"));
        expected.add(ImmutableList.of("Thursday", "51", "401"));
        expected.add(ImmutableList.of("Friday", "8", "37"));
        expected.add(ImmutableList.of("Saturday", "0", "0"));
        expected.add(ImmutableList.of("Sunday", "16", "169"));
        QueryServletTestUtils.testIQL2AndLegacy(expected, "from dayOfWeek 2015-01-01 2015-01-15 group by dayofweek select count(), day");
    }

    @Test
    public void testGroupByDayOfWeek2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "Monday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "Tuesday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "Wednesday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "Thursday", "1", "1"));
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "Friday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "Saturday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "Sunday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "Monday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "Tuesday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "Wednesday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "Thursday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "Friday", "5", "10"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "Saturday", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "Sunday", "0", "0"));
        QueryServletTestUtils.testIQL2AndLegacy(expected, "from dayOfWeek 2015-01-01 2015-01-03 group by time(1d), dayofweek select count(), day");
    }

    @Test
    public void testInvalidTimeBucket() {
        Predicate<String> containsTimeBucketErrorMessage = e -> (e.contains("You requested a time period") && e.contains("not evenly divisible by the bucket size "));
        QueryServletTestUtils.expectExceptionAll("FROM organic 10d today group by time(1w) select count()", containsTimeBucketErrorMessage.and(e -> e.contains("increase the time range by 4 days or reduce the time range by 3 days")));
        QueryServletTestUtils.expectExceptionAll("FROM organic 2015-01-01 2015-03-01 group by time(1w) select count()", containsTimeBucketErrorMessage.and(e -> e.contains("increase the time range by 4 days or reduce the time range by 3 days")));
        QueryServletTestUtils.expectExceptionAll("FROM organic 10d today group by time(3d) select count()", containsTimeBucketErrorMessage.and(e -> e.contains("increase the time range by 2 days or reduce the time range by 1 days")));
        QueryServletTestUtils.expectExceptionAll("FROM organic 2015-01-01T0:0:0 2015-01-01T0:0:3  group by time(2s) select count()", containsTimeBucketErrorMessage.and(e -> e.contains("increase the time range by 1 seconds or reduce the time range by 1 seconds")));
        QueryServletTestUtils.expectExceptionAll("FROM organic 10s today group by time(3b) select count()", containsTimeBucketErrorMessage.and(e -> e.contains("increase the time range by 2 seconds or reduce the time range by 1 seconds")));
        QueryServletTestUtils.expectExceptionAll("FROM organic 100s today group by time(7b) select count()", containsTimeBucketErrorMessage.and(e -> e.contains("increase the time range by 12 seconds or reduce the time range by 2 seconds")));
    }

    @Test
    public void GroupByInferredTime() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        DateTime startDate = new DateTime("2015-01-01T01:00:00", Constants.DEFAULT_IQL_TIME_ZONE);
        final DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").withZone(Constants.DEFAULT_IQL_TIME_ZONE);
        for (int i = 0; i < 60; i++) {
            expected.add(ImmutableList.of("[" + startDate.toString(dateTimeFormatter) + ", " + startDate.plusMinutes(1).toString(dateTimeFormatter) + ")", "1"));
            startDate = startDate.plusMinutes(1);
        }
        QueryServletTestUtils.testAll(expected, "from organic 2015-01-01 01:00:00 2015-01-01 02:00:00 group by time select count()"); // inferred time 1 minute
    }

    @Test
    public void GroupByInferredTime1() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        final DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").withZone(Constants.DEFAULT_IQL_TIME_ZONE);
        DateTime startDate = new DateTime("2015-01-01T03:00:00", Constants.DEFAULT_IQL_TIME_ZONE);
        for (int i = 0; i < 21; i++) {
            expected.add(ImmutableList.of("[" + startDate.toString(dateTimeFormatter) + ", " + startDate.plusHours(1).toString(dateTimeFormatter) + ")", "1"));
            startDate = startDate.plusHours(1);
        }
        QueryServletTestUtils.testAll(expected, "from organic 2015-01-01 03:00:00 2015-01-02 00:00:00 group by time() select count()"); // inferred time 1 hour
    }

    @Test
    public void GroupByInferredTimeRelative() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        //QueryServletTestUtils.testIQL2(expected, "from dataset1 2014-12-01 2014-12-20 group by time(1d) select count()");
        final DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").withZone(Constants.DEFAULT_IQL_TIME_ZONE);
        DateTime startDateJan = new DateTime("2015-01-01T00:00:00", Constants.DEFAULT_IQL_TIME_ZONE);
        expected.add(ImmutableList.of("[" + startDateJan.toString(dateTimeFormatter) + ", " + startDateJan.plusDays(1).toString(dateTimeFormatter) + ")", "2"));
        startDateJan = startDateJan.plusDays(1);
        for (int i = 0; i < 30; i++) {
            expected.add(ImmutableList.of("[" + startDateJan.toString(dateTimeFormatter) + ", " + startDateJan.plusDays(1).toString(dateTimeFormatter) + ")", "0"));
            startDateJan = startDateJan.plusDays(1);
        }

        DateTime startDateFeb = new DateTime("2015-02-01T00:00:00", Constants.DEFAULT_IQL_TIME_ZONE);
        expected.add(ImmutableList.of("[" + startDateFeb.toString(dateTimeFormatter) + ", " + startDateFeb.plusDays(1).toString(dateTimeFormatter) + ")", "2"));
        startDateFeb = startDateFeb.plusDays(1);
        for (int i = 0; i < 27; i++) {
            expected.add(ImmutableList.of("[" + startDateFeb.toString(dateTimeFormatter) + ", " + startDateFeb.plusDays(1).toString(dateTimeFormatter) + ")", "0"));
            startDateFeb = startDateFeb.plusDays(1);
        }
        QueryServletTestUtils.testIQL2(expected, "from multiyeardataset 2015-01-01 2015-03-01, multiyeardataset 2019-01-01 2019-03-01 as multiyear1 group by time(relative) select count()"); // infered time 1 day instead of 1 week
    }

    @Test
    public void GroupByInferredTimeRelativeBucket() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-02-01 00:00:00, 2015-02-15 00:00:00)", "2"));
        expected.add(ImmutableList.of("[2015-02-15 00:00:00, 2015-03-01 00:00:00)", "0"));
        QueryServletTestUtils.testIQL2(expected, "from multiyeardataset 2015-02-01 2015-03-01, multiyeardataset 2019-02-01 2019-03-01 as multiyear1 group by time(2b relative) select count()"); // infered time 1 day instead of 1 week
    }

    @Test
    public void TestGroupByTimeField() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 03:00:00, 2015-01-01 13:30:00)", "0"));
        expected.add(ImmutableList.of("[2015-01-01 13:30:00, 2015-01-02 00:00:00)", "0"));
        QueryServletTestUtils.testIQL2(expected, "from organic 2015-01-01 03:00:00 2015-01-02 00:00:00 group by time(2b,'YYYY-MM-dd HH:mm:ss', ojc) select count()");
        QueryServletTestUtils.testIQL2(expected, "from organic 2015-01-01 03:00:00 2015-01-02 00:00:00 group by time(2b,'YYYY-MM-dd HH:mm:ss', (ojc + ojc)/2) select count()");
    }

    @Test
    public void TestGroupByTimeFieldDefaultFormat() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 03:00:00, 2015-01-01 13:30:00)", "0"));
        expected.add(ImmutableList.of("[2015-01-01 13:30:00, 2015-01-02 00:00:00)", "0"));
        QueryServletTestUtils.testIQL2(expected, "from organic 2015-01-01 03:00:00 2015-01-02 00:00:00 group by time(2b, DEFAULT, ojc) select count()");
    }
}
