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
import com.indeed.iql2.execution.QueryOptions;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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

        QueryServletTestUtils.testAll(AllData.DATASET, expected, "from organic yesterday today group by time(1h) select count(), oji, ojc, distinct(tk)");
        QueryServletTestUtils.testAll(AllData.DATASET, expected, "from organic yesterday today group by time(24b) select count(), oji, ojc, distinct(tk)");
        // Remove DISTINCT to allow streaming, rather than regroup.
        QueryServletTestUtils.testAll(AllData.DATASET, QueryServletTestUtils.withoutLastColumn(expected), "from organic yesterday today group by time(1h) select count(), oji, ojc");
        QueryServletTestUtils.testAll(AllData.DATASET, QueryServletTestUtils.withoutLastColumn(expected), "from organic yesterday today group by time(24b) select count(), oji, ojc");

        // IQL1 filters out empty groups, so 2 tests here
        QueryServletTestUtils.testOriginalIQL1(AllData.DATASET, ImmutableList.of(
                ImmutableList.of("[2015-01-01 23:00:00, 2015-01-01 23:30:00)", "1", "23", "1")),
                "from organic h today group by time(30minute) select count(), oji, ojc");
        QueryServletTestUtils.testIQL2AndLegacy(AllData.DATASET, ImmutableList.of(
                ImmutableList.of("[2015-01-01 23:00:00, 2015-01-01 23:30:00)", "1", "23", "1"),
                ImmutableList.of("[2015-01-01 23:30:00, 2015-01-02 00:00:00)", "0", "0", "0")),
                "from organic h today group by time(30minute) select count(), oji, ojc");

        QueryServletTestUtils.testAll(AllData.DATASET, ImmutableList.of(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "151", "2653", "306")), "from organic yesterday today group by time(d) select count(), oji, ojc");
        // IQL1 fails with error: "You requested a time period (1 days) not evenly divisible by the bucket size (1 weeks)."
        QueryServletTestUtils.testIQL2AndLegacy(AllData.DATASET, ImmutableList.of(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-08 00:00:00)", "151", "2653", "306")), "from organic yesterday today group by time(1W) select count(), oji, ojc");
        // same query with fixed time interval
        QueryServletTestUtils.testIQL1(AllData.DATASET, ImmutableList.of(ImmutableList.of("[2014-12-26 00:00:00, 2015-01-02 00:00:00)", "157", "2659", "312")), "from organic 7d today group by time(1W) select count(), oji, ojc");
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

        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic 24h 12h as o1, organic 12h today as o2 group by time(1h) select o1.count(), o2.count()");
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic 24h 12h as o1, organic 12h today as o2 group by time(24b) select o1.count(), o2.count()");
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

        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic 24h 18h as o1, organic 6h today as o2 group by time(1h) select o1.count(), o2.count()");
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic 24h 18h as o1, organic 6h today as o2 group by time(24b) select o1.count(), o2.count()");
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

        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic 24h 12h as o1, organic 12h today as o2 group by time(1h relative) select o1.count(), o2.count()");
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

        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic 24h 18h as o1, organic 6h today as o2 group by time(1h relative) select o1.count(), o2.count()");
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


        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic 24h 17h as o1, organic 6h today as o2 group by time(1h relative) select o1.count(), o2.count()");
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

        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic 24h 16h as o1, organic 16h 8h as o2, organic 8h today as o3 group by time(1h relative) select o1.count(), o2.count(), o3.count()");
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

        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic 24h 13h as o1, organic 13h today as o2 group by time(1h relative) select o1.count(), o2.count()");
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

        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from organic 24h 11h as o1, organic 11h today as o2 group by time(1h relative) select o1.count(), o2.count()");
    }

    @Test
    public void testMonthRegroup() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("January 2015", "10", "10"));
        expected.add(ImmutableList.of("February 2015", "100", "200"));
        expected.add(ImmutableList.of("March 2015", "1", "3"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from multiMonth 2015-01-01 2015-04-01 group by time(1M) select count(), month");
    }

    @Test
    public void testMonthRegroup2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "January 2015", "10", "10"));
        expected.add(ImmutableList.of("1", "February 2015", "0", "0"));
        expected.add(ImmutableList.of("1", "March 2015", "0", "0"));
        expected.add(ImmutableList.of("2", "January 2015", "0", "0"));
        expected.add(ImmutableList.of("2", "February 2015", "100", "200"));
        expected.add(ImmutableList.of("2", "March 2015", "0", "0"));
        expected.add(ImmutableList.of("3", "January 2015", "0", "0"));
        expected.add(ImmutableList.of("3", "February 2015", "0", "0"));
        expected.add(ImmutableList.of("3", "March 2015", "1", "3"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from multiMonth 2015-01-01 2015-04-01 group by month, time(1M) select count(), month", true);
    }

    @Test
    public void testConsistentMonthRegroup() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-02-01 00:00:00)", "10", "10"));
        expected.add(ImmutableList.of("[2015-02-01 00:00:00, 2015-03-01 00:00:00)", "100", "200"));
        expected.add(ImmutableList.of("[2015-03-01 00:00:00, 2015-04-01 00:00:00)", "1", "3"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from multiMonth 2015-01-01 2015-04-01 group by time(1M) select count(), month OPTIONS [\"" + QueryOptions.Experimental.CONSISTENT_TIME_BUCKETS + "\"]");
    }

    @Test
    public void testConsistentMonthRegroupCustomFormat() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01, 2015-02-01)", "10", "10"));
        expected.add(ImmutableList.of("[2015-02-01, 2015-03-01)", "100", "200"));
        expected.add(ImmutableList.of("[2015-03-01, 2015-04-01)", "1", "3"));
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from multiMonth 2015-01-01 2015-04-01 group by time(1M, 'yyyy-MM-dd') select count(), month OPTIONS [\"" + QueryOptions.Experimental.CONSISTENT_TIME_BUCKETS + "\"]");
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
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from dayOfWeek 2015-01-01 2015-01-15 group by dayofweek select count(), day");
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
        QueryServletTestUtils.testIQL2(AllData.DATASET, expected, "from dayOfWeek 2015-01-01 2015-01-03 group by time(1d), dayofweek select count(), day");
    }
}
