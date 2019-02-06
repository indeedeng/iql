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
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL1;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class DistinctTest extends BasicTest {

    @Test
    public void basicDistinct() throws Exception {
        testAll(ImmutableList.of(ImmutableList.of("", "4")), "from distinct yesterday 2015-01-10 select distinct(tk)");
    }

    @Test
    public void distinctHaving() throws Exception {
        testIQL2(ImmutableList.of(ImmutableList.of("", "3")), "from organic yesterday today select distinct(tk having count() > 2)");
        testIQL2(ImmutableList.of(ImmutableList.of("", "1")), "from organic yesterday today select distinct(tk having count() < 3)");
        testIQL2(ImmutableList.of(ImmutableList.of("", "4", "3", "1")), "from organic yesterday today select distinct(tk), distinct(tk having count() > 2), distinct(tk having count() < 3)");
    }

    @Test
    public void timeDistinct() throws Exception {
        final String query = "from distinct yesterday 2015-01-10 group by time(1d) select distinct(tk)";
        List<List<String>> expected = Lists.newArrayList(
                ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "0"),
                ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "1"),
                ImmutableList.of("[2015-01-03 00:00:00, 2015-01-04 00:00:00)", "4"),
                ImmutableList.of("[2015-01-04 00:00:00, 2015-01-05 00:00:00)", "4"),
                ImmutableList.of("[2015-01-05 00:00:00, 2015-01-06 00:00:00)", "4"),
                ImmutableList.of("[2015-01-06 00:00:00, 2015-01-07 00:00:00)", "3"),
                ImmutableList.of("[2015-01-07 00:00:00, 2015-01-08 00:00:00)", "2"));
        // IQL1 filters out groups with zero results.
        testIQL1(expected, query);
        expected.add(ImmutableList.of("[2015-01-08 00:00:00, 2015-01-09 00:00:00)", "0"));
        expected.add(ImmutableList.of("[2015-01-09 00:00:00, 2015-01-10 00:00:00)", "0"));
        testIQL2(expected, query);
    }

    @Test
    public void timeDistinctWindow() throws Exception {
        testIQL2(ImmutableList.of(
                ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "0"),
                ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "1"),
                ImmutableList.of("[2015-01-03 00:00:00, 2015-01-04 00:00:00)", "4"),
                ImmutableList.of("[2015-01-04 00:00:00, 2015-01-05 00:00:00)", "4"),
                ImmutableList.of("[2015-01-05 00:00:00, 2015-01-06 00:00:00)", "4"),
                ImmutableList.of("[2015-01-06 00:00:00, 2015-01-07 00:00:00)", "4"),
                ImmutableList.of("[2015-01-07 00:00:00, 2015-01-08 00:00:00)", "3"),
                ImmutableList.of("[2015-01-08 00:00:00, 2015-01-09 00:00:00)", "2"),
                ImmutableList.of("[2015-01-09 00:00:00, 2015-01-10 00:00:00)", "0")
        ), "from distinct yesterday 2015-01-10 group by time(1d) select distinct_window(2, tk)");
    }

    @Test
    public void timeDistinctWindow2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "0"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "1"));
        for (int i = 3; i < 30; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-%02d 00:00:00, 2015-01-%02d 00:00:00)", i, i + 1), "4"));
        }
        testIQL2(expected, "from distinct yesterday 2015-01-30 group by time(1d) select distinct_window(30, tk)");
    }

    @Test
    // Not guaranteed to continue to test what we want, but will do the job for the IQL-682 fix
    public void testPopAfterDistinct() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "4", "4"));
        testIQL2(expected, "from distinct yesterday 2015-01-30 select distinct(tk having count() > 1), tk=\"a\"");
    }

    @Test
    public void testDistinctWindowHaving() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "6", "6", "0", "0"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "5", "8", "3", "3"));
        expected.add(ImmutableList.of("[2015-01-03 00:00:00, 2015-01-04 00:00:00)", "4", "6", "3", "3"));
        expected.add(ImmutableList.of("[2015-01-04 00:00:00, 2015-01-05 00:00:00)", "3", "5", "2", "2"));
        expected.add(ImmutableList.of("[2015-01-05 00:00:00, 2015-01-06 00:00:00)", "4", "5", "2", "2"));
        expected.add(ImmutableList.of("[2015-01-06 00:00:00, 2015-01-07 00:00:00)", "3", "5", "2", "2"));
        expected.add(ImmutableList.of("[2015-01-07 00:00:00, 2015-01-08 00:00:00)", "5", "6", "2", "2"));
        testIQL2(
                expected,
                "from snapshot 2015-01-01 2015-01-08 " +
                    "group by time(1d) " +
                    "select distinct(id), distinct_window(2, id), distinct_window(2, id having count() > 1), distinct_window(2, id having count() = 2)",
                true
        );
    }

    @Test
    public void testDistinctWindowHavingAfterGroupBy() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "3", "3", "0", "0"));
        expected.add(ImmutableList.of("0", "[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "4", "5", "2", "2"));
        expected.add(ImmutableList.of("0", "[2015-01-03 00:00:00, 2015-01-04 00:00:00)", "3", "4", "3", "3"));
        expected.add(ImmutableList.of("0", "[2015-01-04 00:00:00, 2015-01-05 00:00:00)", "2", "3", "2", "2"));
        expected.add(ImmutableList.of("0", "[2015-01-05 00:00:00, 2015-01-06 00:00:00)", "3", "3", "2", "2"));
        expected.add(ImmutableList.of("0", "[2015-01-06 00:00:00, 2015-01-07 00:00:00)", "2", "3", "2", "2"));
        expected.add(ImmutableList.of("0", "[2015-01-07 00:00:00, 2015-01-08 00:00:00)", "3", "3", "2", "2"));
        expected.add(ImmutableList.of("1", "[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "3", "3", "0", "0"));
        expected.add(ImmutableList.of("1", "[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "1", "3", "1", "1"));
        expected.add(ImmutableList.of("1", "[2015-01-03 00:00:00, 2015-01-04 00:00:00)", "1", "2", "0", "0"));
        expected.add(ImmutableList.of("1", "[2015-01-04 00:00:00, 2015-01-05 00:00:00)", "1", "2", "0", "0"));
        expected.add(ImmutableList.of("1", "[2015-01-05 00:00:00, 2015-01-06 00:00:00)", "1", "2", "0", "0"));
        expected.add(ImmutableList.of("1", "[2015-01-06 00:00:00, 2015-01-07 00:00:00)", "1", "2", "0", "0"));
        expected.add(ImmutableList.of("1", "[2015-01-07 00:00:00, 2015-01-08 00:00:00)", "2", "3", "0", "0"));
        testIQL2(
                expected,
                "from snapshot 2015-01-01 2015-01-08 " +
                    "group by id >= 5, time(1d) " +
                    "select distinct(id), distinct_window(2, id), distinct_window(2, id having count() > 1), distinct_window(2, id having count() = 2)",
                true
        );
    }

    @Test
    public void testDistinctWindowHavingAfterGroupBy2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "6", "6", "0", "0"));
        expected.add(ImmutableList.of("0", "[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "5", "8", "3", "3"));
        expected.add(ImmutableList.of("0", "[2015-01-03 00:00:00, 2015-01-04 00:00:00)", "4", "6", "3", "3"));
        expected.add(ImmutableList.of("0", "[2015-01-04 00:00:00, 2015-01-05 00:00:00)", "0", "4", "0", "0"));
        expected.add(ImmutableList.of("0", "[2015-01-05 00:00:00, 2015-01-06 00:00:00)", "0", "0", "0", "0"));
        expected.add(ImmutableList.of("0", "[2015-01-06 00:00:00, 2015-01-07 00:00:00)", "0", "0", "0", "0"));
        expected.add(ImmutableList.of("0", "[2015-01-07 00:00:00, 2015-01-08 00:00:00)", "0", "0", "0", "0"));
        expected.add(ImmutableList.of("1", "[2015-01-01 00:00:00, 2015-01-02 00:00:00)", "0", "0", "0", "0"));
        expected.add(ImmutableList.of("1", "[2015-01-02 00:00:00, 2015-01-03 00:00:00)", "0", "0", "0", "0"));
        expected.add(ImmutableList.of("1", "[2015-01-03 00:00:00, 2015-01-04 00:00:00)", "0", "0", "0", "0"));
        expected.add(ImmutableList.of("1", "[2015-01-04 00:00:00, 2015-01-05 00:00:00)", "3", "3", "0", "0"));
        expected.add(ImmutableList.of("1", "[2015-01-05 00:00:00, 2015-01-06 00:00:00)", "4", "5", "2", "2"));
        expected.add(ImmutableList.of("1", "[2015-01-06 00:00:00, 2015-01-07 00:00:00)", "3", "5", "2", "2"));
        expected.add(ImmutableList.of("1", "[2015-01-07 00:00:00, 2015-01-08 00:00:00)", "5", "6", "2", "2"));

        final long jan4Seconds = new DateTime(2015, 1, 4, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis() / 1000;

        testIQL2(
                expected,
                "from snapshot 2015-01-01 2015-01-08 " +
                    "group by unixtime >= " + jan4Seconds + ", time(1d) " +
                    "select distinct(id), distinct_window(2, id), distinct_window(2, id having count() > 1), distinct_window(2, id having count() = 2)",
                true
        );
    }
}
