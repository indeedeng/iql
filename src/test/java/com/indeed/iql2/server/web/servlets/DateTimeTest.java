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
import org.junit.Test;

import java.util.List;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL1;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class DateTimeTest extends BasicTest {
    @Test
    public void testWordDate() throws Exception {
        testAll(ImmutableList.of(ImmutableList.of("", "151", "2653", "306")), "from organic y today select count(), oji, ojc");
        testAll(ImmutableList.of(ImmutableList.of("", "153", "2655", "308")), "from organic 3days ago today select count(), oji, ojc");
        testAll(ImmutableList.of(ImmutableList.of("", "1", "23", "1")), "from organic 60minute ago today select count(), oji, ojc");
        testAll(ImmutableList.of(ImmutableList.of("", "151", "2653", "306")), "from organic d ago today select count(), oji, ojc");
        testAll(ImmutableList.of(ImmutableList.of("", "1", "23", "1")), "from organic 60minute ago today select count(), oji, ojc");
        testIQL1(ImmutableList.of(ImmutableList.of("", "1", "23", "1")), "from organic 60M ago today select count(), oji, ojc");
        testIQL2(ImmutableList.of(ImmutableList.of("", "182", "2684", "337")), "from organic 60M ago today select count(), oji, ojc");
    }

    @Test
    public void testQuotes() throws Exception {
        testAll(ImmutableList.of(ImmutableList.of("", "151", "2653", "306")), "from organic \"d\" \"today\" select count(), oji, ojc");
        testAll(ImmutableList.of(ImmutableList.of("", "151", "2653", "306")), "from organic \"1d\" today select count(), oji, ojc");
        testAll(ImmutableList.of(ImmutableList.of("", "160", "2662", "315")), "from organic \"10d\" \"today\" select count(), oji, ojc");
    }

    @Test
    public void testSingleDigitDates() throws  Exception {
        testAll(ImmutableList.of(ImmutableList.of("","10")), "from organic 2015-1-1T0:0:0 2015-1-1 01:00:00 select count()");
        testAll(ImmutableList.of(ImmutableList.of("","60","600","60")), "from organic 2015-1-01 1:0:0 2015-01-1T2:00:0 select count(), oji, ojc");
        testAll(ImmutableList.of(ImmutableList.of("","19","190","57")), "from organic 2015-1-01 2:1:0 2015-1-01 2:19:4 select count(), oji, ojc");
        testAll(ImmutableList.of(ImmutableList.of("","19","190","57")), "from organic 2015-1-01T2:1:0 2015-1-01 2:19:4 select count(), oji, ojc");
    }

    @Test
    public void testUnixTimeStamp() throws  Exception {
        final List<List<String>> expected = ImmutableList.of(ImmutableList.of("","151"));
        // All 3 queries have the same time range (words, milliseconds, seconds)
        testAll(expected, "from organic yesterday today select count()");
        testAll(expected, "from organic 1420092000000 1420178400000 select count()");
        testAll(expected, "from organic 1420092000 1420178400 select count()");
    }

    @Test
    public void testDateToken() throws Exception {
        // some queries to test corner cases
        testAll(ImmutableList.of(ImmutableList.of("", "182", "2684", "337")), "from organic 2014-1 today select count(), oji, ojc");
        testAll(ImmutableList.of(ImmutableList.of("", "182", "2684", "337")), "from organic 2014 today select count(), oji, ojc");
        testAll(ImmutableList.of(ImmutableList.of("[2014-12-28 00:00:00, 2014-12-29 00:00:00)", "1", "1", "1")),
                "from organic \"5 days ago\" \"4 days\" group by time(\"1b\") select count(), oji, ojc");
    }
}
