package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TimeZoneTest extends BasicTest {
    @Test
    public void testTimeZoneSelect() throws Exception {
        // data is available hourly from
        // 2015-01-01 to 2015-02-01 (exclusive), GMT-6

        final List<List<String>> expected = new ArrayList<>();
        expected.add(Lists.newArrayList("", "151"));
        final List<String> equivalentQueries = new ArrayList<>();

        equivalentQueries.add("FROM organic 2015-01-01 2015-01-02");
        equivalentQueries.add("TIMEZONE GMT\nFROM organic 2015-01-01 06:00 2015-01-02 06:00");
        equivalentQueries.add("TIMEZONE GMT+09:00\nFROM organic 2015-01-01 15:00 2015-01-02 15:00");
        equivalentQueries.add("TIMEZONE GMT+09\nFROM organic 2015-01-01 15:00 2015-01-02 15:00");
        equivalentQueries.add("TIMEZONE UTC-06\nFROM organic 2015-01-01 2015-01-02");
        equivalentQueries.add("TIMEZONE UTC+05:30\nFROM organic 2015-01-01 11:30 2015-01-02 11:30");

        for (final String query : equivalentQueries) {
            QueryServletTestUtils.testIQL2AndLegacy(expected, query);
        }
    }

    @Test
    public void testGroupByTimeTZ1() throws Exception {
        final List<List<String>> expected = new ArrayList<>();

        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60"));
        expected.add(ImmutableList.of("[2015-01-01 03:00:00, 2015-01-01 04:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 04:00:00, 2015-01-01 05:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 05:00:00, 2015-01-01 06:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 06:00:00, 2015-01-01 07:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 07:00:00, 2015-01-01 08:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 08:00:00, 2015-01-01 09:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 09:00:00, 2015-01-01 10:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 10:00:00, 2015-01-01 11:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 11:00:00, 2015-01-01 12:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 12:00:00, 2015-01-01 13:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 13:00:00, 2015-01-01 14:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 14:00:00, 2015-01-01 15:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 15:00:00, 2015-01-01 16:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 16:00:00, 2015-01-01 17:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 17:00:00, 2015-01-01 18:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 18:00:00, 2015-01-01 19:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 19:00:00, 2015-01-01 20:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 20:00:00, 2015-01-01 21:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 21:00:00, 2015-01-01 22:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 22:00:00, 2015-01-01 23:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 23:00:00, 2015-01-02 00:00:00)", "1"));

        QueryServletTestUtils.testIQL2AndLegacy(expected, "FROM organic 2015-01-01 2015-01-02 GROUP BY time(1h)");
        QueryServletTestUtils.testIQL2AndLegacy(expected, "TIMEZONE GMT-6\nFROM organic 2015-01-01 2015-01-02 GROUP BY time(1h)");
        QueryServletTestUtils.testIQL2AndLegacy(expected, "TIMEZONE UTC-6 FROM organic 2015-01-01 2015-01-02 GROUP BY time(1h)");
    }

    @Test
    public void testGroupByTimeTZ2() throws Exception {
        final List<List<String>> expected = new ArrayList<>();

        expected.add(ImmutableList.of("[2015-01-01 15:00:00, 2015-01-01 16:00:00)", "10"));
        expected.add(ImmutableList.of("[2015-01-01 16:00:00, 2015-01-01 17:00:00)", "60"));
        expected.add(ImmutableList.of("[2015-01-01 17:00:00, 2015-01-01 18:00:00)", "60"));
        expected.add(ImmutableList.of("[2015-01-01 18:00:00, 2015-01-01 19:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 19:00:00, 2015-01-01 20:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 20:00:00, 2015-01-01 21:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 21:00:00, 2015-01-01 22:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 22:00:00, 2015-01-01 23:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 23:00:00, 2015-01-02 00:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 00:00:00, 2015-01-02 01:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 01:00:00, 2015-01-02 02:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 02:00:00, 2015-01-02 03:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 03:00:00, 2015-01-02 04:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 04:00:00, 2015-01-02 05:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 05:00:00, 2015-01-02 06:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 06:00:00, 2015-01-02 07:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 07:00:00, 2015-01-02 08:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 08:00:00, 2015-01-02 09:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 09:00:00, 2015-01-02 10:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 10:00:00, 2015-01-02 11:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 11:00:00, 2015-01-02 12:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 12:00:00, 2015-01-02 13:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 13:00:00, 2015-01-02 14:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 14:00:00, 2015-01-02 15:00:00)", "1"));

        QueryServletTestUtils.testIQL2AndLegacy(expected, "TIMEZONE GMT+9\nFROM organic 2015-01-01 15:00 2015-01-02 15:00 GROUP BY time(1h)");
        QueryServletTestUtils.testIQL2AndLegacy(expected, "TIMEZONE UTC+9 FROM organic 2015-01-01 15:00 2015-01-02 15:00 GROUP BY time(1h)");
        QueryServletTestUtils.testIQL2AndLegacy(expected, "FROM organic 2015-01-01 15:00 2015-01-02 15:00 GROUP BY time(1h) TIMEZONE UTC+9");
    }

    @Test
    public void testGroupByTimeTZ3() throws Exception {
        final List<List<String>> expected = new ArrayList<>();

        expected.add(ImmutableList.of("[2015-01-01 11:30:00, 2015-01-01 12:30:00)", "10"));
        expected.add(ImmutableList.of("[2015-01-01 12:30:00, 2015-01-01 13:30:00)", "60"));
        expected.add(ImmutableList.of("[2015-01-01 13:30:00, 2015-01-01 14:30:00)", "60"));
        expected.add(ImmutableList.of("[2015-01-01 14:30:00, 2015-01-01 15:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 15:30:00, 2015-01-01 16:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 16:30:00, 2015-01-01 17:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 17:30:00, 2015-01-01 18:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 18:30:00, 2015-01-01 19:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 19:30:00, 2015-01-01 20:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 20:30:00, 2015-01-01 21:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 21:30:00, 2015-01-01 22:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 22:30:00, 2015-01-01 23:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 23:30:00, 2015-01-02 00:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 00:30:00, 2015-01-02 01:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 01:30:00, 2015-01-02 02:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 02:30:00, 2015-01-02 03:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 03:30:00, 2015-01-02 04:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 04:30:00, 2015-01-02 05:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 05:30:00, 2015-01-02 06:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 06:30:00, 2015-01-02 07:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 07:30:00, 2015-01-02 08:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 08:30:00, 2015-01-02 09:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 09:30:00, 2015-01-02 10:30:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-02 10:30:00, 2015-01-02 11:30:00)", "1"));

        QueryServletTestUtils.testIQL2AndLegacy(expected, "TIMEZONE GMT+05:30\nFROM organic 2015-01-01 11:30 2015-01-02 11:30 GROUP BY time(1h)");
        QueryServletTestUtils.testIQL2AndLegacy(expected, "TIMEZONE UTC+05:30 FROM organic 2015-01-01 11:30 2015-01-02 11:30 GROUP BY time(1h)");
        QueryServletTestUtils.testIQL2AndLegacy(expected, "FROM organic 2015-01-01 11:30 2015-01-02 11:30 GROUP BY time(1h) TIMEZONE UTC+05:30");
    }

    @Test
    public void testGroupByTimeRelative() throws Exception {
        final List<List<String>> expected = new ArrayList<>();

        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "0"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "0"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "0"));
        expected.add(ImmutableList.of("[2015-01-01 03:00:00, 2015-01-01 04:00:00)", "0"));
        expected.add(ImmutableList.of("[2015-01-01 04:00:00, 2015-01-01 05:00:00)", "0"));
        expected.add(ImmutableList.of("[2015-01-01 05:00:00, 2015-01-01 06:00:00)", "0"));
        expected.add(ImmutableList.of("[2015-01-01 06:00:00, 2015-01-01 07:00:00)", "0"));
        expected.add(ImmutableList.of("[2015-01-01 07:00:00, 2015-01-01 08:00:00)", "0"));
        expected.add(ImmutableList.of("[2015-01-01 08:00:00, 2015-01-01 09:00:00)", "0"));
        expected.add(ImmutableList.of("[2015-01-01 09:00:00, 2015-01-01 10:00:00)", "0"));
        expected.add(ImmutableList.of("[2015-01-01 10:00:00, 2015-01-01 11:00:00)", "0"));
        expected.add(ImmutableList.of("[2015-01-01 11:00:00, 2015-01-01 12:00:00)", "7"));
        expected.add(ImmutableList.of("[2015-01-01 12:00:00, 2015-01-01 13:00:00)", "33"));
        expected.add(ImmutableList.of("[2015-01-01 13:00:00, 2015-01-01 14:00:00)", "60"));
        expected.add(ImmutableList.of("[2015-01-01 14:00:00, 2015-01-01 15:00:00)", "31"));
        expected.add(ImmutableList.of("[2015-01-01 15:00:00, 2015-01-01 16:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 16:00:00, 2015-01-01 17:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 17:00:00, 2015-01-01 18:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 18:00:00, 2015-01-01 19:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 19:00:00, 2015-01-01 20:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 20:00:00, 2015-01-01 21:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 21:00:00, 2015-01-01 22:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 22:00:00, 2015-01-01 23:00:00)", "1"));
        expected.add(ImmutableList.of("[2015-01-01 23:00:00, 2015-01-02 00:00:00)", "1"));

        QueryServletTestUtils.testIQL2AndLegacy(expected, "TIMEZONE GMT+05:30\nFROM organic 2015-01-01 2015-01-02 GROUP BY time(1h)");
        QueryServletTestUtils.testIQL2AndLegacy(expected, "TIMEZONE UTC+05:30 FROM organic 2015-01-01 2015-01-02 GROUP BY time(1h)");
        QueryServletTestUtils.testIQL2AndLegacy(expected, "FROM organic 2015-01-01 2015-01-02 GROUP BY time(1h) TIMEZONE UTC+05:30");
    }

    @Test
    public void testExplodeDayOfWeek() throws Exception {
        final List<List<String>> expected = new ArrayList<>();

        expected.add(ImmutableList.of("Monday", "0"));
        expected.add(ImmutableList.of("Tuesday", "0"));
        expected.add(ImmutableList.of("Wednesday", "0"));
        expected.add(ImmutableList.of("Thursday", "136"));
        expected.add(ImmutableList.of("Friday", "15"));
        expected.add(ImmutableList.of("Saturday", "0"));
        expected.add(ImmutableList.of("Sunday", "0"));

        QueryServletTestUtils.testIQL2AndLegacy(expected, "TIMEZONE GMT+09:00\nFROM organic 2015-01-01 15:00 2015-01-02 15:00 GROUP BY DAYOFWEEK()");
    }
}
