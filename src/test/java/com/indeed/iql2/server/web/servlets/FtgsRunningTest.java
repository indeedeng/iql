package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FtgsRunningTest extends BasicTest {
    // PERCENTILE doesn't support having
    // FIELD_MIN doesn't support having
    // FIELD_MAX doesn't support having
    // This leaves just DISTINCT and SUM_OVER.

    @Test
    // Preconditions of the following tests
    public void validateNormalDistinct() throws Exception {
        QueryServletTestUtils.testIQL2(
                ImmutableList.of(ImmutableList.of("", "1", "4", "24", "7")),
                "from organic 2d 0d select distinct(country), distinct(tk), distinct(oji), distinct(ojc)",
                true
        );
    }

    @Test
    public void testOverallDistinctRunning() throws Exception {
        QueryServletTestUtils.testIQL2(
                ImmutableList.of(ImmutableList.of("", "0", "0", "14", "0")),
                "from organic 2d 0d " +
                        "select distinct(country having running(1) > 10), " +
                        "distinct(tk having running(1) > 10), " +
                        "distinct(oji having running(1) > 10), " +
                        "distinct(ojc having running(1) > 10)",
                true
        );
    }

    @Test
    public void testPerTermDistinctRunning() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "1", "0"));
        expected.add(ImmutableList.of("b", "2", "1"));
        expected.add(ImmutableList.of("c", "2", "1"));
        expected.add(ImmutableList.of("d", "22", "21"));
        QueryServletTestUtils.testIQL2(
                expected,
                "from organic 2d 0d " +
                        "group by tk " +
                        "select distinct(oji), " +
                        "distinct(oji having running(1) > 1)",
                true
        );
    }

    @Test
    public void testSumAcrossRunning() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "1", "0"));
        expected.add(ImmutableList.of("b", "2", "1"));
        expected.add(ImmutableList.of("c", "2", "1"));
        expected.add(ImmutableList.of("d", "22", "21"));
        QueryServletTestUtils.testIQL2(
                expected,
                "from organic 2d 0d " +
                        "group by tk " +
                        "select sum_over(oji, 1), " +
                        "sum_over(oji having running(1) > 1, 1)",
                true
        );
    }
}
