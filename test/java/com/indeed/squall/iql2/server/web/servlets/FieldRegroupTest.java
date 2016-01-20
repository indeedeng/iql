package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FieldRegroupTest {
    @Test
    public void testBasicGroupBy() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2", "0"));
        expected.add(ImmutableList.of("1", "84", "84"));
        expected.add(ImmutableList.of("2", "1", "2"));
        expected.add(ImmutableList.of("3", "60", "180"));
        expected.add(ImmutableList.of("5", "1", "5"));
        expected.add(ImmutableList.of("10", "2", "20"));
        expected.add(ImmutableList.of("15", "1", "15"));
        QueryServletTestUtils.testAll(expected, "from organic yesterday today group by ojc select count(), ojc");
    }

    @Test
    public void testFirstK() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2", "0"));
        expected.add(ImmutableList.of("1", "84", "84"));
        expected.add(ImmutableList.of("2", "1", "2"));
        QueryServletTestUtils.testAll(expected, "from organic yesterday today group by ojc select count(), ojc LIMIT 3");
    }

    @Test
    public void testImplicitOrdering() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("1", "84", "84"));
        expected.add(ImmutableList.of("3", "60", "180"));
        // TODO: Introduce fully deterministic ordering for ties and increase to top 3?
//        expected.add(ImmutableList.of("0", "2", "0"));
        QueryServletTestUtils.testAll(expected, "from organic yesterday today group by ojc[2] select count(), ojc");
    }

    @Test
    public void testTopKOrdering() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("15", "1", "15"));
        expected.add(ImmutableList.of("10", "2", "20"));
        expected.add(ImmutableList.of("5", "1", "5"));
        expected.add(ImmutableList.of("3", "60", "180"));
        expected.add(ImmutableList.of("2", "1", "2"));
        expected.add(ImmutableList.of("1", "84", "84"));
        expected.add(ImmutableList.of("0", "2", "0"));
        QueryServletTestUtils.testAll(expected, "from organic yesterday today group by ojc[100 BY ojc/count()] select count(), ojc");
    }

    @Test
    public void testOrderingOnly() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("15", "1", "15"));
        expected.add(ImmutableList.of("10", "2", "20"));
        expected.add(ImmutableList.of("5", "1", "5"));
        expected.add(ImmutableList.of("3", "60", "180"));
        expected.add(ImmutableList.of("2", "1", "2"));
        expected.add(ImmutableList.of("1", "84", "84"));
        expected.add(ImmutableList.of("0", "2", "0"));
        QueryServletTestUtils.testAll(expected, "from organic yesterday today group by ojc[BY ojc/count()] select count(), ojc");
    }
}
