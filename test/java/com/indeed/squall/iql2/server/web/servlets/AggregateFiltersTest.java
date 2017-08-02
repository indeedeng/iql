package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AggregateFiltersTest {
    @Test
    public void testMetricIs() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("c", "4"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() = 4 select count()");
    }

    @Test
    public void testMetricIsnt() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() != 4 select count()");
    }

    @Test
    public void testMetricGte() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("c", "4"));
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() >= 4 select count()");
    }

    @Test
    public void testMetricGt() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() > 4 select count()");
    }

    @Test
    public void testMetricLt() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "2"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() < 4 select count()");
    }

    @Test
    public void testMetricLte() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "2"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() <= 2 select count()");
    }

    @Test
    public void testAnd() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("c", "4"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() <= 4 AND count() >= 4 select count()");
    }

    @Test
    public void testOr() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having count() < 4 OR count() > 4 select count()");
    }

    @Test
    public void testAlways() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("a", "4"));
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("c", "4"));
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having true select count()");
    }

    @Test
    public void testNever() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk having false select count()");
    }
}
