package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class GroupByPostAggregateTest extends BasicTest {
    @Test
    public void groupByHavingDistinct() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk HAVING DISTINCT(oji) > 2");
    }

    @Test
    public void groupByHavingFieldMax() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("c", "4"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk[HAVING FIELD_MAX(oji) >= 100]");
    }

    @Test
    public void groupByMultiple() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("c", "10", "3"));
        expected.add(ImmutableList.of("c", "1000", "1"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by tk, oji HAVING PARENT(FIELD_MAX(oji)) > 100");
    }
}
