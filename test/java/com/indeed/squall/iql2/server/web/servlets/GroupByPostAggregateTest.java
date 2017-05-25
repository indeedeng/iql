package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class GroupByPostAggregateTest extends BasicTest {
    final Dataset dataset = OrganicDataset.create();

    @Test
    public void groupByFieldHavingDistinct() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("c", "4"));
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(dataset, expected, "from organic yesterday today group by tk HAVING DISTINCT(oji) > 1", false);
    }

    @Test
    public void groupByFieldHavingMultipleConditions() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("d", "141"));
        QueryServletTestUtils.testIQL2(dataset, expected, "from organic yesterday today group by tk HAVING DISTINCT(oji) > 1 AND COUNT() > 100", false);
    }

    @Test
    public void groupByTimeHavingDistinct() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10"));
        QueryServletTestUtils.testIQL2(dataset, expected, "from organic yesterday today group by time(1h) HAVING DISTINCT(oji) > 1", false);
    }

    @Test
    public void groupByHavingFieldMax() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("b", "2"));
        expected.add(ImmutableList.of("c", "4"));
        QueryServletTestUtils.testIQL2(dataset, expected, "from organic yesterday today group by tk[HAVING FIELD_MAX(oji) >= 100]", false);
    }

    @Test
    public void groupByMultipleField() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("c", "10", "3"));
        expected.add(ImmutableList.of("c", "1000", "1"));
        QueryServletTestUtils.testIQL2(dataset, expected, "from organic yesterday today group by tk, oji HAVING PARENT(FIELD_MAX(oji)) > 100", false);
    }
}
