package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PredicateRegroupTest {
    @Test
    public void singleDataset() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2"));
        expected.add(ImmutableList.of("1", "149"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic yesterday today group by oji < 100 select count()");
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), QueryServletTestUtils.addConstantColumn(1, "1", expected), "from organic yesterday today group by oji < 100, (true) select count()");
    }

    @Test
    public void dualDataset() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("0", "2"));
        expected.add(ImmutableList.of("1", "149"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected, "from organic 24h 12h as o1, organic 12h 0h as o2 group by oji < 100 select count()");
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), QueryServletTestUtils.addConstantColumn(1, "1", expected), "from organic 24h 12h as o1, organic 12h 0h as o2  group by oji < 100, (true) select count()");
    }
}
