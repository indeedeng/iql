package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ParentDistinctHavingSumTest extends BasicTest {
    @Test
    public void testParentDistinctHavingSum() throws Exception {
        QueryServletTestUtils.testIQL2(ImmutableList.of(ImmutableList.of("", "3")), "from organic 1d 0d select distinct(tk having oji > 50)");
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("US", "3"));
        QueryServletTestUtils.testIQL2(expected, "from organic 1d 0d group by country select parent(distinct(tk having oji > 50))");
    }
}
