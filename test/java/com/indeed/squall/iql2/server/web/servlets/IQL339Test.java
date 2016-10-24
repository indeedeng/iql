package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class IQL339Test extends BasicTest {
    @Test
    public void testBasicFilters() throws Exception {
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(), "from organic yesterday today where oji=-1 group by oji");
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(), "from organic yesterday today where oji=-1 group by oji, oji");
        testAll(OrganicDataset.create(), ImmutableList.<List<String>>of(), "from organic yesterday today where oji=-1 group by oji, oji, oji");
        testIQL2(OrganicDataset.create(), ImmutableList.<List<String>>of(), "from organic(oji=-1) yesterday today group by oji, oji, oji");
    }
}
