package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.squall.iql2.server.web.servlets.dataset.Dataset;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Test;

import java.util.List;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class IQL339Test extends BasicTest {
    @Test
    public void testBasicFilters() throws Exception {
        final Dataset dataset = OrganicDataset.create();
        testAll(dataset, ImmutableList.<List<String>>of(), "from organic yesterday today where oji=-1 group by oji");
        testAll(dataset, ImmutableList.<List<String>>of(), "from organic yesterday today where oji=-1 group by oji, oji");
        testAll(dataset, ImmutableList.<List<String>>of(), "from organic yesterday today where oji=-1 group by oji, oji, oji");
        testIQL2(dataset, ImmutableList.<List<String>>of(), "from organic(oji=-1) yesterday today group by oji, oji, oji");
    }
}
