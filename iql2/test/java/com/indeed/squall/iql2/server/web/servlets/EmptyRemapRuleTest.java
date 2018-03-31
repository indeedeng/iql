package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class EmptyRemapRuleTest extends BasicTest {
    @Test
    public void basicTest() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("DEFAULT", "151"));
        QueryServletTestUtils.testIQL2(OrganicDataset.create(), expected,
                "from organic yesterday today group by oji[having count() > 200] with default", true);
    }

}
