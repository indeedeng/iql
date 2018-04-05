package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Test;

import java.util.List;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.withoutLastColumn;

public class BasicSelectTest extends BasicTest {
    @Test
    public void testUngrouped() throws Exception {
        final List<List<String>> expected = ImmutableList.<List<String>>of(ImmutableList.of("", "151", "2653", "306", "4"));
        testAll(OrganicDataset.create(), expected, "from organic yesterday today select count(), oji, ojc, distinct(tk)");
        // Remove DISTINCT to allow streaming, rather than regroup.
        testAll(OrganicDataset.create(), withoutLastColumn(expected), "from organic yesterday today select count(), oji, ojc");
    }
}
