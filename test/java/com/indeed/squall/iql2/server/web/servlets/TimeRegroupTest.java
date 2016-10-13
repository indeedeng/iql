package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.withoutLastColumn;

public class TimeRegroupTest extends BasicTest {
    @Test
    public void testTimeRegroup() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("[2015-01-01 00:00:00, 2015-01-01 01:00:00)", "10", "1180", "45", "3"));
        expected.add(ImmutableList.of("[2015-01-01 01:00:00, 2015-01-01 02:00:00)", "60", "600", "60", "1"));
        expected.add(ImmutableList.of("[2015-01-01 02:00:00, 2015-01-01 03:00:00)", "60", "600", "180", "1"));
        for (int i = 3; i < 23; i++) {
            expected.add(ImmutableList.of(String.format("[2015-01-01 %02d:00:00, 2015-01-01 %02d:00:00)", i, i + 1), "1", String.valueOf(i), "1", "1"));
        }
        expected.add(ImmutableList.of("[2015-01-01 23:00:00, 2015-01-02 00:00:00)", "1", "23", "1", "1"));

        testAll(OrganicDataset.create(), expected, "from organic yesterday today group by time(1h) select count(), oji, ojc, distinct(tk)");
        // Remove DISTINCT to allow streaming, rather than regroup.
        testAll(OrganicDataset.create(), withoutLastColumn(expected), "from organic yesterday today group by time(1h) select count(), oji, ojc");
    }
}
