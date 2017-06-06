package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testIQL1;

public class DateTimeTest extends BasicTest {
    @Test
    public void testWordDate() throws Exception {
        testIQL1(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "151", "2653", "306")), "from organic y t select count(), oji, ojc");
        testIQL1(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "151", "2653", "306")), "from organic 3days ago select count(), oji, ojc");
    }
}
