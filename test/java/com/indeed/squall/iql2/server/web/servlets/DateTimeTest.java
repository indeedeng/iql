package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import org.junit.Test;

import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testIQL1;
import static com.indeed.squall.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class DateTimeTest extends BasicTest {
    @Test
    public void testWordDate() throws Exception {
        testIQL1(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "151", "2653", "306")), "from organic y t select count(), oji, ojc");
        testAll(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "153", "2655", "308")), "from organic 3days ago select count(), oji, ojc");
        testAll(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "1", "23", "1")), "from organic 60minute ago select count(), oji, ojc");
        testAll(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "151", "2653", "306")), "from organic d ago select count(), oji, ojc");
        testAll(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "1", "23", "1")), "from organic 60minute ago select count(), oji, ojc");
        testIQL1(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "1", "23", "1")), "from organic 60M ago select count(), oji, ojc");
        testIQL2(OrganicDataset.create(), ImmutableList.of(ImmutableList.of("", "182", "2684", "337")), "from organic 60M ago select count(), oji, ojc");
    }
}
