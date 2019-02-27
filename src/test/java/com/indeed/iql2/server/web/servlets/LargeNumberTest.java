package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testIQL2;

public class LargeNumberTest extends BasicTest {
    @Test
    public void testLargeNumber() throws Exception {
        // 16 digits of precision
        final ImmutableList<List<String>> expected = ImmutableList.of(ImmutableList.of("", "36893488147419103000"));
        testIQL2(expected, "from organic yesterday today select 2 ^ 65");
    }
}
