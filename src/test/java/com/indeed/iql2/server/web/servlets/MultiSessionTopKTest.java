package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jwolfe
 */

public class MultiSessionTopKTest extends BasicTest {
    // This test exists because there was a funky bug (IQL-622).
    // Probably not worth trying to understand its reason to be.
    @Test
    public void testIt() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("d", "141", "141"));
        expected.add(ImmutableList.of("a", "4", "4"));
        QueryServletTestUtils.testIQL2(
                expected,
                "from organic yesterday today as FOO, organic group by tk[2 by FOO.count()] select FOO.count(), organic.count()",
                true
        );
    }
}
