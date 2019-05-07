package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jwolfe
 */

public class TopKLimitTest extends BasicTest {
    @Test
    public void testIt() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("d", "141", "141"));
        expected.add(ImmutableList.of("a", "4", "4"));
        QueryServletTestUtils.expectException(
                "from organic yesterday today group by country[0]",
                QueryServletTestUtils.LanguageVersion.IQL2,
                x -> x.contains("The K in Top K must be in [0, INT_MAX). Value was: 0")
        );
        QueryServletTestUtils.expectException(
                "from organic yesterday today group by country[4294967297]",
                QueryServletTestUtils.LanguageVersion.IQL2,
                x -> x.contains("The K in Top K must be in [0, INT_MAX). Value was: 4294967297")
        );
        QueryServletTestUtils.expectException(
                "from organic yesterday today group by country[" + Integer.MAX_VALUE + "]",
                QueryServletTestUtils.LanguageVersion.IQL2,
                x -> x.contains("The K in Top K must be in [0, INT_MAX). Value was: " + Integer.MAX_VALUE)
        );
    }
}
