package com.indeed.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class DocMetricsTest extends BasicTest {

    @Test
    public void testMetrics() throws Exception {
        QueryServletTestUtils.testOriginalIQL1(
                ImmutableList.of(ImmutableList.of("", "499500", "324572", "999000")),
                "from big yesterday today where field < 1000 select cached(field), mulshr(10, field, field), shldiv(10, field, 512)", true);

        QueryServletTestUtils.testIQL1(
                ImmutableList.of(ImmutableList.of("", "499500", "250000", "40315004", "5366", "1", "0", "1000")),
                "from big yesterday today where field < 1000 select field, abs(field-500), " +
                        "exp(field, 128), log(field+1), " +
                        "hasint(\"field:100\"), hasstrfield(field), hasintfield(field)", true);
    }

    @Test
    public void testCached() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "499500", "499500"));
        // supported only in IQL1
        QueryServletTestUtils.testOriginalIQL1(expected, "from big yesterday today where field < 1000 select field, cached(field)");
    }

    @Test
    public void testMulShr() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "499500", "324572"));
        // supported only in IQL1
        QueryServletTestUtils.testOriginalIQL1(expected, "from big yesterday today where field < 1000 select field, mulshr(10, field, field)");
    }

    @Test
    public void testShlDiv() throws Exception {
        final List<List<String>> expected = new ArrayList<>();
        expected.add(ImmutableList.of("", "499500", "999000"));
        // supported only in IQL1
        QueryServletTestUtils.testOriginalIQL1(expected, "from big yesterday today where field < 1000 select field, shldiv(10, field, 512)");
    }

    @Test
    public void testHasStr() throws Exception {
        QueryServletTestUtils.testIQL1(
                ImmutableList.of(ImmutableList.of("", "151", "151", "0")),
                "from organic yesterday today select hasstr(country, \"US\"), hasstr(country, US), hasstr(country, 1)"
        );
        QueryServletTestUtils.testIQL1(
                ImmutableList.of(ImmutableList.of("", "50")),
                "from stringAsInt1 yesterday today select hasstr(page, 1)"
        );
    }
}
