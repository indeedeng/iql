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
    public void testMetricsIntTermsWithStringField() throws Exception {
        QueryServletTestUtils.testAll(
                ImmutableList.of(ImmutableList.of("", "10", "10", "0", "90")),
                "from stringAsInt1 yesterday today select leadingZeroes = \"0001\", leadingZeroes=0001, leadingZeroes=1, leadingZeroes != 0002"
        );
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
        QueryServletTestUtils.testIQL1(
                ImmutableList.of(ImmutableList.of("", "1", "1", "1", "1", "1")),
                "from keywords yesterday today select hasstr(from, from), hasstr(where, where), hasstr(group, group), hasstr(by, by), hasstr(limit, limit)"
        );
    }

    @Test
    public void testOperatorPrecedence() throws Exception {
        QueryServletTestUtils.testIQL1(
                ImmutableList.of(ImmutableList.of("", "129", "306", "3", "3", "435")),
                // checking that 'ojc + oji = 10' is treated as '(ojc + oji) = 10' not as 'ojc + (oji = 10)'
                "from organic yesterday today select oji = 10, ojc, ojc + oji = 10, (ojc + oji) = 10, ojc + (oji = 10)"
        );

        QueryServletTestUtils.testIQL1(
                ImmutableList.of(ImmutableList.of("", "22", "306", "148", "148", "328")),
                // same check for '!='
                "from organic yesterday today select oji != 10, ojc, ojc + oji != 10, (ojc + oji) != 10, ojc + (oji != 10)"
        );

        QueryServletTestUtils.testIQL2(
                ImmutableList.of(ImmutableList.of("", "129", "306", "435", "0", "435")),
                // In IQL2 'ojc + oji = 10' is treated as 'ojc + (oji = 10)', maybe error with operator precedence, ticket IQL-868
                // and '(ojc + oji) = 10' cannot be parsed as AggregateMetric at all, so change to M() for no reason
                "from organic yesterday today select oji = 10, ojc, ojc + oji = 10, M((ojc + oji) = 10), ojc + (oji = 10)"
        );

        QueryServletTestUtils.testIQL2(
                ImmutableList.of(ImmutableList.of("", "22", "306", "328", "1", "328")),
                // same check for '!='
                "from organic yesterday today select oji != 10, ojc, ojc + oji != 10, M((ojc + oji) != 10), ojc + (oji != 10)"
        );
    }

    @Test
    public void testFiltersIntTermsWithStringField() throws Exception {
        // in DocFilter
        QueryServletTestUtils.testAll(
                ImmutableList.of(ImmutableList.of("", "10")),
                "from stringAsInt1 yesterday today where leadingZeroes=0001"
        );

        // in DocFilter
        QueryServletTestUtils.testAll(
                ImmutableList.of(ImmutableList.of("", "90")),
                "from stringAsInt1 yesterday today where leadingZeroes!=0001"
        );

        // lucene style in DocFilter, not supported in IQL2
        QueryServletTestUtils.testIQL1(
                ImmutableList.of(ImmutableList.of("", "10")),
                "from stringAsInt1 yesterday today where leadingZeroes:0001"
        );

        // in terms list
        QueryServletTestUtils.testAll(
                ImmutableList.of(ImmutableList.of("", "30")),
                "from stringAsInt1 yesterday today where leadingZeroes in (0001, 0003, 0005)"
        );

        // terms list in group by
        QueryServletTestUtils.testAll(
                ImmutableList.of(
                        ImmutableList.of("0001", "10"),
                        ImmutableList.of("0003", "10"),
                        ImmutableList.of("0005", "10")),
                "from stringAsInt1 yesterday today group by leadingZeroes in (0001, 0003, 0005)"
        );

        // terms list from subquery
        QueryServletTestUtils.testIQL2(
                ImmutableList.of(ImmutableList.of("", "30")),
                "from stringAsInt1 yesterday today where leadingZeroes in (" +
                        "from stringAsInt1 yesterday today group by leadingZeroes in (0001, 0003, 0005) )"
        );
    }
}
