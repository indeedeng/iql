package com.indeed.iql2.server.web.servlets;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.indeed.iql2.server.web.servlets.QueryServletTestUtils.LanguageVersion;
import org.junit.Test;

import java.util.List;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HeadersTest extends BasicTest {

    private static void checkTotals(
            final String query,
            final String expectedTotals) throws Exception {
        checkTotals(query, expectedTotals, LanguageVersion.ORIGINAL_IQL1);
        checkTotals(query, expectedTotals, LanguageVersion.IQL1_LEGACY_MODE);
    }

    private static void checkTotals(
            final String query,
            final String expectedTotals,
            final LanguageVersion version) throws Exception {
        final JsonNode header = QueryServletTestUtils.getQueryHeader(query, version);
        final JsonNode totals = header.get("IQL-Totals");
        if (totals == null) {
            fail();
        }
        final String quotedValue = totals.toString();
        assertEquals(quotedValue, "\"" + expectedTotals + "\"");
    }

    @Test
    public void testStatsTotals() throws Exception {
        // Without regroup
        final List<List<String>> totals = ImmutableList.of(ImmutableList.of("", "2653"));
        // Check stats.
        testAll(totals, "from organic yesterday today select oji");
        // Check that without regroup totals are the same as stats.
        checkTotals("from organic yesterday today select oji", "[2653.0]");
        // Check that with regroup totals are the same as before regroup...
        checkTotals("from organic yesterday today group by tk select oji", "[2653.0]");
        // ... even if some docs are filtered out
        checkTotals("from organic yesterday today group by tk in (\"a\", \"b\") select oji", "[2653.0]");
    }


    @Test
    public void testStatsTotalsPercentile() throws Exception {
        // In case of percentile or distinct IQL1 does not put it as totals.
        // Legacy mode returns all stats.

        // percentile
        final List<List<String>> percentile = ImmutableList.of(ImmutableList.of("", "10", "2653"));
        testAll(percentile, "from organic yesterday today select percentile(oji, 50), oji", true);

        checkTotals("from organic yesterday today select percentile(oji, 50)", "[]", LanguageVersion.ORIGINAL_IQL1);
        checkTotals("from organic yesterday today group by tk select percentile(oji, 50)", "[]", LanguageVersion.ORIGINAL_IQL1);
        checkTotals("from organic yesterday today group by tk in (\"a\", \"b\") select percentile(oji, 50)", "[]", LanguageVersion.ORIGINAL_IQL1);
        checkTotals("from organic yesterday today select percentile(oji, 50), oji", "[2653.0]", LanguageVersion.ORIGINAL_IQL1);
        checkTotals("from organic yesterday today group by tk select percentile(oji, 50), oji", "[2653.0]", LanguageVersion.ORIGINAL_IQL1);
        checkTotals("from organic yesterday today group by tk in (\"a\", \"b\") select percentile(oji, 50), oji", "[2653.0]", LanguageVersion.ORIGINAL_IQL1);

        checkTotals("from organic yesterday today select percentile(oji, 50)", "[10.0]", LanguageVersion.IQL1_LEGACY_MODE);
        checkTotals("from organic yesterday today group by tk select percentile(oji, 50)", "[10.0]", LanguageVersion.IQL1_LEGACY_MODE);
        checkTotals("from organic yesterday today group by tk in (\"a\", \"b\") select percentile(oji, 50)", "[10.0]", LanguageVersion.IQL1_LEGACY_MODE);
        checkTotals("from organic yesterday today select percentile(oji, 50), oji", "[10.0, 2653.0]", LanguageVersion.IQL1_LEGACY_MODE);
        checkTotals("from organic yesterday today group by tk select percentile(oji, 50), oji", "[10.0, 2653.0]", LanguageVersion.IQL1_LEGACY_MODE);
        checkTotals("from organic yesterday today group by tk in (\"a\", \"b\") select percentile(oji, 50), oji", "[10.0, 2653.0]", LanguageVersion.IQL1_LEGACY_MODE);

        // distinct
        final List<List<String>> distinct = ImmutableList.of(ImmutableList.of("", "23", "2653"));
        testAll(distinct, "from organic yesterday today select distinct(oji), oji", true);

        checkTotals("from organic yesterday today select distinct(oji)", "[]", LanguageVersion.ORIGINAL_IQL1);
        checkTotals("from organic yesterday today group by tk select distinct(oji)", "[]", LanguageVersion.ORIGINAL_IQL1);
        checkTotals("from organic yesterday today group by tk in (\"a\", \"b\") select distinct(oji)", "[]", LanguageVersion.ORIGINAL_IQL1);
        checkTotals("from organic yesterday today select distinct(oji), oji", "[2653.0]", LanguageVersion.ORIGINAL_IQL1);
        checkTotals("from organic yesterday today group by tk select distinct(oji), oji", "[2653.0]", LanguageVersion.ORIGINAL_IQL1);
        checkTotals("from organic yesterday today group by tk in (\"a\", \"b\") select distinct(oji), oji", "[2653.0]", LanguageVersion.ORIGINAL_IQL1);

        checkTotals("from organic yesterday today select distinct(oji)", "[23.0]", LanguageVersion.IQL1_LEGACY_MODE);
        checkTotals("from organic yesterday today group by tk select distinct(oji)", "[23.0]", LanguageVersion.IQL1_LEGACY_MODE);
        checkTotals("from organic yesterday today group by tk in (\"a\", \"b\") select distinct(oji)", "[23.0]", LanguageVersion.IQL1_LEGACY_MODE);
        checkTotals("from organic yesterday today select distinct(oji), oji", "[23.0, 2653.0]", LanguageVersion.IQL1_LEGACY_MODE);
        checkTotals("from organic yesterday today group by tk select distinct(oji), oji", "[23.0, 2653.0]", LanguageVersion.IQL1_LEGACY_MODE);
        checkTotals("from organic yesterday today group by tk in (\"a\", \"b\") select distinct(oji), oji", "[23.0, 2653.0]", LanguageVersion.IQL1_LEGACY_MODE);
    }

    // TODO: add more tests for other header values.
}
