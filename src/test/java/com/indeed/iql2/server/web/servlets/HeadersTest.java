package com.indeed.iql2.server.web.servlets;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.indeed.iql2.server.web.servlets.QueryServletTestUtils.LanguageVersion;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import org.junit.Test;

import java.util.List;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.ResultFormat.TSV;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
        assertNotNull(totals);
        final String quotedValue = totals.toString();
        assertEquals(quotedValue, "\"" + expectedTotals + "\"");
    }

    @Test
    public void testNewestShardHeaderPresent() throws Exception {
        final QueryServletTestUtils.Options options = QueryServletTestUtils.Options.create();
        options.setHeadOnly(true);
        options.setGetVersion(true);
        for (final LanguageVersion languageVersion : LanguageVersion.values()) {
            final JsonNode headers = QueryServletTestUtils.getQueryHeader(
                    AllData.DATASET.getNormalClient(),
                    "from organic yesterday today",
                    languageVersion,
                    options,
                    TSV
            );
            final JsonNode jsonNode = headers.get("IQL-Newest-Shard");
            assertNotNull(jsonNode.asText());
        }
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
    public void testStatsTotalsPercentileAndDistinct() throws Exception {
        // In case of percentile or distinct IQL1 does not put it as totals.

        // percentile
        checkTotals("from organic yesterday today select percentile(oji, 50)", "[]");
        checkTotals("from organic yesterday today group by tk select percentile(oji, 50)", "[]");
        checkTotals("from organic yesterday today group by tk in (\"a\", \"b\") select percentile(oji, 50)", "[]");
        checkTotals("from organic yesterday today select percentile(oji, 50), oji", "[2653.0]");
        checkTotals("from organic yesterday today group by tk select percentile(oji, 50), oji", "[2653.0]");
        checkTotals("from organic yesterday today group by tk in (\"a\", \"b\") select percentile(oji, 50), oji", "[2653.0]");

        // distinct
        checkTotals("from organic yesterday today select distinct(oji)", "[]");
        checkTotals("from organic yesterday today group by tk select distinct(oji)", "[]");
        checkTotals("from organic yesterday today group by tk in (\"a\", \"b\") select distinct(oji)", "[]");
        checkTotals("from organic yesterday today select distinct(oji), oji", "[2653.0]");
        checkTotals("from organic yesterday today group by tk select distinct(oji), oji", "[2653.0]");
        checkTotals("from organic yesterday today group by tk in (\"a\", \"b\") select distinct(oji), oji", "[2653.0]");
    }

    // TODO: add more tests for other header values.
}
