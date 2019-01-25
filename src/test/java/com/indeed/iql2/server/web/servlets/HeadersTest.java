package com.indeed.iql2.server.web.servlets;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.indeed.iql2.server.web.servlets.QueryServletTestUtils.LanguageVersion;
import org.junit.Test;

import java.util.List;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.testAll;
import static org.junit.Assert.assertTrue;

public class HeadersTest extends BasicTest {

    private static void checkTotals(
            final String query,
            final String expectedTotals) throws Exception {
        assertTrue(checkTotals(query, expectedTotals, LanguageVersion.ORIGINAL_IQL1));
        assertTrue(checkTotals(query, expectedTotals, LanguageVersion.IQL1_LEGACY_MODE));
    }

    private static boolean checkTotals(
            final String query,
            final String expectedTotals,
            final LanguageVersion version) throws Exception {
        final JsonNode header = QueryServletTestUtils.getQueryHeader(query, version);
        final JsonNode totals = header.get("IQL-Totals");
        if (totals == null) {
            return false;
        }
        final String quotedValue = totals.toString();
        return quotedValue.equals("\"" + expectedTotals + "\"");
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


    // TODO: add more tests for other header values.
}
