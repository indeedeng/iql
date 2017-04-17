package com.indeed.squall.iql2.language.query;

import com.google.common.collect.ImmutableList;
import com.indeed.common.util.time.DefaultWallClock;
import com.indeed.squall.iql2.language.JQLParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 *
 */
public class QueriesTest {
    @Test
    public void extractHeaders() throws Exception {
        final boolean useLegacy = false;
        Assert.assertEquals(ImmutableList.of("count()"),
                extractHeadersHelper("FROM jobsearch 1d 0d", useLegacy));
        Assert.assertEquals(ImmutableList.of("oji", "count()"),
                extractHeadersHelper("FROM jobsearch 1d 0d GROUP BY oji", useLegacy));
        Assert.assertEquals(ImmutableList.of("oji", "total_count", "o1.count()", "add"),
                extractHeadersHelper("FROM jobsearch 1d 0d as o1, mobsearch as o2 GROUP BY oji " +
                        "SELECT count() as total_count, o1.count(), o1.a+o2.b as add", useLegacy));

    }

    private List<String> extractHeadersHelper(final String q, final boolean useLegacy) {
        final Query query = Queries.parseQuery(
                q, useLegacy, Collections.emptyMap(), Collections.emptyMap(), new DefaultWallClock()).query;
        final JQLParser.QueryContext queryContext = Queries.parseQueryContext(q, useLegacy);
        return Queries.extractHeaders(query, queryContext.start.getInputStream());
    }
}