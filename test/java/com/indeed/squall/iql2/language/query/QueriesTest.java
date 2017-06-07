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
        Assert.assertEquals(ImmutableList.of("", "count()"),
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

    @Test
    public void extractDatasets()  {
        Assert.assertEquals(
                ImmutableList.of(new SplitQuery.Dataset("jobsearch", "", "1d", "0d", "j1", "")),
                extractDatasetsHelper("FROM jobsearch 1d 0d as j1", true));
        Assert.assertEquals(
                ImmutableList.of(new SplitQuery.Dataset("jobsearch", "oji=1 tk='1'", "1d", "0d", "", "ALIASING(oji as o, ojc as c)")),
                extractDatasetsHelper("FROM jobsearch(oji=1 tk='1') 1d 0d ALIASING(oji as o, ojc as c)", false));
        Assert.assertEquals(
                ImmutableList.of(
                        new SplitQuery.Dataset("jobsearch", "oji=1", "1d", "0d", "j1", ""),
                        new SplitQuery.Dataset("jobsearch", "", "2d", "1d", "j2", ""),
                        new SplitQuery.Dataset("mobsearch", "", "1d", "0d", "", "ALIASING(ojc as c)")
                ),
                extractDatasetsHelper("FROM jobsearch(oji=1) 1d 0d as j1, jobsearch 2d 1d as j2, mobsearch ALIASING(ojc as c) GROUP BY oji", false)
        );
    }

    private List<SplitQuery.Dataset> extractDatasetsHelper(final String q, final boolean useLegacy) {
        final JQLParser.QueryContext queryContext = Queries.parseQueryContext(q, useLegacy);
        return Queries.extractDatasets(queryContext.fromContents(), queryContext.start.getInputStream());
    }
}