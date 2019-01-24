package com.indeed.iql2.language.transform;

import com.google.common.base.Functions;
import com.indeed.iql2.language.DocMetricsTest;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.shardresolution.NullShardResolver;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.util.logging.TracingTreeTimer;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class QueryTransformTest {
    @Test
    public void test() {
        final String query = "from synthetic 2d 1d where tk=\"a\" select oji";
        final Queries.ParseResult result = Queries.parseQuery(query, false, AllData.DATASET.getDatasetsMetadata(), Collections.emptySet(), DocMetricsTest.CLOCK, new TracingTreeTimer(), new NullShardResolver());
        final Query parsedQuery = result.query;
        Assert.assertEquals(query, parsedQuery.getRawInput());
        final Query newQuery = parsedQuery.transform(Functions.identity(), Functions.identity(), Functions.identity(), Functions.identity(), Functions.identity());
        Assert.assertEquals(parsedQuery, newQuery);
        Assert.assertNotSame(parsedQuery, newQuery);
        Assert.assertEquals(query, newQuery.getRawInput());
    }
}
