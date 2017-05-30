package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.ImmutableList;
import com.indeed.common.util.time.StoppedClock;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.TestImhotepClient;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.squall.iql2.language.query.Query;
import com.indeed.squall.iql2.server.web.servlets.dataset.OrganicDataset;
import com.indeed.squall.iql2.server.web.servlets.query.SelectQueryExecution;
import com.indeed.util.core.TreeTimer;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CacheTest extends BasicTest {
    // Unique in the context of 1 day of hourly sharded data in organic yesterday (2015-01-01).
    private final ImmutableList<String> uniqueQueries = ImmutableList.of(
            "from organic yesterday today",
            "from organic 60m 0m",
            "from organic 60m 30m",
            "from organic yesterday today group by time(1h)",
            "from organic yesterday today group by time(1h) select oji",
            "from organic yesterday today group by time(1h) select oji, ojc",
            "from organic yesterday today where rcv=\"jsv\"",
            "from organic yesterday today where rcv=\"jsv\" group by time(1h)",
            "from organic yesterday today where rcv=\"jsv\" group by time(1h) select oji"
    );

    private static String getCacheKey(ImhotepClient imhotepClient, String queryString) {
        final HashMap<String, Set<String>> datasetToKeywordAnalyzerFields = new HashMap<>();
        final HashMap<String, Set<String>> datasetToIntFields = new HashMap<>();
        final Query query = Queries.parseQuery(queryString, false /* todo: param? */, datasetToKeywordAnalyzerFields, datasetToIntFields, new Consumer<String>() {
            @Override
            public void accept(String s) {

            }
        }, new StoppedClock(new DateTime(2015, 1, 1, 0, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis())).query;
        return SelectQueryExecution.computeCacheKey(new TreeTimer(), query, Queries.queryCommands(query, Collections.emptyMap()), imhotepClient).cacheFileName;
    }

    @Test
    public void testUniqueCacheValues() {
        final Set<String> values = new HashSet<>();
        final ImhotepClient imhotepClient = new TestImhotepClient(OrganicDataset.create().getShards());
        for (final String query : uniqueQueries) {
            final String cacheKey = getCacheKey(imhotepClient, query);
            Assert.assertFalse(values.contains(cacheKey));
            values.add(cacheKey);
        }
    }

    @Test
    public void testConsistentCaching() {
        final TestImhotepClient imhotepClient = new TestImhotepClient(OrganicDataset.create().getShards());
        for (final String query : uniqueQueries) {
            final String cacheKey1 = getCacheKey(imhotepClient, query);
            final String cacheKey2 = getCacheKey(imhotepClient, query);
            Assert.assertEquals(cacheKey1, cacheKey2);
        }
    }

    @Test
    public void testStorageAndLoading() throws Exception {
        for (final boolean withLimit : new boolean[]{false, true}) {
            final List<Shard> shards = OrganicDataset.create().getShards();
            final String query = "from organic yesterday today group by time(1h) select count()" + (withLimit ? " limit 100" : "");

            final QueryServletTestUtils.Options options = new QueryServletTestUtils.Options();
            final InMemoryQueryCache queryCache = new InMemoryQueryCache();
            options.setQueryCache(queryCache);
            Assert.assertEquals(Collections.emptySet(), queryCache.getReadsTracked());
            final List<List<String>> result1 = QueryServletTestUtils.runQuery(shards, query, QueryServletTestUtils.LanguageVersion.IQL2, true, options);
            Assert.assertEquals(Collections.emptySet(), queryCache.getReadsTracked());
            final List<List<String>> result2 = QueryServletTestUtils.runQuery(shards, query, QueryServletTestUtils.LanguageVersion.IQL2, true, options);
            Assert.assertEquals(1, queryCache.getReadsTracked().size());
            Assert.assertEquals(result1, result2);
        }
    }
}
