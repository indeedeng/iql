/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.iql2.server.web.servlets;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.iql.cache.CompletableOutputStream;
import com.indeed.iql.cache.QueryCache;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.execution.QueryOptions;
import com.indeed.iql2.execution.ResultFormat;
import com.indeed.iql2.language.cachekeys.CacheKey;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.shardresolution.ImhotepClientShardResolver;
import com.indeed.iql2.language.query.shardresolution.ShardResolver;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.logging.TracingTreeTimer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.LanguageVersion.IQL2;
import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.ResultFormat.EVENT_STREAM;

public class CacheTest extends BasicTest {
    // TODO: can we lower this?
    private static final long CACHE_WRITE_TIMEOUT = 1000L;
    private static final DateTimeZone TIME_ZONE = DateTimeZone.forOffsetHours(-6);
    // Unique in the context of 1 day of hourly sharded data in organic yesterday (2015-01-01).
    private final ImmutableList<String> uniqueQueries = ImmutableList.of(
            "from organic yesterday today",
            "from organic 60m 0m",
            "from organic 60m 30m",
            "from organic yesterday today group by time(1h)",
            "from organic yesterday today group by time(1h) select oji",
            "from organic yesterday today group by time(1h) select oji, ojc",
            "from organic yesterday today where oji=10",
            "from organic yesterday today where oji=10 group by time(1h)",
            "from organic yesterday today where oji=10 group by time(1h) select oji",
            "from organic yesterday today where tk in (from same group by tk)",
            "from organic yesterday today where tk in (from same where tk=\"a\" group by tk)",
            "from organic yesterday today where tk in (from same where oji=10 group by tk)",
            "from organic yesterday today where tk in (from organic 60m 0m group by tk)",
            "from organic yesterday today where tk in (from organic 60m 0m where tk=\"a\" group by tk)",
            "from organic yesterday today where tk in (from organic 60m 0m where oji=10 group by tk)",
            "from organic yesterday today where tk not in (from same group by tk)",
            "from organic yesterday today where tk not in (from same where tk=\"a\" group by tk)",
            "from organic yesterday today where tk not in (from same where oji=10 group by tk)",
            "from organic yesterday today where tk not in (from organic 60m 0m group by tk)",
            "from organic yesterday today where tk not in (from organic 60m 0m where tk=\"a\" group by tk)",
            "from organic yesterday today where tk not in (from organic 60m 0m where oji=10 group by tk)",
            "from organic yesterday today group by tk in (from organic 60m 0m where tk=\"a\" group by tk)",
            "from organic yesterday today group by tk not in (from organic 60m 0m where tk=\"a\" group by tk)"
    );
    private static final StoppedClock CLOCK = new StoppedClock(new DateTime(2015, 1, 1, 0, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis());

    private static String getCacheKey(final String queryString) {
        final ImhotepClient imhotepClient = AllData.DATASET.getNormalClient();
        final ImhotepClientShardResolver shardResolver = new ImhotepClientShardResolver(imhotepClient);
        return getCacheKey(queryString, shardResolver);
    }

    private static String getCacheKey(final String queryString, final ShardResolver shardResolver) {
        final DatasetsMetadata datasetsMetadata = AllData.DATASET.getDatasetsMetadata();
        final Query query = Queries.parseQuery(queryString, false /* todo: param? */, datasetsMetadata, Collections.emptySet(), s -> {}, CLOCK, new TracingTreeTimer(), shardResolver).query;
        return CacheKey.computeCacheKey(query, ResultFormat.TSV).cacheFileName;
    }

    @Test
    public void testUniqueCacheValues() {
        final Map<String, String> values = new HashMap<>();
        for (final String query : uniqueQueries) {
            final String cacheKey = getCacheKey(query);
            Assert.assertFalse("Encountered cache collision in what should be unique values:\nq1:" + query + "\nq2:" + values.get(cacheKey), values.containsKey(cacheKey));
            values.put(cacheKey, query);
        }
    }

    @Test
    public void testConsistentCaching() {
        for (final String query : uniqueQueries) {
            final String cacheKey1 = getCacheKey(query);
            final String cacheKey2 = getCacheKey(query);
            Assert.assertEquals(cacheKey1, cacheKey2);
        }
    }

    @Test
    public void testNewShards() {
        // Ensure that we get different cache keys when new shards arrive
        final String oneShardKey = getCacheKey("from organic yesterday today", new ShardResolver() {
            @Nullable
            @Override
            public ShardResolutionResult resolve(final String dataset, final DateTime start, final DateTime end) {
                Assert.assertEquals("organic", dataset);
                Assert.assertEquals(new DateTime(2014, 12, 31, 0, 0, TIME_ZONE), start);
                Assert.assertEquals(new DateTime(2015, 1, 1, 0, 0, TIME_ZONE), end);
                return new ShardResolutionResult(
                        Collections.singletonList(new Shard("index20141231.00", 10, 123L)),
                        Collections.singletonList(new Interval(
                                new DateTime(2014, 12, 31, 1, 0, TIME_ZONE),
                                new DateTime(2015, 1, 1, 0, 0, TIME_ZONE)
                        ))
                );
            }
        });
        final String twoShardKey = getCacheKey("from organic yesterday today", new ShardResolver() {
            @Nullable
            @Override
            public ShardResolutionResult resolve(final String dataset, final DateTime start, final DateTime end) {
                Assert.assertEquals("organic", dataset);
                Assert.assertEquals(new DateTime(2014, 12, 31, 0, 0, TIME_ZONE), start);
                Assert.assertEquals(new DateTime(2015, 1, 1, 0, 0, TIME_ZONE), end);
                return new ShardResolutionResult(
                        Arrays.asList(
                                new Shard("index20141231.00", 10, 123L),
                                new Shard("index20141231.01", 15, 234L)
                        ),
                        Collections.singletonList(new Interval(
                                new DateTime(2014, 12, 31, 2, 0, TIME_ZONE),
                                new DateTime(2015, 1, 1, 0, 0, TIME_ZONE)
                        ))
                );
            }
        });
        Assert.assertFalse("Should have different cache keys for 1 shard and 2 shards", oneShardKey.equals(twoShardKey));
        final String twelveShards = getCacheKey("from organic yesterday today", new ShardResolver() {
            @Nullable
            @Override
            public ShardResolutionResult resolve(final String dataset, final DateTime start, final DateTime end) {
                Assert.assertEquals("organic", dataset);
                Assert.assertEquals(new DateTime(2014, 12, 31, 0, 0, TIME_ZONE), start);
                Assert.assertEquals(new DateTime(2015, 1, 1, 0, 0, TIME_ZONE), end);
                return new ShardResolutionResult(
                        Arrays.asList(
                                new Shard("index20141231.00", 10, 123L),
                                new Shard("index20141231.01", 10, 234L),
                                new Shard("index20141231.02", 10, 125L),
                                new Shard("index20141231.03", 10, 126L),
                                new Shard("index20141231.04", 10, 127L),
                                new Shard("index20141231.05", 10, 128L),
                                new Shard("index20141231.06", 10, 129L),
                                new Shard("index20141231.07", 10, 130L),
                                new Shard("index20141231.08", 10, 131L),
                                new Shard("index20141231.09", 10, 132L),
                                new Shard("index20141231.10", 10, 133L),
                                new Shard("index20141231.11", 10, 134L),
                                new Shard("index20141231.12", 15, 234L)
                        ),
                        Collections.singletonList(new Interval(
                                new DateTime(2014, 12, 31, 13, 0, TIME_ZONE),
                                new DateTime(2015, 1, 1, 0, 0, TIME_ZONE)
                        ))
                );
            }
        });
        final Set<String> keys = Sets.newHashSet(oneShardKey, twoShardKey, twelveShards);
        Assert.assertEquals("Expect unique cache keys for 1, 2, 12 shards", 3, keys.size());
    }

    @Test
    public void testShardUpdate() {
        // Ensure that we get different cache keys when shards get updated
        final String firstShardKey = getCacheKey("from organic yesterday today", new ShardResolver() {
            @Nullable
            @Override
            public ShardResolutionResult resolve(final String dataset, final DateTime start, final DateTime end) {
                Assert.assertEquals("organic", dataset);
                Assert.assertEquals(new DateTime(2014, 12, 31, 0, 0, TIME_ZONE), start);
                Assert.assertEquals(new DateTime(2015, 1, 1, 0, 0, TIME_ZONE), end);
                return new ShardResolutionResult(
                        Collections.singletonList(new Shard("index20141231.00", 10, 123L)),
                        Collections.singletonList(new Interval(
                                new DateTime(2014, 12, 31, 1, 0, TIME_ZONE),
                                new DateTime(2015, 1, 1, 0, 0, TIME_ZONE)
                        ))
                );
            }
        });
        final String secondShardKey = getCacheKey("from organic yesterday today", new ShardResolver() {
            @Nullable
            @Override
            public ShardResolutionResult resolve(final String dataset, final DateTime start, final DateTime end) {
                Assert.assertEquals("organic", dataset);
                Assert.assertEquals(new DateTime(2014, 12, 31, 0, 0, TIME_ZONE), start);
                Assert.assertEquals(new DateTime(2015, 1, 1, 0, 0, TIME_ZONE), end);
                return new ShardResolutionResult(
                        Collections.singletonList(new Shard("index20141231.00", 10, 124L)),
                        Collections.singletonList(new Interval(
                                new DateTime(2014, 12, 31, 1, 0, TIME_ZONE),
                                new DateTime(2015, 1, 1, 0, 0, TIME_ZONE)
                        ))
                );
            }
        });
        Assert.assertFalse("Should have different cache keys for two different versions of a shard", firstShardKey.equals(secondShardKey));
    }

    @Test
    public void testStorageAndLoading() throws Exception {
        for (final boolean withLimit : new boolean[]{false, true}) {
            final String query = "from organic yesterday today group by time(1h) select count()" + (withLimit ? " limit 100" : "");

            final QueryServletTestUtils.Options options = new QueryServletTestUtils.Options();
            final InMemoryQueryCache queryCache = new InMemoryQueryCache();
            options.setQueryCache(queryCache);
            Assert.assertEquals(Collections.emptySet(), queryCache.getReadsTracked());
            final List<List<String>> result1 = QueryServletTestUtils.runQuery(query, IQL2, EVENT_STREAM, options, Collections.emptySet());
            Assert.assertEquals(Collections.emptySet(), queryCache.getReadsTracked());
            final int expectedCachedFiles = 2; // should have 2 files: metadata and data
            awaitCacheWrites(queryCache, expectedCachedFiles);
            final List<List<String>> result2 = QueryServletTestUtils.runQuery(query, IQL2, EVENT_STREAM, options, Collections.emptySet());
            Assert.assertEquals("Didn't read from cache when it was expected to", expectedCachedFiles, queryCache.getReadsTracked().size());
            Assert.assertEquals(result1, result2);
        }
    }

    @Test
    public void testSubQueryCache() throws Exception {
        final QueryServletTestUtils.Options options = new QueryServletTestUtils.Options();
        final InMemoryQueryCache queryCache = new InMemoryQueryCache();
        options.setQueryCache(queryCache);

        Assert.assertEquals(Collections.emptySet(), queryCache.getReadsTracked());
        QueryServletTestUtils.runQuery("from organic 2d 1d where country in (from organic 2d 1d group by country[5]) group by country", IQL2, EVENT_STREAM, options, Collections.emptySet());
        Assert.assertEquals(Collections.emptySet(), queryCache.getReadsTracked());
        // expect top-level metadata and data for both top-level and sub-query.
        awaitCacheWrites(queryCache, 3);

        QueryServletTestUtils.runQuery("from organic 2d 1d where country in (from organic 2d 1d group by country[5]) group by tk", IQL2, EVENT_STREAM, options, Collections.emptySet());
        Assert.assertEquals(1, queryCache.getReadsTracked().size());
    }

    @Test
    public void testResultSizeLimit() throws Exception {
        final String query = "from organic yesterday today group by time(1h) select count()";

        final QueryServletTestUtils.Options options = new QueryServletTestUtils.Options();
        final InMemoryQueryCache queryCache = new InMemoryQueryCache();
        options.setQueryCache(queryCache);
        Assert.assertEquals(Collections.emptySet(), queryCache.getWritesTracked());

        // 10 byte limit, should fail
        options.setMaxCacheQuerySizeLimitBytes(10L);
        QueryServletTestUtils.runQuery(query, IQL2, EVENT_STREAM, options, Collections.emptySet());
        // wait for async cache write, just in case
        Thread.sleep(CACHE_WRITE_TIMEOUT);
        Assert.assertEquals(Collections.emptySet(), queryCache.getWritesTracked());

        // 10KB limit, should succeed
        options.setMaxCacheQuerySizeLimitBytes(10_240L);
        QueryServletTestUtils.runQuery(query, IQL2, EVENT_STREAM, options, Collections.emptySet());
        awaitCacheWrites(queryCache, 2);

        final long fileSize = queryCache.getCachedValues()
                .entrySet()
                .stream()
                .filter(x -> x.getKey().endsWith(".tsv"))
                .mapToLong(x -> x.getValue().getBytes(Charsets.UTF_8).length)
                .max()
                .getAsLong();
        queryCache.clear();

        // Size - 1 should fail to write
        options.setMaxCacheQuerySizeLimitBytes(fileSize - 1L);
        QueryServletTestUtils.runQuery(query, IQL2, EVENT_STREAM, options, Collections.emptySet());
        // wait for async cache write, just in case
        Thread.sleep(CACHE_WRITE_TIMEOUT);
        Assert.assertEquals(Collections.emptySet(), queryCache.getWritesTracked());

        // exact size should succeed
        options.setMaxCacheQuerySizeLimitBytes(fileSize);
        QueryServletTestUtils.runQuery(query, IQL2, EVENT_STREAM, options, Collections.emptySet());
        awaitCacheWrites(queryCache, 2);

        queryCache.clear();

        // no limit should succeed
        options.setMaxCacheQuerySizeLimitBytes(null);
        QueryServletTestUtils.runQuery(query, IQL2, EVENT_STREAM, options, Collections.emptySet());
        awaitCacheWrites(queryCache, 2);
    }

    private void awaitCacheWrites(final InMemoryQueryCache queryCache, final int expectedCachedFiles) throws InterruptedException {
        final long waitStart = System.currentTimeMillis();
        while (queryCache.getWritesTracked().size() != expectedCachedFiles) {
            if ((System.currentTimeMillis() - waitStart) > CACHE_WRITE_TIMEOUT) {
                Assert.fail("Async cache upload didn't complete in " + CACHE_WRITE_TIMEOUT + " second(s). Expected files written to cache " + expectedCachedFiles +
                        ", actually written " + queryCache.getWritesTracked().size());
            }
            Thread.sleep(1);
        }
    }

    @Test
    public void testBrokenCache() throws Exception {
        final String query = "from organic yesterday today group by time(1h) select count()";

        final QueryServletTestUtils.Options options = new QueryServletTestUtils.Options();
        options.setQueryCache(new FailOnCloseQueryCache());
        ensureTmpFilesCleanedUp(query, options);
    }

    @Test
    public void testFailureCleanup() throws Exception {
        final String query = "from organic yesterday today group by time(1h) select count() OPTIONS [\"" + QueryOptions.DIE_AT_END + "\"]";

        final QueryServletTestUtils.Options options = new QueryServletTestUtils.Options();
        options.setQueryCache(new InMemoryQueryCache());
        ensureTmpFilesCleanedUp(query, options);
    }

    private void ensureTmpFilesCleanedUp(final String query, final QueryServletTestUtils.Options options) throws Exception {
        final File tmpTmpDir = Files.createTempDir();
        try {
            options.setTmpDir(tmpTmpDir);
            try {
                QueryServletTestUtils.runQuery(query, IQL2, EVENT_STREAM, options, Collections.emptySet());
            } catch (final Exception ignored) {
            }
            final long waitStart = System.currentTimeMillis();
            while (tmpTmpDir.list().length > 0) {
                if (System.currentTimeMillis() - waitStart > 1000) {
                    Assert.fail("Temp files were not cleaned up within 1 second. Likely error in error case cleanup code.");
                }
                Thread.sleep(1);
            }
        } finally {
            tmpTmpDir.delete();
        }
    }

    private static class FailOnCloseQueryCache implements QueryCache {
        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public boolean isEnabledInConfig() {
            return true;
        }

        @Override
        public boolean isFileCached(final String fileName) {
            return false;
        }

        @Nullable
        @Override
        public InputStream getInputStream(final String cachedFileName) {
            return null;
        }

        @Override
        public CompletableOutputStream getOutputStream(final String cachedFileName) {
            return new CompletableOutputStream() {
                @Override
                public void write(final int b) {
                }

                @Override
                public void close() {
                    throw new UnsupportedOperationException("You thought you could close me? (expected exception)");
                }
            };
        }

        @Override
        public void healthcheck() {
        }
    }
}
