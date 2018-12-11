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
import com.google.common.io.Files;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.iql.cache.CompletableOutputStream;
import com.indeed.iql.cache.QueryCache;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.execution.QueryOptions;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.iql2.server.web.servlets.query.SelectQueryExecution;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.logging.TracingTreeTimer;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static com.indeed.iql2.server.web.servlets.QueryServletTestUtils.LanguageVersion.IQL2;

public class CacheTest extends BasicTest {
    // TODO: can we lower this?
    public static final long CACHE_WRITE_TIMEOUT = 1000L;
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
            "from organic yesterday today where oji=10 group by time(1h) select oji"
    );

    private static String getCacheKey(final String queryString) {
        final DatasetsMetadata datasetsMetadata = AllData.DATASET.getDatasetsMetadata();
        final ImhotepClient imhotepClient = AllData.DATASET.getNormalClient();
        final Query query = Queries.parseQuery(queryString, false /* todo: param? */, datasetsMetadata, Collections.emptySet(), new Consumer<String>() {
            @Override
            public void accept(String s) {

            }
        }, new StoppedClock(new DateTime(2015, 1, 1, 0, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis())).query;
        return SelectQueryExecution.computeCacheKey(new TracingTreeTimer(), query, Queries.queryCommands(query, datasetsMetadata), imhotepClient).cacheFileName;
    }

    @Test
    public void testUniqueCacheValues() {
        final Set<String> values = new HashSet<>();
        for (final String query : uniqueQueries) {
            final String cacheKey = getCacheKey(query);
            Assert.assertFalse(values.contains(cacheKey));
            values.add(cacheKey);
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
    public void testStorageAndLoading() throws Exception {
        for (final boolean withLimit : new boolean[]{false, true}) {
            final ImhotepClient imhotepClient = AllData.DATASET.getNormalClient();
            final String query = "from organic yesterday today group by time(1h) select count()" + (withLimit ? " limit 100" : "");

            final QueryServletTestUtils.Options options = new QueryServletTestUtils.Options();
            final InMemoryQueryCache queryCache = new InMemoryQueryCache();
            options.setQueryCache(queryCache);
            Assert.assertEquals(Collections.emptySet(), queryCache.getReadsTracked());
            final List<List<String>> result1 = QueryServletTestUtils.runQuery(imhotepClient, query, IQL2, true, options, Collections.emptySet());
            Assert.assertEquals(Collections.emptySet(), queryCache.getReadsTracked());
            final int expectedCachedFiles = 2; // should have 2 files: metadata and data
            awaitCacheWrites(queryCache, expectedCachedFiles);
            final List<List<String>> result2 = QueryServletTestUtils.runQuery(imhotepClient, query, IQL2, true, options, Collections.emptySet());
            Assert.assertEquals("Didn't read from cache when it was expected to", expectedCachedFiles, queryCache.getReadsTracked().size());
            Assert.assertEquals(result1, result2);
        }
    }

    @Test
    public void testResultSizeLimit() throws Exception {
        final ImhotepClient imhotepClient = AllData.DATASET.getNormalClient();
        final String query = "from organic yesterday today group by time(1h) select count()";

        final QueryServletTestUtils.Options options = new QueryServletTestUtils.Options();
        final InMemoryQueryCache queryCache = new InMemoryQueryCache();
        options.setQueryCache(queryCache);
        Assert.assertEquals(Collections.emptySet(), queryCache.getWritesTracked());

        // 10 byte limit, should fail
        options.setMaxCacheQuerySizeLimitBytes(10L);
        QueryServletTestUtils.runQuery(imhotepClient, query, IQL2, true, options, Collections.emptySet());
        // wait for async cache write, just in case
        Thread.sleep(CACHE_WRITE_TIMEOUT);
        Assert.assertEquals(Collections.emptySet(), queryCache.getWritesTracked());

        // 10KB limit, should succeed
        options.setMaxCacheQuerySizeLimitBytes(10_240L);
        QueryServletTestUtils.runQuery(imhotepClient, query, IQL2, true, options, Collections.emptySet());
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
        QueryServletTestUtils.runQuery(imhotepClient, query, IQL2, true, options, Collections.emptySet());
        // wait for async cache write, just in case
        Thread.sleep(CACHE_WRITE_TIMEOUT);
        Assert.assertEquals(Collections.emptySet(), queryCache.getWritesTracked());

        // exact size should succeed
        options.setMaxCacheQuerySizeLimitBytes(fileSize);
        QueryServletTestUtils.runQuery(imhotepClient, query, IQL2, true, options, Collections.emptySet());
        awaitCacheWrites(queryCache, 2);

        queryCache.clear();

        // no limit should succeed
        options.setMaxCacheQuerySizeLimitBytes(null);
        QueryServletTestUtils.runQuery(imhotepClient, query, IQL2, true, options, Collections.emptySet());
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
        final ImhotepClient imhotepClient = AllData.DATASET.getNormalClient();
        final String query = "from organic yesterday today group by time(1h) select count()";

        final QueryServletTestUtils.Options options = new QueryServletTestUtils.Options();
        options.setQueryCache(new FailOnCloseQueryCache());
        ensureTmpFilesCleanedUp(imhotepClient, query, options);
    }

    @Test
    public void testFailureCleanup() throws Exception {
        final ImhotepClient imhotepClient = AllData.DATASET.getNormalClient();
        final String query = "from organic yesterday today group by time(1h) select count() OPTIONS [\"" + QueryOptions.DIE_AT_END + "\"]";

        final QueryServletTestUtils.Options options = new QueryServletTestUtils.Options();
        options.setQueryCache(new InMemoryQueryCache());
        ensureTmpFilesCleanedUp(imhotepClient, query, options);
    }

    private void ensureTmpFilesCleanedUp(final ImhotepClient imhotepClient, final String query, final QueryServletTestUtils.Options options) throws Exception {
        final File tmpTmpDir = Files.createTempDir();
        try {
            options.setTmpDir(tmpTmpDir);
            try {
                QueryServletTestUtils.runQuery(imhotepClient, query, IQL2, true, options, Collections.emptySet());
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
