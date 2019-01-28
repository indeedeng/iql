package com.indeed.iql2.language.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.indeed.imhotep.Shard;
import com.indeed.iql2.language.query.shardresolution.ImhotepClientShardResolver;
import com.indeed.iql2.language.query.shardresolution.NullShardResolver;
import com.indeed.iql2.language.query.shardresolution.ShardResolver;
import com.indeed.iql2.server.web.servlets.QueryServletTestUtils;
import com.indeed.iql2.server.web.servlets.dataset.AllData;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.logging.TracingTreeTimer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;

public class DatasetTest {

    public static final StoppedClock CLOCK = new StoppedClock(new DateTime(2015, 1, 2, 0, 0, DateTimeZone.forOffsetHours(-6)).getMillis());
    public static final ImhotepClientShardResolver SHARD_RESOLVER = new ImhotepClientShardResolver(AllData.DATASET.getNormalClient());

    private static List<Dataset> parseQueryDatasets(
            final String query,
            final QueryServletTestUtils.LanguageVersion languageVersion,
            @Nullable final ShardResolver shardResolverOverride
    ) {
        Preconditions.checkArgument(languageVersion != QueryServletTestUtils.LanguageVersion.ORIGINAL_IQL1);
        return Queries.parseQuery(
                query,
                languageVersion == QueryServletTestUtils.LanguageVersion.IQL1_LEGACY_MODE,
                AllData.DATASET.getDatasetsMetadata(),
                Collections.emptySet(),
                CLOCK,
                new TracingTreeTimer(),
                Optional.ofNullable(shardResolverOverride).orElse(SHARD_RESOLVER)
        ).query.datasets;
    }

    @Test
    public void testEquality() {
        final List<Dataset> ds1 = parseQueryDatasets("from organic yesterday today", QueryServletTestUtils.LanguageVersion.IQL2, null);
        final List<Dataset> ds2 = parseQueryDatasets("from organic 1d 0d", QueryServletTestUtils.LanguageVersion.IQL2, null);
        assertSame(ds1, ds2);

        final List<Dataset> shuffledShards = parseQueryDatasets("from organic yesterday today", QueryServletTestUtils.LanguageVersion.IQL2, new ShufflingShardResolver(SHARD_RESOLVER));
        assertSame(ds1, shuffledShards);
    }

    private static void assertSame(final List<Dataset> a, final List<Dataset> b) {
        Assert.assertEquals(a, b);
        Assert.assertEquals(a.toString(), b.toString());
    }

    @Test
    public void testShardEquality() {
        final List<Dataset> normal = parseQueryDatasets("from organic yesterday today", QueryServletTestUtils.LanguageVersion.IQL2, null);
        Assert.assertEquals(24, normal.get(0).shards.size());

        final List<Dataset> nullShards = parseQueryDatasets("from organic yesterday today", QueryServletTestUtils.LanguageVersion.IQL2, new NullShardResolver());
        Assert.assertNull(nullShards.get(0).shards);
        assertDifferent(normal, nullShards);

        final List<Dataset> noShards = parseQueryDatasets("from organic yesterday today", QueryServletTestUtils.LanguageVersion.IQL2, new NoShardResolver());
        Assert.assertEquals(0, noShards.get(0).shards.size());
        assertDifferent(normal, noShards);
    }

    private static void assertDifferent(final List<Dataset> a, final List<Dataset> b) {
        Assert.assertFalse("Datasets should be different", a.equals(b));
        Assert.assertFalse("Dataset::toString should be different", a.toString().equals(b.toString()));
    }

    private static class ShufflingShardResolver implements ShardResolver {
        private final ShardResolver inner;

        private ShufflingShardResolver(final ShardResolver inner) {
            this.inner = inner;
        }

        @Nullable
        @Override
        public ShardResolutionResult resolve(final String dataset, final DateTime start, final DateTime end) {
            final ShardResolutionResult innerResult = inner.resolve(dataset, start, end);
            final ArrayList<Shard> shuffledShards = Lists.newArrayList(innerResult.shards);
            Collections.shuffle(shuffledShards, new Random(0));
            Preconditions.checkState(!Ordering.natural().isOrdered(shuffledShards));
            return new ShardResolutionResult(shuffledShards, innerResult.missingShardTimeIntervals);
        }
    }

    private static class NoShardResolver implements ShardResolver {
        public ShardResolutionResult resolve(final String dataset, final DateTime start, final DateTime end) {
            return new ShardResolutionResult(Collections.emptyList(), Collections.singletonList(new Interval(start, end)));
        }
    }
}