package com.indeed.iql2.language.query.shardresolution;

import com.google.common.collect.ImmutableList;
import com.indeed.imhotep.Shard;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;

public interface ShardResolver {
    @Nullable
    ShardResolutionResult resolve(
            final String dataset,
            final DateTime start,
            final DateTime end
    );

    class ShardResolutionResult {
        public final List<Shard> shards;
        public final List<Interval> missingShardTimeIntervals;

        public ShardResolutionResult(final List<Shard> shards, final List<Interval> missingShardTimeIntervals) {
            this.shards = shards;
            this.missingShardTimeIntervals = missingShardTimeIntervals;
        }
    }
}
