package com.indeed.iql2.language.query.shardresolution;

import org.joda.time.DateTime;

import javax.annotation.Nullable;

public class NullShardResolver implements ShardResolver {
    @Override
    @Nullable
    public ShardResolutionResult resolve(final String dataset, final DateTime start, final DateTime end) {
        return null;
    }
}
