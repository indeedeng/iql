package com.indeed.iql2.language.query.shardresolution;

import com.indeed.imhotep.Shard;
import com.indeed.imhotep.client.ImhotepClient;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;

public class ImhotepClientShardResolver implements ShardResolver {
    private final ImhotepClient imhotepClient;

    public ImhotepClientShardResolver(final ImhotepClient imhotepClient) {
        this.imhotepClient = imhotepClient;
    }

    @Override
    public ShardResolutionResult resolve(final String dataset, final DateTime start, final DateTime end) {
        final ImhotepClient.SessionBuilder sessionBuilder = imhotepClient.sessionBuilder(dataset, start, end);
        final List<Shard> chosenShards = sessionBuilder.getChosenShards();
        final List<Interval> missingShards = sessionBuilder.getTimeIntervalsMissingShards();
        return new ShardResolutionResult(chosenShards, missingShards);
    }
}
