package com.indeed.imhotep.client;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.indeed.imhotep.AbstractImhotepMultiSession;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.local.ImhotepJavaLocalSession;
import com.indeed.imhotep.local.ImhotepLocalSession;
import com.indeed.squall.iql2.server.web.servlets.dataset.Shard;
import org.joda.time.DateTime;
import org.junit.Ignore;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

@Ignore
public class TestImhotepClient extends ImhotepClient {
    private final List<Shard> shards;

    public TestImhotepClient(List<Shard> shards) {
        super(new DummyHostsReloader(Collections.<Host>emptyList()));
        this.shards = shards;

        final Map<String, List<ShardInfo>> datasetToShardInfos = new HashMap<>();
        final Map<String, Set<String>> datasetToIntFields = new HashMap<>();
        final Map<String, Set<String>> datasetToStringFields = new HashMap<>();
        for (final Shard shard : shards) {
            final String dataset = shard.dataset;
            if (!datasetToShardInfos.containsKey(dataset)) {
                datasetToShardInfos.put(dataset, new ArrayList<ShardInfo>());
                datasetToIntFields.put(dataset, new HashSet<String>());
                datasetToStringFields.put(dataset, new HashSet<String>());
            }
            // TODO: Not hardcode version to 2015-01-01 00:00:00?
            datasetToShardInfos.get(dataset).add(new ShardInfo(shard.shardId, shard.flamdex.getNumDocs(), 20150101000000L));
            datasetToIntFields.get(dataset).addAll(shard.flamdex.getIntFields());
            datasetToStringFields.get(dataset).addAll(shard.flamdex.getStringFields());
        }

        final ArrayList<DatasetInfo> datasetInfos = new ArrayList<>();
        for (final Map.Entry<String, List<ShardInfo>> entry : datasetToShardInfos.entrySet()) {
            final String dataset = entry.getKey();
            final List<ShardInfo> shardInfos = entry.getValue();
            datasetInfos.add(new DatasetInfo(dataset, shardInfos, datasetToIntFields.get(dataset), datasetToStringFields.get(dataset)));
        }

        final Map<Host, List<DatasetInfo>> hostToDatasetInfos = ImmutableMap.<Host, List<DatasetInfo>>of(new Host("", 1), datasetInfos);


        final Field shardListReloader;
        try {
            shardListReloader = ImhotepClient.class.getDeclaredField("shardListReloader");
            shardListReloader.setAccessible(true);

            final Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(shardListReloader, shardListReloader.getModifiers() & ~Modifier.FINAL);

            shardListReloader.set(this, new DumbImhotepClientShardListReloader(hostToDatasetInfos));
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public SessionBuilder sessionBuilder(final String dataset, DateTime start, DateTime end) {
        return new SessionBuilder(dataset, start, end) {
            private List<String> readShardsOverride() {
                try {
                    final Field shardsOverrideField = SessionBuilder.class.getDeclaredField("shardsOverride");
                    shardsOverrideField.setAccessible(true);
                    return (List<String>) shardsOverrideField.get(this);
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    throw Throwables.propagate(e);
                }
            }

            @Override
            public ImhotepSession build() {
                final List<String> shardsOverride = readShardsOverride();
                final List<String> shardIds = shardsOverride != null ? shardsOverride : ShardIdWithVersion.keepShardIds(this.getChosenShards());

                final List<ImhotepLocalSession> sessions = new ArrayList<>();

                for (final Shard shard : TestImhotepClient.this.shards) {
                    if (shardIds.contains(shard.shardId) && shard.dataset.equals(dataset)) {
                        try {
                            sessions.add(new ImhotepJavaLocalSession(shard.flamdex));
                        } catch (ImhotepOutOfMemoryException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                }

                return new AbstractImhotepMultiSession(sessions.toArray(new ImhotepSession[sessions.size()])) {
                    @Override
                    protected void postClose() {

                    }

                    @Override
                    public void writeFTGSIteratorSplit(String[] intFields, String[] stringFields, int splitIndex, int numSplits, long termLimit, Socket socket) throws ImhotepOutOfMemoryException {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    protected ImhotepRemoteSession createImhotepRemoteSession(InetSocketAddress address, String sessionId, AtomicLong tempFileSizeBytesLeft) {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }
}
