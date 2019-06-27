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

package com.indeed.iql2.server.web.servlets.dataset;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.service.ShardMasterAndImhotepDaemonClusterRunner;
import com.indeed.ims.client.DatasetInterface;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.ims.client.yamlFile.DatasetYaml;
import com.indeed.ims.client.yamlFile.MetricsYaml;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import com.indeed.iql.web.FieldFrequencyCache;
import it.unimi.dsi.fastutil.longs.LongList;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * dataset for normal and dimension shards
 * for dimension shards will do a mapping:
 1. name those int fields as DIMENSION_field_name.
 2. create dimensions which map field_name = DIMENSION_field_name + 0
 * Usage:
 * for queries that require FTGS on int fields, should use the normal shards, eg: testAll(dataset.getShards(), query)
 * for other queries use: eg: testAll(dataset, query), it will check the normal shards and dimension shards
 */
public class Dataset {
    static final String DIMENSION_SHARDMASTER = "DIMENSION_SHARDMASTER";
    static final String NORMAL_SHARDMASTER = "NORMAL_SHARDMASTER";

    public final List<DatasetShard> shards;
    private final ImsClientInterface dimensionImsClient;

    private static final String DIMENSION_PREFIX = "_DIMENSION_";

    private List<Shard> normalShards;
    private List<Shard> dimensionShards;
    private DatasetsMetadata datasetsMetada;

    Dataset(List<DatasetShard> shards) {
        this.shards = shards;
        dimensionImsClient = new AliasDimensionClient(getShards());
    }

    public DatasetsMetadata getDatasetsMetadata() {
        if (datasetsMetada == null) {
            final ImhotepMetadataCache metadataCache = new ImhotepMetadataCache(null, getNormalClient(), "", new FieldFrequencyCache(null));
            metadataCache.updateDatasets();
            datasetsMetada = metadataCache.get();
        }
        return datasetsMetada;
    }

    public ImsClientInterface getDimensionImsClient() {
        return dimensionImsClient;
    }

    public List<Shard> getShards() {
        if (normalShards == null) {
            normalShards = flamdexShards(false);
        }
        return normalShards;
    }

    public List<Shard> getDimensionShards() {
        if (dimensionShards == null) {
            dimensionShards = flamdexShards(true);
        }
        return dimensionShards;
    }

    private static int hash(final List<Shard> shards) {
        return shards.stream()
                .sorted(Comparator.comparing((Shard x) -> x.dataset).thenComparing(x -> x.shardId))
                .map(s -> s.dataset + " " + s.shardId + " " + s.flamdex.hashCode())
                .collect(Collectors.joining())
                .hashCode();
    }

    private ImhotepClient normalClient;

    public ImhotepClient getNormalClient() {
        if (normalClient == null) {
            final String propertiesShardmaster = System.getProperty(NORMAL_SHARDMASTER);
            if (propertiesShardmaster != null) {
                normalClient = new ImhotepClient(parseHostStrings(propertiesShardmaster));
            } else {
                try {
                    normalClient = makeClient(getShards());
                } catch (final Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        }
        return normalClient;
    }

    private static List<Host> parseHostStrings(final String raw) {
        final String[] hostStrings = raw.split(",");
        final List<Host> hosts = Arrays.stream(hostStrings)
                .map(x -> {
                    final String[] split = x.split(":");
                    final String hostname = split[0];
                    final int port = Integer.parseInt(split[1]);
                    return new Host(hostname, port);
                })
                .collect(Collectors.toList());
        return hosts;
    }

    private ImhotepClient dimensionsClient;

    public ImhotepClient getDimensionsClient() {
        if (dimensionsClient == null) {
            final String propertiesShardmaster = System.getProperty(DIMENSION_SHARDMASTER);
            if (propertiesShardmaster != null) {
                dimensionsClient = new ImhotepClient(parseHostStrings(propertiesShardmaster));
            } else {
                try {
                    dimensionsClient = makeClient(getDimensionShards());
                } catch (final Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        }
        return dimensionsClient;
    }

    private static ImhotepClient makeClient(final List<Shard> shards) throws InterruptedException, IOException, TimeoutException {
        // directory representing $PROJECT/target/
        final File targetDir = new File(Dataset.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParentFile();
        final Path tempDir = targetDir.toPath().resolve("iql_test_shardmaster_" + hash(shards));
        final boolean aleadyExisted = tempDir.toFile().exists();
        final ShardMasterAndImhotepDaemonClusterRunner cluster = new ShardMasterAndImhotepDaemonClusterRunner(tempDir.resolve("shards"), tempDir);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                cluster.stop();
            } catch (final IOException e) {
                throw Throwables.propagate(e);
            }
        }));
        if (!aleadyExisted) {
            for (final Shard shard : shards) {
                shard.addTo(cluster);
            }
        }
        // TODO: is this a good idea?
        cluster.startDaemon();
        cluster.startDaemon();
        cluster.startDaemon();
        final ImhotepClient client = cluster.createClient();
        return client;
    }

    private List<Shard> flamdexShards(boolean isDimension) {
        final List<Shard> flamdexShards = Lists.newArrayList();
        for (final DatasetShard shard : shards) {
            final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();
            final List<FlamdexDocument> documents;
            if (isDimension) {
                documents = shard.flamdex.dimensionDocuments;
            } else {
                documents = shard.flamdex.originDocuments;
            }
            documents.forEach(memoryFlamdex::addDocument);
            flamdexShards.add(new Shard(shard.dataset, shard.shardId, memoryFlamdex));
        }
        return flamdexShards;
    }

    public static class DatasetShard {
        final String dataset;
        final String shardId;
        final DatasetFlamdex flamdex;

        public DatasetShard(final String dataset, final String shardId, final DatasetFlamdex flamdex) {
            this.dataset = dataset;
            this.shardId = shardId;
            this.flamdex = flamdex;
        }
    }

    public static class DatasetFlamdex {
        private final List<FlamdexDocument> originDocuments;
        private final List<FlamdexDocument> dimensionDocuments;

        public DatasetFlamdex() {
            originDocuments = Lists.newArrayList();
            dimensionDocuments =  Lists.newArrayList();
        }

        public DatasetFlamdex addDocument(FlamdexDocument doc) {
            originDocuments.add(doc);
            dimensionDocuments.add(makeDimensionDocument(doc));
            return this;
        }

        private FlamdexDocument makeDimensionDocument(FlamdexDocument doc) {
            final FlamdexDocument dimensionDoc = new FlamdexDocument();
            for (Map.Entry<String, LongList> intFieldEntry : doc.getIntFields().entrySet()) {
                if (intFieldEntry.getKey().equals("unixtime")) {
                    dimensionDoc.addIntTerms(intFieldEntry.getKey(), intFieldEntry.getValue());
                } else {
                    dimensionDoc.addIntTerms(DIMENSION_PREFIX + intFieldEntry.getKey(), intFieldEntry.getValue());
                }
            }

            for (Map.Entry<String, List<String>> stringFieldEntry : doc.getStringFields().entrySet()) {
                dimensionDoc.addStringTerms(stringFieldEntry.getKey(), stringFieldEntry.getValue());
            }
            return dimensionDoc;
        }
    }

    private class AliasDimensionClient implements ImsClientInterface {
        private final Map<String, DatasetYaml> datasetMap;
        AliasDimensionClient(final List<Shard> shards) {
            datasetMap = new HashMap<>();
            for (final Shard shard : shards) {
                if (datasetMap.containsKey(shard.dataset)) {
                    continue;
                }
                DatasetYaml dataset = new DatasetYaml();
                dataset.setName(shard.dataset);

                List<MetricsYaml> metrics = Lists.newArrayList();
                final MemoryFlamdex flamdex = shard.flamdex;
                for (String intField : flamdex.getIntFields()) {
                    if (intField.equals("unixtime")) {
                        continue;
                    }
                    final MetricsYaml metric = new MetricsYaml();
                    metric.setName(intField);
                    metric.setExpr(DIMENSION_PREFIX +intField+"+0");
                    metrics.add(metric);
                }
                dataset.setMetrics(metrics.toArray(new MetricsYaml[metrics.size()]));
                datasetMap.put(shard.dataset, dataset);
            }
        }
        @Override
        public DatasetYaml[] getDatasets() {
            return datasetMap.values().toArray(new DatasetYaml[datasetMap.size()]);
        }

        @Override
        public DatasetInterface getDataset(final String s) {
            throw new UnsupportedOperationException("You need to implement this");
        }
    }
}
