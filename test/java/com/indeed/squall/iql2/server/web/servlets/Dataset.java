package com.indeed.squall.iql2.server.web.servlets;

import com.google.common.collect.Lists;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.ims.client.DatasetInterface;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.ims.client.yamlFile.DatasetYaml;
import com.indeed.ims.client.yamlFile.MetricsYaml;
import it.unimi.dsi.fastutil.longs.LongList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class Dataset {

    public final List<DatasetShard> shards;
    private final ImsClientInterface aliasImsClient;

    private static final String ALIAS_PREFIX = "_ALIAS_";

    protected Dataset(List<DatasetShard> shards) {
        this.shards = shards;
        aliasImsClient = new AliasDimensionClient(getShards());
    }

    public ImsClientInterface getAliasImsClient() {
        return aliasImsClient;
    }

    public List<Shard> getShards() {
        return flamdexShards(false);
    }

    public List<Shard> getDimensionFields() {
        return flamdexShards(true);
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

    static class DatasetShard {
        final String dataset;
        final String shardId;
        final DatasetFlamdex flamdex;

        public DatasetShard(final String dataset, final String shardId, final DatasetFlamdex flamdex) {
            this.dataset = dataset;
            this.shardId = shardId;
            this.flamdex = flamdex;
        }
    }

    static class DatasetFlamdex {
        private final List<FlamdexDocument> originDocuments;
        private final List<FlamdexDocument> dimensionDocuments;

        DatasetFlamdex() {
            originDocuments = Lists.newArrayList();
            dimensionDocuments =  Lists.newArrayList();
        }

        DatasetFlamdex addDocument(FlamdexDocument doc) {
            originDocuments.add(doc);
            dimensionDocuments.add(makeDimensionDocument(doc));
            return this;
        }

        private FlamdexDocument makeDimensionDocument(FlamdexDocument doc) {
            final FlamdexDocument aliasDoc = new FlamdexDocument();
            for (Map.Entry<String, LongList> intFieldEntry : doc.getIntFields().entrySet()) {
                if (intFieldEntry.getKey().equals("unixtime")) {
                    aliasDoc.addIntTerms(intFieldEntry.getKey(), intFieldEntry.getValue());
                } else {
                    aliasDoc.addIntTerms(ALIAS_PREFIX + intFieldEntry.getKey(), intFieldEntry.getValue());
                }
            }

            for (Map.Entry<String, List<String>> stringFieldEntry : doc.getStringFields().entrySet()) {
                aliasDoc.addStringTerms(stringFieldEntry.getKey(), stringFieldEntry.getValue());
            }
            return aliasDoc;
        }
    }

    private class AliasDimensionClient implements ImsClientInterface {
        private final Map<String, DatasetYaml> datasetMap;
        public AliasDimensionClient(final List<Shard> shards) {
            datasetMap = new HashMap<>();
            for (final Shard shard : shards) {
                if (datasetMap.containsKey(shard.dataset)) {
                    continue;
                }
                DatasetYaml dataset = new DatasetYaml();
                dataset.setType("Imhotep");
                dataset.setName(shard.dataset);

                List<MetricsYaml> metrics = Lists.newArrayList();
                final MemoryFlamdex flamdex = shard.flamdex;
                for (String intField : flamdex.getIntFields()) {
                    final MetricsYaml metric = new MetricsYaml();
                    metric.setName(intField);
                    metric.setExpr(ALIAS_PREFIX+intField+"+0");
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
        public Set<String> getKeywordAnalyzerWhitelist(final String s) {
            return new HashSet<>();
        }

        @Override
        public Map<String, Set<String>> getWhitelist() {
            return new HashMap<>();
        }

        @Override
        public DatasetInterface getDataset(final String s) {
            throw new UnsupportedOperationException("You need to implement this");
        }
    }
}
