package com.indeed.squall.iql2.language.metadata;

import com.google.common.base.Optional;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 *
 */
public class DatasetsMetadata {
    private final Map<String, DatasetMetadata> metadata;
    private final Map<String, Set<String>> datasetToKeywordAnalyzerFields;
    private final Map<String, Set<String>> datasetToIntFields;
    private static final DatasetsMetadata emptyDatasets = new DatasetsMetadata();

    private DatasetsMetadata() {
        metadata = Collections.emptyMap();
        datasetToKeywordAnalyzerFields = Collections.emptyMap();
        datasetToIntFields = Collections.emptyMap();
    }

    public DatasetsMetadata(final Map<String, DatasetMetadata> metadata) {
        this.metadata = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.metadata.putAll(metadata);
        datasetToKeywordAnalyzerFields = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        datasetToIntFields = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        metadata.forEach((dataset, meta) -> {
            datasetToKeywordAnalyzerFields.put(dataset, meta.keywordAnaylzerWhitelist);
            datasetToIntFields.put(dataset, meta.intFields);
        });
    }

    public static DatasetsMetadata empty() {
        return emptyDatasets;
    }

    public Map<String, DatasetMetadata> getDatasetToMetadata() {
        return metadata;
    }

    public Optional<DatasetMetadata> getMetadata(final String dataset) {
        return Optional.fromNullable(metadata.get(dataset));
    }


    public Map<String, Set<String>> getDatasetToKeywordAnalyzerFields() {
        return datasetToKeywordAnalyzerFields;
    }

    public Map<String, Set<String>> getDatasetToIntFields() {
        return datasetToIntFields;
    }
}
