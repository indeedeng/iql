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

    private DatasetsMetadata() {
        metadata = Collections.emptyMap();
    }

    public DatasetsMetadata(final Map<String, DatasetMetadata> metadata) {
        this.metadata = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.metadata.putAll(metadata);
    }

    public static DatasetsMetadata empty() {
        return new DatasetsMetadata();
    }

    public Map<String, DatasetMetadata> getDatasetToMetadata() {
        return metadata;
    }

    public Optional<DatasetMetadata> getMetadata(final String dataset) {
        return Optional.fromNullable(metadata.get(dataset));
    }


    public Map<String, Set<String>> getDatasetToKeywordAnalyzerFields() {
        final Map<String, Set<String>> keywords = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        metadata.forEach((dataset, metadata) -> keywords.put(dataset, metadata.keywordAnaylzerWhitelist));
        return keywords;
    }

    public Map<String, Set<String>> getDatasetToIntFields() {
        final Map<String, Set<String>> intFields = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        metadata.forEach((dataset, metadata) -> intFields.put(dataset, metadata.intFields));
        return intFields;
    }
}
