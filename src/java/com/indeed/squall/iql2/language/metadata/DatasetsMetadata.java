package com.indeed.squall.iql2.language.metadata;

import com.google.common.base.Optional;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 *
 */
public class DatasetsMetadata {
    private final Map<String, DatasetMetadata> metadata;
    private final Map<String, Set<String>> datasetToKeywordAnalyzerFields;
    private final Map<String, Set<String>> datasetToIntFields;
    private final Map<String, Map<String, String>> datasetToDimensionAliasFields;
    private static final DatasetsMetadata EMPTY_META = new DatasetsMetadata();

    private DatasetsMetadata() {
        metadata = Collections.emptyMap();
        datasetToKeywordAnalyzerFields = Collections.emptyMap();
        datasetToIntFields = Collections.emptyMap();
        datasetToDimensionAliasFields = Collections.emptyMap();
    }

    public DatasetsMetadata(final Map<String, Set<String>> datasetToKeywordAnalyzerFields,
                            final Map<String, Set<String>> datasetToIntFields) {
        this.datasetToKeywordAnalyzerFields = toCaseInsensitiveMap(datasetToKeywordAnalyzerFields);
        this.datasetToIntFields = toCaseInsensitiveMap(datasetToIntFields);
        this.datasetToDimensionAliasFields = Collections.emptyMap();
        this.metadata = Collections.emptyMap();
    }

    public DatasetsMetadata(final Map<String, DatasetMetadata> metadata) {
        this.metadata = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.metadata.putAll(metadata);
        datasetToKeywordAnalyzerFields = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        datasetToIntFields = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        datasetToDimensionAliasFields = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        metadata.forEach((dataset, meta) -> {
            datasetToKeywordAnalyzerFields.put(dataset, meta.keywordAnaylzerWhitelist);
            datasetToIntFields.put(dataset, meta.intFields);
            datasetToDimensionAliasFields.put(dataset, meta.fieldToDimension.entrySet()
                    .stream().filter(dimension -> dimension.getValue().isAlias)
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getAliasActualField().get())));
        });
    }

    public static DatasetsMetadata empty() {
        return EMPTY_META;
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

    public Map<String, Map<String, String>> getDatasetToDimensionAliasFields() {
        return datasetToDimensionAliasFields;
    }

    private Map<String, Set<String>> toCaseInsensitiveMap(final Map<String, Set<String>> map) {
        final Map<String, Set<String>> caseInsensitiveMap= new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
            final Set<String> caseInsensitiveSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            caseInsensitiveSet.addAll(entry.getValue());
            caseInsensitiveMap.put(entry.getKey(), caseInsensitiveSet);
        }
        return caseInsensitiveMap;
    }
}
