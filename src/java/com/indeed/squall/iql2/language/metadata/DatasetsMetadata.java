package com.indeed.squall.iql2.language.metadata;

import com.google.common.base.Optional;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 *
 */
public class DatasetsMetadata {
    private final Map<String, DatasetMetadata> metadata;
    private final Map<String, Map<String, String>> datasetToDimensionAliasFields;
    private static final DatasetsMetadata EMPTY_META = new DatasetsMetadata();

    private DatasetsMetadata() {
        metadata = Collections.emptyMap();
        datasetToDimensionAliasFields = Collections.emptyMap();
    }

    public DatasetsMetadata(final Map<String, DatasetMetadata> metadata) {
        this.metadata = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.metadata.putAll(metadata);
        datasetToDimensionAliasFields = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        metadata.forEach((dataset, meta) -> {
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

    public Map<String, Map<String, String>> getDatasetToDimensionAliasFields() {
        return datasetToDimensionAliasFields;
    }
}
