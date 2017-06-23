package com.indeed.squall.iql2.language.util;

import com.indeed.squall.iql2.language.dimensions.Dimension;
import com.indeed.squall.iql2.language.metadata.DatasetsMetadata;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class DatasetsFields {
    private final DatasetsMetadata datasetsMetadata;
    private final Map<String, Set<String>> datasetAliasIntFields;
    private final Map<String, Set<String>> datasetAliasStringFields;

    public DatasetsFields(final DatasetsMetadata datasetsMetadata,
                          final Map<String, Set<String>> datasetAliasIntFields,
                          final Map<String, Set<String>> datasetAliasStringFields) {
        this.datasetsMetadata = datasetsMetadata;
        this.datasetAliasIntFields = toCaseInsensitive(datasetAliasIntFields);
        this.datasetAliasStringFields = toCaseInsensitive(datasetAliasStringFields);
    }

    private Map<String, Set<String>> toCaseInsensitive(final Map<String, Set<String>> map) {
        final Map<String, Set<String>> caseInsensitiveMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
            caseInsensitiveMap.put(entry.getKey(), new TreeSet<>(String.CASE_INSENSITIVE_ORDER));
            caseInsensitiveMap.get(entry.getKey()).addAll(entry.getValue());
        }
        return caseInsensitiveMap;
    }


    public boolean containsStringField(String dataset, String field) {
        return (datasetsMetadata.getMetadata(dataset).isPresent() && datasetsMetadata.getMetadata(dataset).get().stringFields.contains(field)) ||
                (datasetAliasStringFields.containsKey(dataset) && datasetAliasStringFields.get(dataset).contains(field));
    }

    public boolean containsIntField(String dataset, String field) {
        return (datasetsMetadata.getMetadata(dataset).isPresent() && datasetsMetadata.getMetadata(dataset).get().intFields.contains(field)) ||
                (datasetAliasIntFields.containsKey(dataset) && datasetAliasIntFields.get(dataset).contains(field));
    }

    public boolean containsIntOrAliasField(String dataset, String field) {
        return containsIntField(dataset, field) || containsAliasMetricField(dataset, field);
    }

    public boolean containsMetricField(String dataset, String field) {
        return getDimension(dataset, field).isPresent();
    }

    public boolean containsNonAliasMetricField(String dataset, String field) {
        final Optional<Dimension> dimension = getDimension(dataset, field);
        return dimension.isPresent() && !dimension.get().isAlias;
    }

    public boolean containsAliasMetricField(String dataset, String field) {
        final Optional<Dimension> dimension = getDimension(dataset, field);
        return dimension.isPresent() && dimension.get().isAlias;
    }

    // if field is in intFields or stringFields
    public boolean containsField(String dataset, String field) {
        return containsIntField(dataset, field) || containsStringField(dataset, field) || containsAliasMetricField(dataset, field);
    }

    private Optional<Dimension> getDimension(String dataset, String field) {
        if (datasetsMetadata.getMetadata(dataset).isPresent() &&
                datasetsMetadata.getMetadata(dataset).get().fieldToDimension.containsKey(field)) {
            return Optional.of(datasetsMetadata.getMetadata(dataset).get().fieldToDimension.get(field));
        } else {
            return Optional.empty();
        }
    }

    public Set<String> datasets() {
        return datasetsMetadata.getDatasetToMetadata().keySet();
    }
}
