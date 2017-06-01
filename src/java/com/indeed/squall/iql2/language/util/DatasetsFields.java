package com.indeed.squall.iql2.language.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

public class DatasetsFields {
    private final Map<String, ImmutableSet<String>> uppercasedDatasetToIntFields;
    private final Map<String, ImmutableSet<String>> uppercasedDatasetToStringFields;
    private final Map<String, ImmutableSet<String>> uppercasedDatasetToNonAliasMetricFields;

    public DatasetsFields(
            Map<String, Set<String>> uppercasedDatasetToIntFields, Map<String, Set<String>> uppercasedDatasetToStringFields,
            Map<String, Set<String>> uppercasedDatasetToNonAliasMetricFields) {
        this.uppercasedDatasetToIntFields = copy(uppercasedDatasetToIntFields);
        this.uppercasedDatasetToStringFields = copy(uppercasedDatasetToStringFields);
        this.uppercasedDatasetToNonAliasMetricFields = copy(uppercasedDatasetToNonAliasMetricFields);
    }

    public ImmutableSet<String> getUppercasedStringFields(String dataset) {
        final String uppercasedDataset = dataset.toUpperCase();
        if (!uppercasedDatasetToStringFields.containsKey(uppercasedDataset)) {
            return ImmutableSet.of();
        } else {
            return uppercasedDatasetToStringFields.get(uppercasedDataset);
        }
    }

    public ImmutableSet<String> getUppercasedIntFields(String dataset) {
        final String uppercasedDataset = dataset.toUpperCase();
        if (!uppercasedDatasetToIntFields.containsKey(uppercasedDataset)) {
            return ImmutableSet.of();
        } else {
            return uppercasedDatasetToIntFields.get(uppercasedDataset);
        }
    }

    public boolean containsStringField(String dataset, String field) {
        final String uppercasedDataset = dataset.toUpperCase();
        if (!uppercasedDatasetToStringFields.containsKey(uppercasedDataset)) {
            return false;
        } else {
            return uppercasedDatasetToStringFields.get(uppercasedDataset).contains(field.toUpperCase());
        }
    }

    public boolean containsIntField(String dataset, String field) {
        final String uppercasedDataset = dataset.toUpperCase();
        if (!uppercasedDatasetToIntFields.containsKey(uppercasedDataset)) {
            return false;
        } else {
            return uppercasedDatasetToIntFields.get(uppercasedDataset).contains(field.toUpperCase());
        }
    }

    public boolean containsMetricField(String dataset, String field) {
        final String uppercasedDataset = dataset.toUpperCase();
        if (!uppercasedDatasetToNonAliasMetricFields.containsKey(uppercasedDataset)) {
            return false;
        } else {
            return uppercasedDatasetToNonAliasMetricFields.get(uppercasedDataset).contains(field.toUpperCase());
        }
    }

    // if field is in intFields or stringFields
    public boolean containsField(String dataset, String field) {
        return containsIntField(dataset, field) || containsStringField(dataset, field);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builderFrom(DatasetsFields datasetsFields) {
        final Builder builder = new Builder();
        for (final Map.Entry<String, ImmutableSet<String>> entry : datasetsFields.uppercasedDatasetToIntFields.entrySet()) {
            for (final String field : entry.getValue()) {
                builder.addIntField(entry.getKey(), field);
            }
        }
        for (final Map.Entry<String, ImmutableSet<String>> entry : datasetsFields.uppercasedDatasetToStringFields.entrySet()) {
            for (final String field : entry.getValue()) {
                builder.addStringField(entry.getKey(), field);
            }
        }
        for (final Map.Entry<String, ImmutableSet<String>> entry : datasetsFields.uppercasedDatasetToNonAliasMetricFields.entrySet()) {
            for (final String field : entry.getValue()) {
                builder.addNonAliasMetricField(entry.getKey(), field);
            }
        }
        return builder;
    }

    public Set<String> uppercasedDatasets() {
        return Sets.union(uppercasedDatasetToIntFields.keySet(), uppercasedDatasetToStringFields.keySet());
    }

    public static class Builder {
        private final Map<String, Set<String>> uppercasedDatasetToIntFields = Maps.newHashMap();
        private final Map<String, Set<String>> uppercasedDatasetToStringFields = Maps.newHashMap();
        private final Map<String, Set<String>> uppercasedDatasetToNonAliasMetricFields = Maps.newHashMap();

        public void addIntField(String dataset, String field) {
            final String uppercasedDataset = dataset.toUpperCase();
            ensurePresent(uppercasedDataset);
            uppercasedDatasetToIntFields.get(uppercasedDataset).add(field.toUpperCase());
        }

        public void addStringField(String dataset, String field) {
            final String uppercasedDataset = dataset.toUpperCase();
            ensurePresent(uppercasedDataset);
            uppercasedDatasetToStringFields.get(uppercasedDataset).add(field.toUpperCase());
        }

        public void addNonAliasMetricField(String dataset, String field) {
            final String uppercasedDataset = dataset.toUpperCase();
            ensurePresent(dataset);
            uppercasedDatasetToNonAliasMetricFields.get(uppercasedDataset).add(field.toUpperCase());
        }

        private void ensurePresent(String dataset) {
            final String uppercasedDataset = dataset.toUpperCase();
            if (!uppercasedDatasetToIntFields.containsKey(uppercasedDataset)) {
                uppercasedDatasetToIntFields.put(uppercasedDataset, Sets.<String>newHashSet());
            }

            if (!uppercasedDatasetToStringFields.containsKey(uppercasedDataset)) {
                uppercasedDatasetToStringFields.put(uppercasedDataset, Sets.<String>newHashSet());
            }

            if (!uppercasedDatasetToNonAliasMetricFields.containsKey(uppercasedDataset)) {
                uppercasedDatasetToNonAliasMetricFields.put(uppercasedDataset, Sets.newHashSet());
            }
        }

        public DatasetsFields build() {
            return new DatasetsFields(uppercasedDatasetToIntFields, uppercasedDatasetToStringFields, uppercasedDatasetToNonAliasMetricFields);
        }
    }

    private static ImmutableMap<String, ImmutableSet<String>> copy(Map<String, Set<String>> m) {
        ImmutableMap.Builder<String, ImmutableSet<String>> builder = ImmutableMap.builder();
        for (final Map.Entry<String, Set<String>> entry : m.entrySet()) {
            builder.put(entry.getKey(), ImmutableSet.copyOf(entry.getValue()));
        }
        return builder.build();
    }
}
