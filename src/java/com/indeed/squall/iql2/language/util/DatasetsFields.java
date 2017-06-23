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
    private final Map<String, ImmutableSet<String>> uppercasedDatasetToAliasMetricFields;

    public DatasetsFields(
            Map<String, Set<String>> uppercasedDatasetToIntFields, Map<String, Set<String>> uppercasedDatasetToStringFields,
            Map<String, Set<String>> uppercasedDatasetToAliasMetricFields, Map<String, Set<String>> uppercasedDatasetToNonAliasMetricFields) {
        this.uppercasedDatasetToIntFields = copy(uppercasedDatasetToIntFields);
        this.uppercasedDatasetToStringFields = copy(uppercasedDatasetToStringFields);
        this.uppercasedDatasetToAliasMetricFields = copy(uppercasedDatasetToAliasMetricFields);
        this.uppercasedDatasetToNonAliasMetricFields = copy(uppercasedDatasetToNonAliasMetricFields);
    }

    public ImmutableSet<String> getUppercasedStringFields(String dataset) {
        return uppercasedDatasetToStringFields.getOrDefault(dataset.toUpperCase(), ImmutableSet.of());
    }

    public ImmutableSet<String> getUppercasedIntFields(String dataset) {
        return uppercasedDatasetToIntFields.getOrDefault(dataset.toUpperCase(), ImmutableSet.of());
    }

    public boolean containsStringField(String dataset, String field) {
       return getUppercasedStringFields(dataset).contains(field.toUpperCase());
    }

    public boolean containsIntField(String dataset, String field) {
        return getUppercasedIntFields(dataset).contains(field.toUpperCase());
    }

    public boolean containsIntOrAliasField(String dataset, String field) {
        return containsIntField(dataset, field) || containsAliasMetricField(dataset, field);
    }

    public boolean containsMetricField(String dataset, String field) {
        return containsNonAliasMetricField(dataset, field) || containsAliasMetricField(dataset, field);
    }

    public boolean containsNonAliasMetricField(String dataset, String field) {
        return uppercasedDatasetToNonAliasMetricFields.getOrDefault(dataset.toUpperCase(), ImmutableSet.of()).contains(field.toUpperCase());
    }

    public boolean containsAliasMetricField(String dataset, String field) {
        return uppercasedDatasetToAliasMetricFields.getOrDefault(dataset.toUpperCase(), ImmutableSet.of()).contains(field.toUpperCase());
    }


    // if field is in intFields or stringFields
    public boolean containsField(String dataset, String field) {
        return containsIntField(dataset, field) || containsStringField(dataset, field) || containsAliasMetricField(dataset, field);
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
        for (final Map.Entry<String, ImmutableSet<String>> entry : datasetsFields.uppercasedDatasetToAliasMetricFields.entrySet()) {
            for (final String field : entry.getValue()) {
                builder.addMetricField(entry.getKey(), field, true);
            }
        }
        for (final Map.Entry<String, ImmutableSet<String>> entry : datasetsFields.uppercasedDatasetToNonAliasMetricFields.entrySet()) {
            for (final String field : entry.getValue()) {
                builder.addMetricField(entry.getKey(), field, false);
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
        private final Map<String, Set<String>> uppercasedDatasetToAliasMetricFields = Maps.newHashMap();

        public void addIntField(String dataset, String field) {
            ensurePresent(dataset);
            uppercasedDatasetToIntFields.get(dataset.toUpperCase()).add(field.toUpperCase());
        }

        public void addStringField(String dataset, String field) {
            ensurePresent(dataset);
            uppercasedDatasetToStringFields.get(dataset.toUpperCase()).add(field.toUpperCase());
        }

        public void addMetricField(String dataset, String field, boolean isAlias) {
            ensurePresent(dataset);
            if (isAlias) {
                uppercasedDatasetToAliasMetricFields.get(dataset.toUpperCase()).add(field.toUpperCase());
            } else {
                uppercasedDatasetToNonAliasMetricFields.get(dataset.toUpperCase()).add(field.toUpperCase());
            }
        }

        private void ensurePresent(String dataset) {
            final String uppercasedDataset = dataset.toUpperCase();
            if (!uppercasedDatasetToIntFields.containsKey(uppercasedDataset)) {
                uppercasedDatasetToIntFields.put(uppercasedDataset, Sets.<String>newHashSet());
            }

            if (!uppercasedDatasetToStringFields.containsKey(uppercasedDataset)) {
                uppercasedDatasetToStringFields.put(uppercasedDataset, Sets.<String>newHashSet());
            }

            if (!uppercasedDatasetToAliasMetricFields.containsKey(uppercasedDataset)) {
                uppercasedDatasetToAliasMetricFields.put(uppercasedDataset, Sets.newHashSet());
            }

            if (!uppercasedDatasetToNonAliasMetricFields.containsKey(uppercasedDataset)) {
                uppercasedDatasetToNonAliasMetricFields.put(uppercasedDataset, Sets.newHashSet());
            }
        }

        public DatasetsFields build() {
            return new DatasetsFields(uppercasedDatasetToIntFields, uppercasedDatasetToStringFields,
                    uppercasedDatasetToAliasMetricFields, uppercasedDatasetToNonAliasMetricFields);
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
