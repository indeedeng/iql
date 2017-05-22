package com.indeed.squall.iql2.language.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

public class DatasetsFields {
    private final Map<String, ImmutableSet<String>> datasetToIntFields;
    private final Map<String, ImmutableSet<String>> datasetToStringFields;
    private final Map<String, ImmutableSet<String>> datasetToNonAliasMetricFields;

    public DatasetsFields(
            Map<String, Set<String>> datasetToIntFields, Map<String, Set<String>> datasetToStringFields, Map<String, Set<String>> datasetToNonAliasMetricFields) {
        this.datasetToIntFields = copy(datasetToIntFields);
        this.datasetToStringFields = copy(datasetToStringFields);
        this.datasetToNonAliasMetricFields = copy(datasetToNonAliasMetricFields);
    }

    public ImmutableSet<String> getStringFields(String dataset) {
        if (!datasetToStringFields.containsKey(dataset)) {
            return ImmutableSet.of();
        } else {
            return datasetToStringFields.get(dataset);
        }
    }

    public ImmutableSet<String> getIntFields(String dataset) {
        if (!datasetToIntFields.containsKey(dataset)) {
            return ImmutableSet.of();
        } else {
            return datasetToIntFields.get(dataset);
        }
    }

    public ImmutableSet<String> getMetricFields(String dataset) {
        if (!datasetToNonAliasMetricFields.containsKey(dataset)) {
            return ImmutableSet.of();
        } else {
            return datasetToNonAliasMetricFields.get(dataset);
        }
    }

    public Set<String> getAllFields(String dataset) {
        return Sets.union(getStringFields(dataset), getIntFields(dataset));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builderFrom(DatasetsFields datasetsFields) {
        final Builder builder = new Builder();
        for (final Map.Entry<String, ImmutableSet<String>> entry : datasetsFields.datasetToIntFields.entrySet()) {
            for (final String field : entry.getValue()) {
                builder.addIntField(entry.getKey(), field);
            }
        }
        for (final Map.Entry<String, ImmutableSet<String>> entry : datasetsFields.datasetToStringFields.entrySet()) {
            for (final String field : entry.getValue()) {
                builder.addStringField(entry.getKey(), field);
            }
        }
        for (final Map.Entry<String, ImmutableSet<String>> entry : datasetsFields.datasetToNonAliasMetricFields.entrySet()) {
            for (final String field : entry.getValue()) {
                builder.addNonAliasMetricField(entry.getKey(), field);
            }
        }
        return builder;
    }

    public Set<String> datasets() {
        return Sets.union(datasetToIntFields.keySet(), datasetToStringFields.keySet());
    }

    public static class Builder {
        private final Map<String, Set<String>> datasetToIntFields = Maps.newHashMap();
        private final Map<String, Set<String>> datasetToStringFields = Maps.newHashMap();
        private final Map<String, Set<String>> datasetToNonAliasMetricFields = Maps.newHashMap();

        public void addIntField(String dataset, String field) {
            ensurePresent(dataset);
            datasetToIntFields.get(dataset).add(field);
        }

        public void addStringField(String dataset, String field) {
            ensurePresent(dataset);
            datasetToStringFields.get(dataset).add(field);
        }

        public void addNonAliasMetricField(String dataset, String field) {
            ensurePresent(dataset);
            datasetToNonAliasMetricFields.get(dataset).add(field);
        }

        private void ensurePresent(String dataset) {
            if (!datasetToIntFields.containsKey(dataset)) {
                datasetToIntFields.put(dataset, Sets.<String>newHashSet());
            }

            if (!datasetToStringFields.containsKey(dataset)) {
                datasetToStringFields.put(dataset, Sets.<String>newHashSet());
            }

            if (!datasetToNonAliasMetricFields.containsKey(dataset)) {
                datasetToNonAliasMetricFields.put(dataset, Sets.newHashSet());
            }
        }

        public DatasetsFields build() {
            return new DatasetsFields(datasetToIntFields, datasetToStringFields, datasetToNonAliasMetricFields);
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
