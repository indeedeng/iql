package com.indeed.squall.iql2.language.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

public class DatasetsFields {
    public final ImmutableMap<String, ImmutableSet<String>> datasetToIntFields;
    public final ImmutableMap<String, ImmutableSet<String>> datasetToStringFields;

    public DatasetsFields(Map<String, Set<String>> datasetToIntFields, Map<String, Set<String>> datasetToStringFields) {
        this.datasetToIntFields = copy(datasetToIntFields);
        this.datasetToStringFields = copy(datasetToStringFields);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final Map<String, Set<String>> datasetToIntFields = Maps.newHashMap();
        private final Map<String, Set<String>> datasetToStringFields = Maps.newHashMap();

        public void addIntField(String dataset, String field) {
            ensurePresent(dataset);
            datasetToIntFields.get(dataset).add(field);
        }

        public void addStringField(String dataset, String field) {
            ensurePresent(dataset);
            datasetToStringFields.get(dataset).add(field);
        }

        private void ensurePresent(String dataset) {
            if (!datasetToIntFields.containsKey(dataset)) {
                datasetToIntFields.put(dataset, Sets.<String>newHashSet());
            }

            if (!datasetToStringFields.containsKey(dataset)) {
                datasetToStringFields.put(dataset, Sets.<String>newHashSet());
            }
        }

        public DatasetsFields build() {
            return new DatasetsFields(datasetToIntFields, datasetToStringFields);
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
