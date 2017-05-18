package com.indeed.squall.iql2.language.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.squall.iql2.language.dimensions.Dimension;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class DatasetsFields {
    private final Map<String, ImmutableSet<String>> datasetToIntFields;
    private final Map<String, ImmutableSet<String>> datasetToStringFields;
    private final Map<String, ImmutableSet<Dimension>> datasetToMetricFields;

    public DatasetsFields(
            Map<String, Set<String>> datasetToIntFields, Map<String, Set<String>> datasetToStringFields,
            Map<String, Set<Dimension>> datasetToMetricFields) {
        this.datasetToIntFields = copy(datasetToIntFields);
        this.datasetToStringFields = copy(datasetToStringFields);
        this.datasetToMetricFields = copyDimension(datasetToMetricFields);
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

    public ImmutableSet<String> getIntAndAliasFields(String dataset) {
        if (!datasetToIntFields.containsKey(dataset) && getMetricFields(dataset, MetricFieldsType.ALIAS).isEmpty()) {
            return ImmutableSet.of();
        } else {
            return Sets.union(datasetToIntFields.get(dataset), getMetricFields(dataset, MetricFieldsType.ALIAS)).immutableCopy();
        }
    }

    public ImmutableSet<String> getMetricFields(String dataset, MetricFieldsType type) {
        if (!datasetToMetricFields.containsKey(dataset)) {
            return ImmutableSet.of();
        } else {
            return ImmutableSet.copyOf(datasetToMetricFields.get(dataset).stream().filter(dimension -> {
                if (type == MetricFieldsType.ALL) {
                    return true;
                } else if (type == MetricFieldsType.ALIAS) {
                    return dimension.isAlias;
                } else {
                    return !dimension.isAlias;
                }
            }).map(dimension -> dimension.name.toUpperCase()).collect(Collectors.toSet()));
        }
    }

    public Optional<String> getMetricExpression(String dataset, String metric) {
        if (datasetToMetricFields.containsKey(dataset)) {
            final Optional<Dimension> dimension = datasetToMetricFields.get(dataset).stream().filter(d -> d.name.equalsIgnoreCase(metric)).findAny();
            if (dimension.isPresent()) {
                return Optional.of(dimension.get().expression);
            }
        }
        return Optional.empty();
    }

    public Set<String> getAllFields(String dataset) {
        return getAllFields(dataset, MetricFieldsType.ALL);
    }

    public Set<String> getAllFields(String dataset, MetricFieldsType type) {
        return Sets.union(
                Sets.union(getStringFields(dataset), getIntFields(dataset)),
                getMetricFields(dataset, type));
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
        for (Map.Entry<String, ImmutableSet<Dimension>> entry : datasetsFields.datasetToMetricFields.entrySet()) {
            for (Dimension dimension : entry.getValue()) {
                builder.addMetricField(entry.getKey(), dimension);
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
        private final Map<String, Set<Dimension>> datasetToMetricFields = Maps.newHashMap();

        public void addIntField(String dataset, String field) {
            ensurePresent(dataset);
            datasetToIntFields.get(dataset).add(field);
        }

        public void addStringField(String dataset, String field) {
            ensurePresent(dataset);
            datasetToStringFields.get(dataset).add(field);
        }

        public void addMetricField(String dataset, Dimension dimension) {
            ensurePresent(dataset);
            datasetToMetricFields.get(dataset).add(dimension);
        }

        private void ensurePresent(String dataset) {
            if (!datasetToIntFields.containsKey(dataset)) {
                datasetToIntFields.put(dataset, Sets.<String>newHashSet());
            }

            if (!datasetToStringFields.containsKey(dataset)) {
                datasetToStringFields.put(dataset, Sets.<String>newHashSet());
            }

            if (!datasetToMetricFields.containsKey(dataset)) {
                datasetToMetricFields.put(dataset, Sets.newHashSet());
            }
        }

        public DatasetsFields build() {
            return new DatasetsFields(datasetToIntFields, datasetToStringFields, datasetToMetricFields);
        }
    }

    private static ImmutableMap<String, ImmutableSet<String>> copy(Map<String, Set<String>> m) {
        ImmutableMap.Builder<String, ImmutableSet<String>> builder = ImmutableMap.builder();
        for (final Map.Entry<String, Set<String>> entry : m.entrySet()) {
            builder.put(entry.getKey(), ImmutableSet.copyOf(entry.getValue()));
        }
        return builder.build();
    }

    private static ImmutableMap<String, ImmutableSet<Dimension>> copyDimension(Map<String, Set<Dimension>> m) {
        ImmutableMap.Builder<String, ImmutableSet<Dimension>> builder = ImmutableMap.builder();
        for (final Map.Entry<String, Set<Dimension>> entry : m.entrySet()) {
            builder.put(entry.getKey(), ImmutableSet.copyOf(entry.getValue()));
        }
        return builder.build();

    }

    public enum MetricFieldsType {
        ALL, ALIAS, NON_ALIAS
    }
}
