/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.iql2.server.web.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.iql.metadata.FieldMetadata;
import com.indeed.iql.metadata.FieldType;
import com.indeed.iql.web.FieldFrequencyCache;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.ims.client.yamlFile.DatasetYaml;
import com.indeed.ims.client.yamlFile.MetricsYaml;
import com.indeed.iql.metadata.MetricMetadata;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql.metadata.DatasetMetadata;
import com.indeed.iql2.language.metadata.DatasetsMetadata;
import com.indeed.iql2.language.query.Queries;
import com.indeed.util.core.time.DefaultWallClock;
import org.apache.log4j.Logger;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class MetadataCache {
    private static final Logger log = Logger.getLogger(MetadataCache.class);
    private static final ImmutableMap<String, MetricMetadata> DEFAULT_DIMENSIONS = initDefaultDimension();

    private final AtomicReference<DatasetsMetadata> atomMetadata = new AtomicReference<>(DatasetsMetadata.empty());
    @Nullable
    private final ImsClientInterface imsClient;
    private final ImhotepClient imhotepClient;
    private final FieldFrequencyCache fieldFrequencyCache;

    public MetadataCache(ImsClientInterface imsClient, ImhotepClient imhotepClient, FieldFrequencyCache fieldFrequencyCache) {
        this.imsClient = imsClient;
        this.imhotepClient = imhotepClient;
        this.fieldFrequencyCache = fieldFrequencyCache;
    }

    @Scheduled(fixedRate = 60000)
    public void updateMetadata() {
        log.info("Started updating metadata");
        try {
            final Map<String, DatasetInfo> datasetToShardList;
            final List<DatasetYaml> imsDatasets;

            if (imsClient == null) {
                imsDatasets = Collections.emptyList();
                log.warn("imsClient was null when updating metadata");
            } else {
                imsDatasets = Lists.newArrayList(imsClient.getDatasets());
            }

            if (imhotepClient == null) {
                datasetToShardList = Collections.emptyMap();
                log.warn("imhotepClient was null when updating metadata");
            } else {
                datasetToShardList = imhotepClient.getDatasetToDatasetInfo();
            }

            atomMetadata.set(buildDatasetsMetadata(datasetToShardList, imsDatasets));
            log.info("Updated dimension");
        } catch (Exception e) {
            log.error("Failed to update metadata", e);
        }
    }

    public DatasetsMetadata get() {
        return atomMetadata.get();
    }

    private DatasetsMetadata buildDatasetsMetadata(
            final Map<String, DatasetInfo> datasetToShardList,
            final List<DatasetYaml> imsDatasets) throws IOException {
        Map<String, Set<String>> datasetToMetrics = new HashMap<>();
        Map<String, Set<FieldMetadata>> datasetToIntFields = new HashMap<>();
        Map<String, Set<FieldMetadata>> datasetToStringFields = new HashMap<>();
        Map<String, DatasetYaml> datasetToDatasetYaml = new HashMap<>();

        for (DatasetYaml imsDataset : imsDatasets) {
            datasetToMetrics.put(imsDataset.getName(),
                    Arrays.stream(imsDataset.getMetrics()).map(MetricsYaml::getName).collect(Collectors.toSet()));
            datasetToDatasetYaml.put(imsDataset.getName(), imsDataset);
        }

        final Map<String, Map<String, Integer>> datasetToFieldFrequencies = fieldFrequencyCache.getFieldFrequencies();

        for (final Map.Entry<String, DatasetInfo> entry : datasetToShardList.entrySet()) {
            final Set<FieldMetadata> intFields = new HashSet<>();
            final Set<FieldMetadata> strFields = new HashSet<>();

            final String dataset = entry.getKey();
            final DatasetInfo datasetInfo = entry.getValue();
            //metrics defined in iql metadata webapp
            //all metrics defined there has type of Integer
            final Set<String> metrics = datasetToMetrics.getOrDefault(dataset, Collections.emptySet());
            final Map<String, Integer> fieldFrequencies = datasetToFieldFrequencies.getOrDefault(dataset, Maps.newHashMap());

            for (String intField: datasetInfo.getIntFields()) {
                final DatasetYaml datasetYaml = datasetToDatasetYaml.get(dataset);
                final String description = (datasetYaml == null || datasetYaml.getFieldsMap().get(intField) == null)
                                ? null : datasetYaml.getFieldsMap().get(intField).getDescription();
                intFields.add(new FieldMetadata(intField, FieldType.Integer).setDescription(Strings.nullToEmpty(description)).setFrequency(fieldFrequencies.getOrDefault(intField, 0)));
            }
            for (String strField : datasetInfo.getStringFields()) {
                final DatasetYaml datasetYaml = datasetToDatasetYaml.get(dataset);
                final String description = (datasetYaml == null || datasetYaml.getFieldsMap().get(strField) == null)
                                ? null : datasetYaml.getFieldsMap().get(strField).getDescription();
                if (metrics.contains(strField)) {
                    intFields.add(new FieldMetadata(strField, FieldType.Integer).setDescription(Strings.nullToEmpty(description)));
                }
                strFields.add(new FieldMetadata(strField, FieldType.String).setDescription(Strings.nullToEmpty(description)).setFrequency(fieldFrequencies.getOrDefault(strField, 0)));
            }
            // TODO: don't define unixtime as a field if it doesn't already exist in the dataset
            intFields.add(new FieldMetadata("unixtime", FieldType.Integer).setDescription("time of imhotep index"));
            datasetToIntFields.put(dataset, intFields);
            datasetToStringFields.put(dataset, strFields);
        }

        final Map<String, Map<String, MetricMetadata>> datasetToDimensions = buildDatasetsDimensions(imsDatasets);

        final Map<String, DatasetMetadata> datasetToMetadata = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (final String datasetName : datasetToShardList.keySet()) {
            DatasetYaml datasetYaml = datasetToDatasetYaml.get(datasetName);
            datasetToMetadata.put(datasetName, new DatasetMetadata(true, datasetName,
                    Strings.nullToEmpty(datasetYaml == null ? null : datasetYaml.getDescription()),
                    datasetYaml != null && datasetYaml.getDeprecated() != null && datasetYaml.getDeprecated(),
                    datasetToIntFields.getOrDefault(datasetName, Collections.emptySet()),
                    datasetToStringFields.getOrDefault(datasetName, Collections.emptySet()),
                    datasetToDimensions.getOrDefault(datasetName, Collections.emptyMap())));
        }

        return new DatasetsMetadata(datasetToMetadata);
    }

    private static ImmutableMap<String, MetricMetadata> initDefaultDimension() {
        final ImmutableMap.Builder<String, MetricMetadata> typeBuilder = new ImmutableMap.Builder<>();
        final String timeField = DatasetMetadata.TIME_FIELD_NAME;
        final String countsExpression = "count()";

        final List<String> metricParseOptions = Collections.emptyList();

        typeBuilder.put("counts", new MetricMetadata("counts", countsExpression, "Count of all documents",
                parseMetric("counts", countsExpression, metricParseOptions)));
        typeBuilder.put("dayofweek", new MetricMetadata("dayofweek", "(((" + timeField + "-280800)%604800)\\86400)",
                "day of week (days since Sunday)", parseMetric("dayofweek", "(((" + timeField + "-280800)%604800)\\86400)", metricParseOptions)));
        typeBuilder.put("timeofday", new MetricMetadata("timeofday", "((" + timeField + "-21600)%86400)",
                "time of day (seconds since midnight)", parseMetric("timeofday", "((" + timeField + "-21600)%86400)", metricParseOptions)));
        return typeBuilder.build();
    }

    private Map<String, Map<String, MetricMetadata>> buildDatasetsDimensions(
            final List<DatasetYaml> datasets) {
        final ImmutableMap.Builder<String, Map<String, MetricMetadata>> builder = new ImmutableMap.Builder<>();

        for (final DatasetYaml dataset : datasets) {
            builder.put(dataset.getName(), buildDatasetDimension(dataset));
        }
        return builder.build();
    }

    @VisibleForTesting
    ImmutableMap<String, MetricMetadata> buildDatasetDimension(final DatasetYaml dataset) {
        final ImmutableMap<String, MetricMetadata> datasetDimensions = parseMetrics(dataset.getName(), dataset.getMetrics());
        return new ImmutableMap.Builder<String, MetricMetadata>().putAll(DEFAULT_DIMENSIONS).putAll(datasetDimensions).build();
    }

    private ImmutableMap<String, MetricMetadata> parseMetrics(String dataset, MetricsYaml[] metrics) {
        final ImmutableMap.Builder<String, MetricMetadata> fieldToDimensionBuilder = new ImmutableMap.Builder<>();
        for (final MetricsYaml metric : metrics) {
            try {
                // TODO
//                final com.indeed.iql1.metadata.MetricMetadata metricMetadata = ImhotepMetadataCache.imsMetricYamlToIQLMetricMetadata(metric);
                fieldToDimensionBuilder.put(
                        metric.getName(),
                        new MetricMetadata(metric.getName(), metric.getExpr(), metric.getDescription(),
                                parseMetric(metric.getName(), metric.getExpr(), DatasetsMetadata.empty())));
            } catch (Exception e) {
                log.error(String.format("can't parse DimensionMetric, dataset: %s, name: %s, expr: %s, error: %s",
                        dataset, metric.getName(), metric.getExpr(), e.getMessage()));
            }
        }
        return fieldToDimensionBuilder.build();
    }

    @VisibleForTesting
    static AggregateMetric parseMetric(final String name, final String expr, List<String> options) {
        return parseMetric(name, expr, DatasetsMetadata.empty());
    }

    private static AggregateMetric parseMetric(final String name, final String expr, final DatasetsMetadata datasetsMetadata) {
        final String metricExpression;
        if (Strings.isNullOrEmpty(expr)) {
            metricExpression = name;
        } else {
            metricExpression = expr;
        }
        final AggregateMetric dimensionMetric = Queries.parseAggregateMetric(
                metricExpression, true, null, datasetsMetadata,
                s -> log.warn(String.format("parse DimensionMetric name: %s, expr: %s, warning: %s", name, expr, s)), new DefaultWallClock());
        if (dimensionMetric.requiresFTGS()) {
            throw new UnsupportedOperationException("Dimension metric requires FTGS is not supported");
        }
        return dimensionMetric;
    }
}
