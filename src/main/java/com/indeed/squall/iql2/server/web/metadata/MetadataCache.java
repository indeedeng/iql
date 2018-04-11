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

package com.indeed.squall.iql2.server.web.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.ims.client.yamlFile.DatasetYaml;
import com.indeed.ims.client.yamlFile.MetricsYaml;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.dimensions.Dimension;
import com.indeed.squall.iql2.language.metadata.DatasetMetadata;
import com.indeed.squall.iql2.language.metadata.DatasetsMetadata;
import com.indeed.squall.iql2.language.metadata.FieldMetadata;
import com.indeed.squall.iql2.language.metadata.ImmutableFieldMetadata;
import com.indeed.squall.iql2.language.query.Queries;
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
    private static final ImmutableMap<DatasetType, ImmutableMap<String, Dimension>> DEFAULT_DIMENSIONS = initDefaultDimension();

    private final AtomicReference<DatasetsMetadata> atomMetadata = new AtomicReference<>(DatasetsMetadata.empty());
    @Nullable
    private final ImsClientInterface imsClient;
    private final ImhotepClient imhotepClient;

    public MetadataCache(ImsClientInterface imsClient, ImhotepClient imhotepClient) {
        this.imsClient = imsClient;
        this.imhotepClient = imhotepClient;
    }

    @Scheduled(fixedRate = 600000)
    public void updateMetadata() {
        log.info("Started updating metadata");
        try {
            final Map<String, DatasetInfo> datasetToShardList;
            final List<DatasetYaml> imsDatasets;

            if (imsClient == null) {
                imsDatasets = Collections.emptyList();
            } else {
                imsDatasets = Lists.newArrayList(imsClient.getDatasets());
            }

            if (imhotepClient == null) {
                datasetToShardList = Collections.emptyMap();
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

        for (final Map.Entry<String, DatasetInfo> entry : datasetToShardList.entrySet()) {
            final Set<FieldMetadata> intFields = new HashSet<>();
            final Set<FieldMetadata> strFields = new HashSet<>();

            final String dataset = entry.getKey();
            final DatasetInfo datasetInfo = entry.getValue();
            //metrics defined in iql metadata webapp
            //all metrics defined there has type of Integer
            final Set<String> metrics = datasetToMetrics.getOrDefault(dataset, Collections.emptySet());
            final ImmutableFieldMetadata.Builder integerBuilder = ImmutableFieldMetadata.builder().setType(FieldMetadata.Type.Integer);
            final ImmutableFieldMetadata.Builder stringBuilder = ImmutableFieldMetadata.builder().setType(FieldMetadata.Type.String);
            for (String intField: datasetInfo.getIntFields()) {
                final DatasetYaml datasetYaml = datasetToDatasetYaml.get(dataset);
                final String description = (datasetYaml == null || datasetYaml.getFieldsMap().get(intField) == null)
                                ? null : datasetYaml.getFieldsMap().get(intField).getDescription();
                intFields.add(integerBuilder.setName(intField).setDescription(Strings.nullToEmpty(description)).build());
            }
            for (String strField : datasetInfo.getStringFields()) {
                final DatasetYaml datasetYaml = datasetToDatasetYaml.get(dataset);
                final String description = (datasetYaml == null || datasetYaml.getFieldsMap().get(strField) == null)
                                ? null : datasetYaml.getFieldsMap().get(strField).getDescription();
                if (metrics.contains(strField)) {
                    intFields.add(integerBuilder.setName(strField).setDescription(Strings.nullToEmpty(description)).build());
                }
                strFields.add(stringBuilder.setName(strField).setDescription(Strings.nullToEmpty(description)).build());
            }
            if (entry.getValue().getIntFields().isEmpty()) {
                log.trace(dataset + " is ramses index");
                intFields.add(integerBuilder.setName("time").setDescription("time of ramses index").build());
            } else {
                log.trace(dataset + " is imhotep index");
                intFields.add(integerBuilder.setName("unixtime").setDescription("time of imhotep index").build());
            }
            datasetToIntFields.put(dataset, intFields);
            datasetToStringFields.put(dataset, strFields);
        }

        final Map<String, Map<String, Dimension>> datasetToDimensions = buildDatasetsDimensions(imsDatasets);

        final Map<String, DatasetMetadata> datasetToMetadata = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (final String datasetName : datasetToShardList.keySet()) {
            DatasetYaml datasetYaml = datasetToDatasetYaml.get(datasetName);
            datasetToMetadata.put(datasetName, new DatasetMetadata(datasetName,
                    Strings.nullToEmpty(datasetYaml == null ? null : datasetYaml.getDescription()),
                    datasetToIntFields.getOrDefault(datasetName, Collections.emptySet()),
                    datasetToStringFields.getOrDefault(datasetName, Collections.emptySet()),
                    datasetToDimensions.getOrDefault(datasetName, Collections.emptyMap())));
        }

        return new DatasetsMetadata(datasetToMetadata);
    }

    private static ImmutableMap<DatasetType, ImmutableMap<String, Dimension>> initDefaultDimension() {
        final ImmutableMap.Builder<DatasetType, ImmutableMap<String, Dimension>> defaultBuilder = new ImmutableMap.Builder<>();
        for (final DatasetType datasetType : DatasetType.values()) {
            final ImmutableMap.Builder<String, Dimension> typeBuilder = new ImmutableMap.Builder<>();
            final String timeField;
            final String countsExpression;
            if (datasetType == DatasetType.Imhotep) {
                timeField = "unixtime";
                countsExpression = "count()";
            } else {
                timeField = "time";
                // for Ramses datasets we should allow counts to be pushed so that scaling can be applied
                countsExpression = "counts";
            }

            final List<String> metricParseOptions = Collections.emptyList();

            typeBuilder.put("counts", new Dimension("counts", countsExpression, "Count of all documents",
                    parseMetric("counts", countsExpression, metricParseOptions)));
            typeBuilder.put("dayofweek", new Dimension("dayofweek", "(((" + timeField + "-280800)%604800)\\86400)",
                    "day of week (days since Sunday)", parseMetric("dayofweek", "(((" + timeField + "-280800)%604800)\\86400)", metricParseOptions)));
            typeBuilder.put("timeofday", new Dimension("timeofday", "((" + timeField + "-21600)%86400)",
                    "time of day (seconds since midnight)", parseMetric("timeofday", "((" + timeField + "-21600)%86400)", metricParseOptions)));
            defaultBuilder.put(datasetType, typeBuilder.build());
        }
        return defaultBuilder.build();
    }

    private Map<String, Map<String, Dimension>> buildDatasetsDimensions(
            final List<DatasetYaml> datasets) {
        final ImmutableMap.Builder<String, Map<String, Dimension>> builder = new ImmutableMap.Builder<>();

        for (final DatasetYaml dataset : datasets) {
            builder.put(dataset.getName(), buildDatasetDimension(dataset));
        }
        return builder.build();
    }

    @VisibleForTesting
    ImmutableMap<String, Dimension> buildDatasetDimension(final DatasetYaml dataset) {
        final ImmutableMap<String, Dimension> defaultDimensions;
        final DatasetType datasetType = asDatasetType(dataset.getType());
        if (!DEFAULT_DIMENSIONS.containsKey(datasetType)) {
            log.error(String.format("get dataset default dimension for %s failed, unknown dataset type: %s",
                    dataset.getName(), dataset.getType()));
            defaultDimensions = ImmutableMap.of();
        } else {
            defaultDimensions = DEFAULT_DIMENSIONS.get(datasetType);
        }
        final ImmutableMap<String, Dimension> datasetDimensions = parseMetrics(dataset.getName(), dataset.getMetrics());
        return new ImmutableMap.Builder<String, Dimension>().putAll(defaultDimensions).putAll(datasetDimensions).build();
    }

    private ImmutableMap<String, Dimension> parseMetrics(String dataset, MetricsYaml[] metrics) {
        final ImmutableMap.Builder<String, Dimension> fieldToDimensionBuilder = new ImmutableMap.Builder<>();
        for (final MetricsYaml metric : metrics) {
            try {
                fieldToDimensionBuilder.put(
                        metric.getName(),
                        new Dimension(metric.getName(), metric.getExpr(), metric.getDescription(),
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

    enum DatasetType {
        Imhotep, Ramses
    }

    public static DatasetType asDatasetType(String str) {
        for (DatasetType datasetType : DatasetType.values()) {
            if (datasetType.name().equalsIgnoreCase(str)) {
                return datasetType;
            }
        }
        return null;
    }
}
