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
 package com.indeed.iql.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.ims.client.yamlFile.DatasetYaml;
import com.indeed.ims.client.yamlFile.FieldsYaml;
import com.indeed.ims.client.yamlFile.MetricsYaml;
import com.indeed.iql.web.FieldFrequencyCache;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.query.Queries;
import com.indeed.util.core.time.DefaultWallClock;
import org.apache.log4j.Logger;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/**
 * @author vladimir
 */

public class ImhotepMetadataCache {
    private static final Logger log = Logger.getLogger(ImhotepMetadataCache.class);
    private static final ImmutableMap<String, MetricMetadata> DEFAULT_DIMENSIONS = initDefaultDimension();


    private final AtomicReference<DatasetsMetadata> atomMetadata = new AtomicReference<>(DatasetsMetadata.empty());
    private final ImhotepClient imhotepClient;
    private final List<Pattern> disabledFields = Lists.newArrayList();
    @Nullable
    private ImsClientInterface metadataClient;
    private final FieldFrequencyCache fieldFrequencyCache;
    private final boolean caseInsensitiveNames;

    public ImhotepMetadataCache(ImsClientInterface imsClient, ImhotepClient client, String disabledFields, final FieldFrequencyCache fieldFrequencyCache, boolean caseInsensitiveNames) {
        metadataClient = imsClient;
        imhotepClient = client;
        this.fieldFrequencyCache = fieldFrequencyCache;
        this.caseInsensitiveNames = caseInsensitiveNames;
        if(!Strings.isNullOrEmpty(disabledFields)) {
            for(String field : disabledFields.split(",")) {
                try {
                    this.disabledFields.add(Pattern.compile(field.trim()));
                } catch (Exception e) {
                    log.warn("Failed to compile regex pattern for disabled field: " + field);
                }
            }
        }
    }

    // updated every 60s and actual shards in ImhotepClient are reloaded every 60s
    @Scheduled(fixedRate = 60000)
    public void updateDatasets() {
        log.trace("Started metadata update");
        Map<String, DatasetInfo> datasetToShardList = imhotepClient.getDatasetToDatasetInfo();
        log.trace("Loaded metadata for " + datasetToShardList.size() + " datasets from Imhotep");

        if(datasetToShardList.size() == 0) {   // if we get no data, just keep what we already have
            log.warn("Imhotep returns no datasets");
            return;
        }

        final Map<String, DatasetMetadata> newDatasets = Maps.newHashMap();
        for(DatasetInfo datasetInfo : datasetToShardList.values()) {
            final String datasetName = datasetInfo.getDataset();

            final DatasetMetadata datasetMetadata = new DatasetMetadata(caseInsensitiveNames, datasetName);
            newDatasets.put(datasetName, datasetMetadata);
            List<String> dsIntFields = Lists.newArrayList(datasetInfo.getIntFields());
            List<String> dsStringFields = Lists.newArrayList(datasetInfo.getStringFields());
            removeDisabledFields(dsIntFields);
            removeDisabledFields(dsStringFields);

            for(String intField : dsIntFields) {
                datasetMetadata.intFields.add(new FieldMetadata(intField, FieldType.Integer));
            }

            for(String stringField : dsStringFields) {
                datasetMetadata.stringFields.add(new FieldMetadata(stringField, FieldType.String));
            }

            // set default computed metrics on all datasets
            datasetMetadata.fieldToDimension.putAll(DEFAULT_DIMENSIONS);
        }

        log.trace("Metadata loaded from Imhotep. Querying IMS");

        if(metadataClient != null) {
            // now load the metadata from the IMS
            try {
                DatasetYaml[] datasetYamls = metadataClient.getDatasets();
                log.trace("Got metadata for " + datasetYamls.length + " datasets from IMS");
                for (final DatasetYaml datasetYaml : datasetYamls) {
                    if (newDatasets.containsKey(datasetYaml.getName())) {
                        DatasetMetadata newDataset = newDatasets.get(datasetYaml.getName());
                        newDataset.setDescription(datasetYaml.getDescription());
                        if (datasetYaml.getDeprecated() != null) {
                            newDataset.setDeprecated(datasetYaml.getDeprecated());
                        }

                        FieldsYaml[] fieldsYamls = datasetYaml.getFields();
                        for (FieldsYaml fieldYaml : fieldsYamls) {
                            FieldMetadata fieldMetadata = newDataset.getField(fieldYaml.getName());
                            if (fieldMetadata != null) {
                                fieldMetadata.setDescription(fieldYaml.getDescription());
                                fieldMetadata.setHidden(fieldYaml.getHidden());
                                fieldMetadata.setFriendlyName(fieldYaml.getFriendlyName());

                            }
                        }
                        MetricsYaml metricsYamls[] = datasetYaml.getMetrics();
                        Map<String, MetricMetadata> metrics = newDataset.fieldToDimension;

                        for (MetricsYaml metricYaml : metricsYamls) {
                            final MetricMetadata metricMetadata = getMetricMetadataFromMetricsYaml(metricYaml, newDataset.name);
                            if(metricMetadata == null) {
                                continue;
                            }
                            metrics.put(metricYaml.getName(), metricMetadata);
                            // try to reuse the metric description on the field it describes
                            final FieldMetadata relatedField = newDataset.getField(metricMetadata.getName());
                            if (relatedField != null) {
                                if (Strings.isNullOrEmpty(metricMetadata.getExpression())) {
                                    // Having a metric defined with the same name is a marker we use
                                    // for fields that should be treated as Integer
                                    relatedField.setType(FieldType.Integer);
                                }
                                if (Strings.isNullOrEmpty(metricMetadata.getExpression()) &&
                                        !Strings.isNullOrEmpty(metricMetadata.getDescription()) &&
                                        Strings.isNullOrEmpty(relatedField.getDescription())) {

                                    relatedField.setDescription(metricMetadata.getDescription());
                                }
                            }
                        }

                    }
                }
            } catch (Exception e) {
                log.error("An error occurred when receiving metadata from IMS", e);
            }
        }

        // Set field frequencies
        final Map<String, Map<String, Integer>> fieldFrequencies = fieldFrequencyCache.getFieldFrequencies();
        if (fieldFrequencies != null) {
            for (final DatasetMetadata datasetMetadata : newDatasets.values()) {
                if (fieldFrequencies.containsKey(datasetMetadata.name)) {
                    final Map<String, Integer> fieldToFrequency = fieldFrequencies.get(datasetMetadata.name);
                    for (final Map.Entry<String, Integer> entry : fieldToFrequency.entrySet()) {
                        final FieldMetadata fieldMetadata = datasetMetadata.getField(entry.getKey());
                        if(fieldMetadata != null) {
                            fieldMetadata.setFrequency(entry.getValue());
                        }
                    }
                }
            }
        }


        for (final DatasetMetadata datasetMetadata : newDatasets.values()) {
            datasetMetadata.finishLoading();
        }

        // new metadata instance is ready for use
        atomMetadata.set(new DatasetsMetadata(caseInsensitiveNames, newDatasets));

        log.debug("Finished metadata update");
    }

    MetricMetadata getMetricMetadataFromMetricsYaml(MetricsYaml metric, String dataset) {
        try {
            final MetricMetadata metricMetadata = new MetricMetadata(metric.getName(), metric.getExpr(), metric.getDescription(),
                    parseMetric(metric.getName(), metric.getExpr(), DatasetsMetadata.empty()));
            metricMetadata.setUnit(metric.getUnits());
            metricMetadata.setFriendlyName(metric.getFriendlyName());
            return metricMetadata;
        } catch (Exception e) {
            log.error(String.format("can't parse DimensionMetric, dataset: %s, name: %s, expr: %s, error: %s",
                    dataset, metric.getName(), metric.getExpr(), e.getMessage()));
            return null;
        }
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

    public DatasetsMetadata get() {
        return atomMetadata.get();
    }

    private void removeDisabledFields(List<String> fields) {
        Iterator<String> iterator = fields.iterator();
        while(iterator.hasNext()) {
            final String field = iterator.next();
            for(Pattern regex : disabledFields) {
                if(regex.matcher(field).matches()) {
                    iterator.remove();
                }
            }
        }
    }

    @Nonnull
    public DatasetMetadata getDataset(String dataset) {
        return get().getMetadata(dataset).or(new DatasetMetadata(false, dataset, "", false));    // empty)
    }

    // applicable to all indexes
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

    private static Set<String> RESERVED_KEYWORDS = ImmutableSet.of("time", "bucket", "buckets", "lucene", "in");

    private boolean tryAddMetricAlias(String metricName, String replacement, String description, DatasetMetadata datasetMetadata) {
        // only add the alias if it's safe to do so. it shouldn't hide an existing field or be a reserved keyword
        if(datasetMetadata.hasField(metricName)
                && !replacement.startsWith("floatscale")    // allow floatscale operation to replace the original field as floats are not usable as is
                || RESERVED_KEYWORDS.contains(metricName)) {

            log.trace("Skipped adding alias due to conflict: " + datasetMetadata.name + "." + metricName + "->" + replacement);
            return false;
        }

        MetricMetadata metricMetadata = datasetMetadata.getMetric(metricName);
        if(metricMetadata == null) {
            metricMetadata = new MetricMetadata(metricName);
            datasetMetadata.fieldToDimension.put(metricName, metricMetadata);
        }

        metricMetadata.setExpression(replacement);
        if(description != null) {
            metricMetadata.setDescription(description);
        }
        return true;
    }
}