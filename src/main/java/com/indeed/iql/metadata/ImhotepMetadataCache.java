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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.ims.client.yamlFile.DatasetYaml;
import com.indeed.ims.client.yamlFile.FieldsYaml;
import com.indeed.ims.client.yamlFile.MetricsYaml;
import com.indeed.iql.web.FieldFrequencyCache;
import com.indeed.iql2.language.Identifiers;
import com.indeed.iql2.language.JQLParser;
import com.indeed.iql2.language.query.Queries;
import org.apache.log4j.Logger;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
    private final ImsClientInterface metadataClient;
    private final FieldFrequencyCache fieldFrequencyCache;

    public ImhotepMetadataCache(@Nullable ImsClientInterface imsClient, ImhotepClient client, String disabledFields, final FieldFrequencyCache fieldFrequencyCache) {
        metadataClient = imsClient;
        imhotepClient = client;
        this.fieldFrequencyCache = fieldFrequencyCache;
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

        if(datasetToShardList.isEmpty()) {   // if we get no data, just keep what we already have
            log.warn("Imhotep returns no datasets");
            return;
        }

        final Map<String, DatasetMetadata> newDatasets = Maps.newHashMap();
        for(DatasetInfo datasetInfo : datasetToShardList.values()) {
            final String datasetName = datasetInfo.getDataset();

            final DatasetMetadata datasetMetadata = new DatasetMetadata(datasetName);
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
                        newDataset.setOwner(datasetYaml.getOwner());
                        newDataset.setCertification(datasetYaml.getCertification());
                        newDataset.setProject(datasetYaml.getBuilderJiraProject());
                        if (datasetYaml.getDeprecated() != null) {
                            newDataset.setDeprecated(datasetYaml.getDeprecated());
                        }

                        FieldsYaml[] fieldsYamls = datasetYaml.getFields();
                        for (FieldsYaml fieldYaml : fieldsYamls) {
                            for (final FieldMetadata fieldMetadata : newDataset.getFieldAllTypes(fieldYaml.getName())) {
                                fieldMetadata.setDescription(fieldYaml.getDescription());
                                fieldMetadata.setHidden(fieldYaml.getHidden());
                                fieldMetadata.setCertified(fieldYaml.getCertified());
                                fieldMetadata.setAliases(fieldYaml.getAliases());
                                for (final String alias : fieldMetadata.getAliases()) {
                                    newDataset.getIql1ExpressionAliases().put(alias, fieldMetadata.getName());
                                    // TODO: make aliases not go through computed metrics?
                                    final MetricMetadata aliasAsComputedMetric = new MetricMetadata(alias,
                                            fieldMetadata.getName(), null, fieldMetadata.getName());
                                    newDataset.fieldToDimension.put(alias, aliasAsComputedMetric);
                                }
                            }
                        }
                        MetricsYaml[] metricsYamls = datasetYaml.getMetrics();
                        Map<String, MetricMetadata> metrics = newDataset.fieldToDimension;

                        for (MetricsYaml metricYaml : metricsYamls) {
                            final MetricMetadata metricMetadata = getMetricMetadataFromMetricsYaml(metricYaml, newDataset.name);
                            if(metricMetadata == null) {
                                continue;
                            }
                            metrics.put(metricYaml.getName(), metricMetadata);
                            // try to reuse the metric description on the field it describes
                            for(final FieldMetadata relatedField : newDataset.getFieldAllTypes(metricMetadata.getName())) {
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
                        for (final FieldMetadata fieldMetadata : datasetMetadata.getFieldAllTypes(entry.getKey())) {
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
        atomMetadata.set(new DatasetsMetadata(newDatasets));

        log.debug("Finished metadata update");
    }

    @VisibleForTesting
    MetricMetadata getMetricMetadataFromMetricsYaml(MetricsYaml metric, String dataset) {
        try {
            String fieldAlias = null;
            try {
                final JQLParser.IdentifierContext identifier = Queries.runParser(metric.getExpr(), JQLParser::identifierTerminal).identifier();
                fieldAlias = Identifiers.extractIdentifier(identifier);
            } catch (Exception e) {
                // Guess it wasn't an alias.
            }
            final MetricMetadata metricMetadata = new MetricMetadata(metric.getName(), metric.getExpr(), metric.getDescription(), fieldAlias);
            metricMetadata.setUnit(metric.getUnits());
            return metricMetadata;
        } catch (Exception e) {
            log.error(String.format("can't parse DimensionMetric, dataset: %s, name: %s, expr: %s, error: %s",
                    dataset, metric.getName(), metric.getExpr(), e.getMessage()));
            return null;
        }
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
    public DatasetMetadata getDataset(final String dataset) {
        return get().getMetadata(dataset).orElse(new DatasetMetadata(dataset, "", null, null, false));    // empty)
    }

    // applicable to all indexes
    private static ImmutableMap<String, MetricMetadata> initDefaultDimension() {
        final ImmutableMap.Builder<String, MetricMetadata> typeBuilder = new ImmutableMap.Builder<>();
        final String timeField = DatasetMetadata.TIME_FIELD_NAME;
        final String countsExpression = "count()";

        typeBuilder.put("counts", new MetricMetadata("counts", countsExpression, "Count of all documents", null));
        typeBuilder.put("dayofweek", new MetricMetadata("dayofweek", "(((" + timeField + "-280800)%604800)\\86400)",
                "day of week (days since Sunday)", null));
        typeBuilder.put("timeofday", new MetricMetadata("timeofday", "((" + timeField + "-21600)%86400)",
                "time of day (seconds since midnight)", null));
        return typeBuilder.build();
    }
}