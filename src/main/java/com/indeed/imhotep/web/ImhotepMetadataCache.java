/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.imhotep.web;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.metadata.DatasetMetadata;
import com.indeed.imhotep.metadata.FieldMetadata;
import com.indeed.imhotep.metadata.FieldType;
import com.indeed.imhotep.metadata.MetricMetadata;
import com.indeed.imhotep.metadata.YamlMetadataConverter;
import com.indeed.ims.client.ImsClient;
import com.indeed.ims.client.ImsClientInterface;
import com.indeed.ims.client.yamlFile.DatasetYaml;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.Nonnull;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.Query;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @author vladimir
 */

public class ImhotepMetadataCache {
    private static final Logger log = Logger.getLogger(ImhotepMetadataCache.class);

    private LinkedHashMap<String, DatasetMetadata> datasets = Maps.newLinkedHashMap();
    // TODO: integrate into the metadata above?
    private volatile Map<String, Set<String>> datasetToKeywordAnaylzerWhitelist = Maps.newHashMap();
    private final ImhotepClient imhotepClient;
    private String ramsesMetadataPath;
    private final List<Pattern> disabledFields = Lists.newArrayList();
    private ImsClientInterface metadataClient;


    public ImhotepMetadataCache(ImhotepClient client, String ramsesMetadataPath, String disabledFields) {
        imhotepClient = client;
        this.ramsesMetadataPath = ramsesMetadataPath;
        try {
            ///A way to get the port from tomcat without a request
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            Set<ObjectName> objs = mbs.queryNames(new ObjectName("*:type=Connector,*"),
                       Query.match(Query.attr("protocol"), Query.value("HTTP/1.1")));
            String hostname = InetAddress.getLocalHost().getHostName();
            InetAddress[] addresses = InetAddress.getAllByName(hostname);
            ArrayList<String> ports = new ArrayList<String>();
            for (Iterator<ObjectName> i = objs.iterator(); i.hasNext();) {
                ObjectName obj = i.next();
                String port = obj.getKeyProperty("port");
                ports.add(port);
            }
            ///
            String url = "http://localhost:" + ports.get(0)+"/iql/";
            metadataClient = ImsClient.build(url);
        } catch (URISyntaxException e) {
                log.error("Failed to connect to the metadata service",e);
        }
        catch (Exception e) {
            log.error(e);
        }
        if(disabledFields != null) {
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
        // First make empty DatasetMetadata instances
        final LinkedHashMap<String, DatasetMetadata> newDatasets = Maps.newLinkedHashMap();

//       now load the metadata from the IMS
        DatasetYaml[] datasetYamls = metadataClient.getDatasets();
        for (final DatasetYaml dataset : datasetYamls) {
            newDatasets.put(dataset.getName(), YamlMetadataConverter.convertDataset(dataset));
        }

        for (final DatasetMetadata datasetMetadata : newDatasets.values()) {
            datasetMetadata.finishLoading();
        }

        // new metadata instance is ready for use
        datasets = newDatasets;

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

    public LinkedHashMap<String, DatasetMetadata> getDatasets() {
        return datasets;
    }

    @Nonnull
    public DatasetMetadata getDataset(String dataset) {
        if(!datasets.containsKey(dataset)) {
            return new DatasetMetadata(dataset);    // empty
        }
        return datasets.get(dataset);
    }

    public Set<String> getKeywordAnalyzerWhitelist(String dataset) {
        if(!datasetToKeywordAnaylzerWhitelist.containsKey(dataset)) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(datasetToKeywordAnaylzerWhitelist.get(dataset));
    }

    @Scheduled(fixedRate = 60000)
    private void updateKeywordAnalyzerWhitelist() {
        datasetToKeywordAnaylzerWhitelist = metadataClient.getWhitelist();//newKeywordAnaylzerWhitelist;

    }

    private boolean loadMetadataFromFiles(LinkedHashMap<String, DatasetMetadata> newDatasetToAliases) {
        File ramsesDir = new File(ramsesMetadataPath);
        if(!ramsesDir.exists() || !ramsesDir.isDirectory()) {
            log.error("Directory not found at " + ramsesMetadataPath);
            return false;
        }
        File[] files = ramsesDir.listFiles();
        if(files == null) {
            log.error("Failed to stat directory at " + ramsesMetadataPath);
            return false;
        }
        for(File indexDir : files) {
            if(!indexDir.isDirectory()) {
                continue;
            }
            final String indexName = indexDir.getName();

            final DatasetMetadata datasetMetadata = newDatasetToAliases.get(indexName);
            if(datasetMetadata == null) {
                log.trace("Found dimensions data for unknown dataset: " + indexName);
                continue;
            }

            loadDimensions(indexDir, datasetMetadata);

            loadSuggestions(indexDir, datasetMetadata);
        }
        return true;
    }

    // aliases applicable to all indexes
    private void addStandardAliases(DatasetMetadata datasetMetadata) {
        MetricMetadata countsMetadata = datasetMetadata.getMetric("counts");
        if(countsMetadata == null) {
            countsMetadata = new MetricMetadata("counts");
            datasetMetadata.getMetrics().put("counts", countsMetadata);
        }
        if(!datasetMetadata.isRamsesDataset()) {    // for Ramses datasets we should allow counts to be pushed so that scaling can be applied
            countsMetadata.setExpression("count()");
        }
        countsMetadata.setDescription("Count of all documents");

        final String timeField = datasetMetadata.getTimeFieldName();

        // make sure we have time field in Ramses indexes   // TODO: why is it not returned by Imhotep?
        if(datasetMetadata.isRamsesDataset()) {
            final String ramsesTimeField = "time";
            final String timeDescription = "Unix timestamp (seconds since epoch)";
            MetricMetadata timeMetric = datasetMetadata.getMetric(ramsesTimeField);
            if(timeMetric == null) {
                timeMetric = new MetricMetadata(ramsesTimeField);
                datasetMetadata.getMetrics().put(ramsesTimeField, timeMetric);
            }
            timeMetric.setDescription(timeDescription);
            timeMetric.setUnit("seconds");

            FieldMetadata timeFieldMetadata = datasetMetadata.getField(ramsesTimeField);
            if(timeFieldMetadata == null) {
                timeFieldMetadata = new FieldMetadata(ramsesTimeField, FieldType.String);
                datasetMetadata.getFields().put(ramsesTimeField, timeFieldMetadata);
            }
            timeFieldMetadata.setDescription(timeDescription);
            timeFieldMetadata.setType(FieldType.Integer);
        }

        tryAddMetricAlias("dayofweek", "(((" + timeField + "-280800)%604800)\\86400)", "day of week (days since Sunday)", datasetMetadata);
        tryAddMetricAlias("timeofday", "((" + timeField + "-21600)%86400)", "time of day (seconds since midnight)", datasetMetadata);
    }

    private static Set<String> RESERVED_KEYWORDS = ImmutableSet.of("time", "bucket", "buckets", "lucene", "in");

    private boolean tryAddMetricAlias(String metricName, String replacement, String description, DatasetMetadata datasetMetadata) {
        // only add the alias if it's safe to do so. it shouldn't hide an existing field or be a reserved keyword
        if(datasetMetadata.hasField(metricName)
                && !replacement.startsWith("floatscale")    // allow floatscale operation to replace the original field as floats are not usable as is
                || RESERVED_KEYWORDS.contains(metricName)) {

            log.trace("Skipped adding alias due to conflict: " + datasetMetadata.getName() + "." + metricName + "->" + replacement);
            return false;
        }

        MetricMetadata metricMetadata = datasetMetadata.getMetric(metricName);
        if(metricMetadata == null) {
            metricMetadata = new MetricMetadata(metricName);
            datasetMetadata.getMetrics().put(metricName, metricMetadata);
        }

        metricMetadata.setExpression(replacement);
        if(description != null) {
            metricMetadata.setDescription(description);
        }
        return true;
    }

    private void loadSuggestions(File indexDir, DatasetMetadata datasetMetadata) {
        final File suggestionsXml = new File(indexDir, "suggestions.xml");
        if (!suggestionsXml.exists()) {
            return;
        }
        @SuppressWarnings("unchecked")
        final Map<String, String> suggestions = (Map<String, String>) new XmlBeanFactory(new FileSystemResource(suggestionsXml)).getBean("suggestionMap");
        if (suggestions != null) {
            for(Map.Entry<String, String> suggestion : suggestions.entrySet()) {
                datasetMetadata.addFieldMetricDescription(suggestion.getKey(), suggestion.getValue(), null, false, true, false);
            }
        }
    }

    /**
     * Loads metrics descriptions and aliases for an index from a Ramses dimensions file
     */
    private void loadDimensions(File indexDir, DatasetMetadata datasetMetadata) {
        final File dimensionsFile = new File(indexDir, "dimensions.desc");
        if(!dimensionsFile.exists()) {
            return;
        }
        BufferedReader reader = null;
        try {
            final Map<String, Alias> fieldToAlias = Maps.newHashMap();

            reader = new BufferedReader(new InputStreamReader(new FileInputStream(dimensionsFile)));
            for(String line = reader.readLine(); line != null; line = reader.readLine()) {
                if(line.startsWith("#")) {
                    if(line.startsWith("#/")) {
                        // dimension only for IQL but not ramses hack
                        line = line.substring(2);
                    } else {
                        continue;
                    }
                }
                String[] split = line.split(",");
                if(split.length < 5) {
                    continue; // invalid field entry?
                }

                String name = split[0].trim();
                String desc = split[1].trim();
                String unit = split[2].trim();
                final String dimType = split[3].trim();

                if(Strings.isNullOrEmpty(unit) || "null".equals(unit)) {
                    unit = null;
                }
                if(Strings.isNullOrEmpty(desc) || "null".equals(desc)) {
                    desc = null;
                }
                boolean isHidden = name.startsWith("!");
                if(isHidden) {
                    name = name.substring(1);
                }

                if(name.equals("time")) {
                    continue;   // time is a reserved field/keyword
                }

                boolean metricHasField = false;
                Alias alias = null;

                if ("add".equals(dimType) || "subtract".equals(dimType) ||
                        "multiply".equals(dimType) || "divide".equals(dimType)) {

                    String dim1 = split[4].trim();
                    String dim2 = split[5].trim();
                    if (dim1.startsWith("!")) dim1 = dim1.substring(1);
                    if (dim2.startsWith("!")) dim2 = dim2.substring(1);
                    final String op;
                    if ("add".equals(dimType)) {
                        op = "+";
                    } else if ("subtract".equals(dimType)) {
                        op = "-";
                    } else if("divide".equals(dimType)) {
                        op = "\\";
                    } else {
                        op = "*";
                    }
                    alias = new CompositeOp(op, dim1, dim2, isHidden);
                } else if("lossless".equals(dimType)) {
                    String realField = split[4].trim();
                    if(!name.equals(realField)) {
                        if(realField.startsWith("floatscale")) {
                            realField = realField.replace(' ', '(').replace('*', ',').replace('+', ',') + ')';
                        }
                        alias = new SimpleField(realField, isHidden);
                    } else {
                        metricHasField = true;
                    }
                }

                if(!(isHidden && alias != null)) {    // if it's an aliased hidden metric, it's intermediary and we can skip it
                    datasetMetadata.addFieldMetricDescription(name, desc, unit, isHidden, metricHasField, true);
                }

                if(alias != null) {
                    fieldToAlias.put(name, alias);
                }
            }

            // now that we have all the aliases loaded we can resolve them
            for(Map.Entry<String, Alias> entry : fieldToAlias.entrySet()) {
                final Alias alias = entry.getValue();
                if(alias.hidden) {
                    continue;   // this is just an intermediate metric
                }
                final String metricName = entry.getKey();
                final String resolvedAlias = alias.resolve(fieldToAlias);
                if(resolvedAlias == null) {
                    log.warn("Found a metric alias with a circular dependency which is illegal: " + datasetMetadata.getName() + "." + metricName);
                    continue;
                }
                tryAddMetricAlias(metricName, resolvedAlias, null, datasetMetadata);
                log.trace("Aliasing: " + datasetMetadata.getName() + "." + metricName + "->" + resolvedAlias);
            }
        } catch (FileNotFoundException e) {
            log.warn("Dimensions file read failed for " + indexDir, e);
        } catch (IOException e) {
            log.warn("Dimensions file read failed for " + indexDir, e);
        } finally {
            if(reader != null) {
                Closeables2.closeQuietly(reader, log);
            }
        }
    }

    private static abstract class Alias {
        boolean hidden;

        protected Alias(boolean hidden) {
            this.hidden = hidden;
        }

        abstract String resolve(Map<String,Alias> fieldToAlias);
    }

    private static class SimpleField extends Alias {
        String fieldName;

        private SimpleField(String fieldName, boolean hidden){
            super(hidden);
            this.fieldName = fieldName;
        }

        @Override
        public String resolve(Map<String, Alias> fieldToAlias) {
            return fieldName;
        }
    }

    private static class CompositeOp extends Alias {
        String operator;
        String dim1;
        String dim2;
        boolean isSeen; // keeps track of whether the resolve process has already encountered this object

        private CompositeOp(String operator, String dim1, String dim2, boolean hidden) {
            super(hidden);
            this.operator = operator;
            this.dim1 = dim1;
            this.dim2 = dim2;
        }

        @Override
        public String resolve(Map<String, Alias> fieldToAlias) {
            if(isSeen) { // protection from infinite recursion
                return null;
            }
            final String dim1Resolved;
            final String dim2Resolved;
            isSeen = true;
            try {
                final Alias dim1Alias = fieldToAlias.get(dim1);
                if(dim1Alias != null) {
                    dim1Resolved = dim1Alias.resolve(fieldToAlias);
                } else {
                    dim1Resolved = dim1;
                }

                final Alias dim2Alias = fieldToAlias.get(dim2);
                if(dim2Alias != null) {
                    dim2Resolved = dim2Alias.resolve(fieldToAlias);
                } else {
                    dim2Resolved = dim2;
                }

                if(dim1Resolved == null || dim2Resolved == null) {  // encountered a loop
                    return null;
                }
            } finally {
                isSeen = false;
            }
            return "(" + dim1Resolved + operator + dim2Resolved + ")";
        }
    }
}
