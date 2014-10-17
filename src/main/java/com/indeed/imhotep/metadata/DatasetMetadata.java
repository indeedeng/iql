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
 package com.indeed.imhotep.metadata;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author vladimir
 */

public class DatasetMetadata implements Comparable<DatasetMetadata> {
    private static final Logger log = Logger.getLogger(DatasetMetadata.class);

    @Nonnull final String name;
    @Nullable String description;
    @Nonnull final LinkedHashMap<String, FieldMetadata> fields = Maps.newLinkedHashMap();
    @Nonnull final LinkedHashMap<String, MetricMetadata> metrics = Maps.newLinkedHashMap();
    @Nullable DatasetType type = null;

    // Required by LuceneQueryTranslator, so cache it here
    @Nonnull Set<String> intImhotepFieldSet = Sets.newHashSet();

    // used by the preprocessor
    @Nonnull Map<String, String> aliases = Maps.newHashMap();

    public DatasetMetadata(@Nonnull String name) {
        Preconditions.checkNotNull(name);
        this.name = name;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    @Nullable
    public FieldMetadata getField(String name) {
        return fields.get(name);
    }

    @Nullable
    public MetricMetadata getMetric(String name) {
        return metrics.get(name);
    }

    @Nonnull
    public LinkedHashMap<String, FieldMetadata> getFields() {
        return fields;
    }

    @Nonnull
    public LinkedHashMap<String, MetricMetadata> getMetrics() {
        return metrics;
    }

    @Nullable
    public String getDescription() {
        return description;
    }

    public void setDescription(@Nullable String description) {
        this.description = description;
    }

    public boolean hasField(String field) {
        return fields.containsKey(field);
    }

    public boolean hasIntField(String field) {
        final FieldMetadata fieldMetadata = fields.get(field);
        return fieldMetadata != null && fieldMetadata.isIntImhotepField();
    }

    public boolean hasStringField(String field) {
        final FieldMetadata fieldMetadata = fields.get(field);
        return fieldMetadata != null && fieldMetadata.isStringImhotepField();
    }

    public DatasetType getType() {
        if(type == null) {
            // try to detect
            for(FieldMetadata fieldMetadata : fields.values()) {
                if(fieldMetadata.isIntImhotepField()) {
                    type = DatasetType.Imhotep;
                    break;
                }
            }
            if(type == null) {
                type = DatasetType.Ramses;
            }
        }
        return type;
    }

    public boolean isImhotepDataset() {
        return getType() == DatasetType.Imhotep;
    }

    public boolean isRamsesDataset() {
        return getType() == DatasetType.Ramses;
    }

    public String getTimeFieldName() {
        return isImhotepDataset() ? "unixtime" : "time";
        // may want to try finding the time field among available fields like below
//        if(intFields.isEmpty()) {
//            return "time";  // in ramses indexes (which have no int fields) time is the mandatory name
//        }
//        // in imhotep indexes it is normally unixtime but things can vary
//        for(String field : intFields) {
//            if("time".equals(field) || "unixtime".equals(field)) {
//                return field;
//            }
//        }
//        throw new UnsupportedOperationException("time field not found");
    }

    @Nonnull
    public Set<String> getIntImhotepFieldSet() {
        return intImhotepFieldSet;
    }

    @Nonnull
    public Map<String, String> getAliases() {
        return aliases;
    }

    public void addFieldMetricDescription(String name, String desc, String unit, boolean isHidden, boolean hasField, boolean hasMetric) {
        if(hasField) {
            final FieldMetadata fieldMetadata = getField(name);
            if(fieldMetadata != null) {
                fieldMetadata.setDescription(desc);
                if(isHidden) {
                    fieldMetadata.setHidden(isHidden);
                }
                if(hasMetric && fieldMetadata.isStringImhotepField()) {
                    fieldMetadata.setType(FieldType.Integer);   // mark this as an int since it's a metric
                }
            } else {
                log.trace("Unknown field defined: " + getName() + "." + name);
            }
        }

        if(!hasMetric) {
            return; // no metric for this
        }

        MetricMetadata metricMetadata = getMetric(name);
        if(metricMetadata == null) {
            metricMetadata = new MetricMetadata(name);
            metrics.put(name, metricMetadata);
        }
        metricMetadata.setDescription(desc);
        if(isHidden) {
            metricMetadata.setHidden(isHidden);
        }
        if(unit != null) {
            metricMetadata.setUnit(unit);
        }
    }

    /**
     * Should be called when the metadata loading is complete to update caches.
     */
    public void finishLoading() {
        Preconditions.checkState(intImhotepFieldSet.isEmpty());
        for(FieldMetadata fieldMetadata : fields.values()) {
            if(fieldMetadata.isIntImhotepField()) {
                intImhotepFieldSet.add(fieldMetadata.getName());
            }
        }
        intImhotepFieldSet = Collections.unmodifiableSet(intImhotepFieldSet);


        Preconditions.checkState(aliases.isEmpty());
        for(MetricMetadata metric : metrics.values()) {
            if(metric.expression != null && !metric.expression.equals(metric.name)) {
                aliases.put(metric.name, metric.expression);
            }
        }
        aliases = Collections.unmodifiableMap(aliases);
    }

    @Override
    public int compareTo(DatasetMetadata o) {
        return name.compareTo(o.name);
    }

    public void toJSON(ObjectNode jsonNode, ObjectMapper mapper, boolean summaryOnly) {
        jsonNode.put("name", getName());
        jsonNode.put("description", Strings.nullToEmpty(getDescription()));
        if(summaryOnly) {
            return;
        }

        final ArrayNode fieldsArray = mapper.createArrayNode();
        jsonNode.put("fields", fieldsArray);
        for(FieldMetadata field : getFields().values()) {
            if(field.isHidden()) {
                continue;
            }
            final ObjectNode fieldInfo = mapper.createObjectNode();
            field.toJSON(fieldInfo);
            fieldsArray.add(fieldInfo);
        }

        final ArrayNode metricsArray = mapper.createArrayNode();
        jsonNode.put("metrics", metricsArray);
        for(MetricMetadata metric : getMetrics().values()) {
            if(metric.isHidden()) {
                continue;
            }
            final ObjectNode datasetInfo = mapper.createObjectNode();
            metric.toJSON(datasetInfo);
            metricsArray.add(datasetInfo);
        }
    }
}
