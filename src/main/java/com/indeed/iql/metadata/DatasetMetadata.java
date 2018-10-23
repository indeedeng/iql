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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.log4j.Logger;
import java.util.stream.Collectors;

public class DatasetMetadata {
    private static final Logger log = Logger.getLogger(DatasetMetadata.class);

    public static String TIME_FIELD_NAME = "unixtime";
    private final Comparator<FieldMetadata> fieldMetadataComparator;
    private final Comparator<String> fieldNameComparator;
    private final boolean iql2mode;
    public final String name;
    @Nullable public String description;
    @Nullable public String owner;
    @Nullable public String project;
    public boolean deprecated;
    public final TreeSet<FieldMetadata> intFields;
    public final TreeSet<FieldMetadata> stringFields;
    public Set<String> conflictFieldNames;
    public final Map<String, MetricMetadata> fieldToDimension;

    // Required by LuceneQueryTranslator, so cache it here
    @Nonnull Set<String> iql1IntImhotepFieldSet = Sets.newHashSet();
    // used by the preprocessor
    @Nonnull Map<String, String> iql1Aliases = Maps.newHashMap();

    public DatasetMetadata(boolean iql2mode, String name) {
        this(iql2mode, name, null, null, null, false);
    }

    public DatasetMetadata(boolean iql2mode, String name, String description, String owner, String project, boolean deprecated) {
        this(iql2mode, name, description, owner, project, deprecated, Collections.emptySet(), Collections.emptySet(), Collections.emptyMap());
    }

    public DatasetMetadata(final boolean iql2mode, final String name, final String description, String owner, String project, boolean deprecated, final Set<FieldMetadata> intFields, final Set<FieldMetadata> stringFields,
                           final Map<String, MetricMetadata> fieldToDimension) {
        fieldMetadataComparator = iql2mode ? FieldMetadata.CASE_INSENSITIVE_ORDER : FieldMetadata.CASE_SENSITIVE_ORDER;
        fieldNameComparator = iql2mode ? String.CASE_INSENSITIVE_ORDER : null;
        this.iql2mode = iql2mode;
        this.name = name;
        this.owner = owner;
        this.project = project;
        this.deprecated = deprecated;
        this.intFields = toCaseInsensitive(intFields);
        this.stringFields = toCaseInsensitive(stringFields);
        this.conflictFieldNames = new TreeSet<>();

        final Map<String, MetricMetadata> fieldToDimensionCopy = new TreeMap<>(fieldNameComparator);
        fieldToDimensionCopy.putAll(fieldToDimension);
        this.fieldToDimension = fieldToDimensionCopy;
        this.description = description;
    }

    private TreeSet<FieldMetadata> toCaseInsensitive(final Set<FieldMetadata> set) {
        final TreeSet<FieldMetadata> caseInsensitiveSet = new TreeSet<>(fieldMetadataComparator);
        caseInsensitiveSet.addAll(set);
        return caseInsensitiveSet;
    }

    public void setDescription(@Nullable String description) {
        this.description = description;
    }

    public void setOwner(@Nullable String owner) {
        this.owner = owner;
    }

    public void setProject(@Nullable String project) {
        this.project = project;
    }

    public void setDeprecated(boolean deprecated) {
        this.deprecated = deprecated;
    }

    public TreeSet<String> getIntFieldsStringFromMetadata() {
        return intFields.stream().map(fieldMetadata -> fieldMetadata.getName()).collect(
                Collectors.toCollection(()->new TreeSet<>(fieldNameComparator)));
    }

    public Set<String> getStrFieldsStringFromMetadata() {
        return stringFields.stream().map(fieldMetadata -> fieldMetadata.getName()).collect(
                Collectors.toCollection(()->new TreeSet<>(fieldNameComparator)));
    }

    public String getFieldDescription(final String field) {
        final FieldMetadata fieldMetadata = getField(field);
        return fieldMetadata == null ? null : Strings.nullToEmpty(fieldMetadata.description);
    }

    @Nullable
    public FieldMetadata getField(String field) {
        final FieldMetadata fakeFieldMetadata = new FieldMetadata(field, FieldType.Integer);
        // TODO: unify default type between iql1 and iql2
        if(iql2mode) {
            final FieldMetadata intField = getIntField(fakeFieldMetadata);
            if (intField == null) {
                return getStringField(fakeFieldMetadata);
            } else {
                return intField;
            }
        } else {
            final FieldMetadata stringField = getStringField(fakeFieldMetadata);
            if (stringField == null) {
                return getIntField(fakeFieldMetadata);
            } else {
                return stringField;
            }
        }
    }

    private FieldMetadata getIntField(FieldMetadata fakeFieldMetadata) {
        final FieldMetadata ceilingIntObject = intFields.ceiling(fakeFieldMetadata);
        if (ceilingIntObject != null && fieldMetadataComparator.compare(fakeFieldMetadata, ceilingIntObject) == 0) {
            return ceilingIntObject;
        } else {
            return null;
        }
    }

    private FieldMetadata getStringField(FieldMetadata fakeFieldMetadata) {
        final FieldMetadata ceilingStringObject = stringFields.ceiling(fakeFieldMetadata);
        if (ceilingStringObject != null && fieldMetadataComparator.compare(fakeFieldMetadata, ceilingStringObject) == 0) {
            return ceilingStringObject;
        }
        return null;
    }

    @Nullable
    public MetricMetadata getMetric(String name) {
        return null;
    }

    public boolean hasField(String field) {
        // FieldType is ignored for searching
        final FieldMetadata fieldToFind = new FieldMetadata(field, FieldType.String);
        return intFields.contains(fieldToFind) || stringFields.contains(fieldToFind);
    }

    public boolean hasIntField(String field) {
        return intFields.contains(new FieldMetadata(field, FieldType.Integer));
    }

    public boolean hasStringField(String field) {
        return stringFields.contains(new FieldMetadata(field, FieldType.String));
    }

    @Nonnull
    public Set<String> getIql1IntImhotepFieldSet() {
        return iql1IntImhotepFieldSet;
    }

    @Nonnull
    public Map<String, String> getIql1Aliases() {
        return iql1Aliases;
    }

    /**
     * Should be called when the metadata loading is complete to update caches.
     */
    public void finishLoading() {
        Preconditions.checkState(iql1IntImhotepFieldSet.isEmpty());
        for(FieldMetadata fieldMetadata : intFields) {
            iql1IntImhotepFieldSet.add(fieldMetadata.getName());
        }
        iql1IntImhotepFieldSet = Collections.unmodifiableSet(iql1IntImhotepFieldSet);

        Preconditions.checkState(iql1Aliases.isEmpty());
        for(MetricMetadata metric : fieldToDimension.values()) {
            if(!Strings.isNullOrEmpty(metric.expression) && !metric.expression.equals(metric.name)) {
                iql1Aliases.put(metric.name, metric.expression);
            }
        }
        iql1Aliases = Collections.unmodifiableMap(iql1Aliases);

        final Set<String> intFieldNames = intFields.stream().map(x -> x.name).collect(Collectors.toSet());
        final Set<String> stringFieldNames = stringFields.stream().map(x -> x.name).collect(Collectors.toSet());
        conflictFieldNames = Sets.intersection(intFieldNames, stringFieldNames);
    }

    public void toJSON(ObjectNode jsonNode, ObjectMapper mapper, boolean summaryOnly) {
        jsonNode.put("name", name);
        if (deprecated) {
            jsonNode.put("deprecated", true);
        }
        jsonNode.put("description", Strings.nullToEmpty(description));
        if (owner != null) {
            jsonNode.put("owner", owner);
        }
        if (project != null) {
            jsonNode.put("project", project);
        }
        if(summaryOnly) {
            return;
        }

        final ArrayNode fieldsArray = mapper.createArrayNode();
        jsonNode.set("fields", fieldsArray);
        fieldListToJson(mapper, fieldsArray, intFields);
        fieldListToJson(mapper, fieldsArray, stringFields);

        final ArrayNode metricsArray = mapper.createArrayNode();
        jsonNode.set("metrics", metricsArray);
        for(MetricMetadata metric : fieldToDimension.values()) {
            if(metric.isHidden()) {
                continue;
            }
            final ObjectNode datasetInfo = mapper.createObjectNode();
            metric.toJSON(datasetInfo);
            metricsArray.add(datasetInfo);
        }
    }

    private void fieldListToJson(ObjectMapper mapper, ArrayNode fieldsArray, TreeSet<FieldMetadata> intFields) {
        for(FieldMetadata field : intFields) {
            if(field.isHidden()) {
                continue;
            }
            final ObjectNode fieldInfo = mapper.createObjectNode();
            field.toJSON(fieldInfo);
            fieldsArray.add(fieldInfo);
        }
    }
}
