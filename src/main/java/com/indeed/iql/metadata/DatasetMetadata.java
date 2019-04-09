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
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.iql.exceptions.IqlKnownException;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class DatasetMetadata {
    private static final Logger log = Logger.getLogger(DatasetMetadata.class);

    public static final String TIME_FIELD_NAME = "unixtime";
    public final String name;
    @Nullable public String description;
    @Nullable public String owner;
    @Nullable public String certification;
    @Nullable public String project;
    public boolean deprecated;
    // Always case sensitive
    public final TreeSet<FieldMetadata> intFields;
    // Always case sensitive
    public final TreeSet<FieldMetadata> stringFields;
    // Case insensitive in the keys
    public final Map<String, Set<String>> fieldNameEquivalenceSets = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    public Set<String> conflictFieldNames;
    public final Map<String, MetricMetadata> fieldToDimension;
    private final Map<String, Set<String>> dimensionEquivalenceSets = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    // Required by LuceneQueryTranslator, so cache it here
    @Nonnull Set<String> iql1IntImhotepFieldSet = Sets.newHashSet();
    // used by the preprocessor
    @Nonnull Map<String, String> iql1ExpressionAliases = Maps.newHashMap();

    public DatasetMetadata(final String name) {
        this(name, null, null, null, false);
    }

    public DatasetMetadata(
            final String name,
            final String description,
            final String owner,
            final String project,
            final boolean deprecated) {
        this(name, description, owner, project, deprecated, Collections.emptySet(), Collections.emptySet(), Collections.emptyMap());
    }

    public DatasetMetadata(
            final String name,
            final String description,
            final String owner,
            final String project,
            final boolean deprecated,
            final Set<FieldMetadata> intFields,
            final Set<FieldMetadata> stringFields,
            final Map<String, MetricMetadata> fieldToDimension) {
        this.name = name;
        this.owner = owner;
        this.project = project;
        this.deprecated = deprecated;
        this.intFields = toTreeSet(intFields);
        this.stringFields = toTreeSet(stringFields);
        this.conflictFieldNames = new TreeSet<>();
        this.fieldToDimension = new HashMap<>(fieldToDimension);
        this.description = description;
    }

    private TreeSet<FieldMetadata> toTreeSet(final Set<FieldMetadata> set) {
        final TreeSet<FieldMetadata> caseInsensitiveSet = new TreeSet<>(FieldMetadata.COMPARATOR);
        caseInsensitiveSet.addAll(set);
        return caseInsensitiveSet;
    }

    public void setDescription(@Nullable String description) {
        this.description = description;
    }

    public void setOwner(@Nullable String owner) {
        this.owner = owner;
    }

    public void setCertification(@Nullable String certification) {
        this.certification = certification;
    }

    public void setProject(@Nullable String project) {
        this.project = project;
    }

    public void setDeprecated(boolean deprecated) {
        this.deprecated = deprecated;
    }

    public Set<String> getIntFieldsStringFromMetadata() {
        return intFields.stream()
                .map(FieldMetadata::getName)
                .collect(Collectors.toSet());
    }

    public String getFieldDescription(final String field, final boolean useLegacy) {
        final FieldMetadata fieldMetadata = getField(field, useLegacy);
        return fieldMetadata == null ? null : Strings.nullToEmpty(fieldMetadata.getDescription());
    }

    @Nullable
    public FieldMetadata getField(final String field, final boolean useLegacy) {
        final FieldMetadata fakeFieldMetadata = new FieldMetadata(field, FieldType.Integer);
        // TODO: unify default type between iql1 and iql2
        if(!useLegacy) {
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

    // returns metadata for both field types
    public List<FieldMetadata> getFieldAllTypes(final String field) {
        final List<FieldMetadata> fields = new ArrayList<>(1);
        final FieldMetadata fakeFieldMetadata = new FieldMetadata(field, FieldType.Integer);
        final FieldMetadata intField = getIntField(fakeFieldMetadata);
        if (intField != null) {
            fields.add(intField);
        }
        final FieldMetadata stringField = getStringField(fakeFieldMetadata);
        if (stringField != null) {
            fields.add(stringField);
        }
        return fields;
    }

    private FieldMetadata getIntField(FieldMetadata fakeFieldMetadata) {
        final FieldMetadata ceilingIntObject = intFields.ceiling(fakeFieldMetadata);
        if (ceilingIntObject != null && FieldMetadata.COMPARATOR.compare(fakeFieldMetadata, ceilingIntObject) == 0) {
            return ceilingIntObject;
        } else {
            return null;
        }
    }

    private FieldMetadata getStringField(FieldMetadata fakeFieldMetadata) {
        final FieldMetadata ceilingStringObject = stringFields.ceiling(fakeFieldMetadata);
        if (ceilingStringObject != null && FieldMetadata.COMPARATOR.compare(fakeFieldMetadata, ceilingStringObject) == 0) {
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

    public String resolveFieldName(final String field) {
        final Set<String> equivalent = fieldNameEquivalenceSets.getOrDefault(field, Collections.emptySet());
        // Prefer an exact match
        if (equivalent.contains(field)) {
            return field;
        }

        // Ambiguous, no exact match
        if (equivalent.size() > 1) {
            throw new IqlKnownException.UnknownFieldException("Multiple fields match, and none are an exact match: " + equivalent + ", seeking \"" + field + "\"");
        }

        // None found
        if (equivalent.isEmpty()) {
            throw new IqlKnownException.UnknownFieldException("Field not found in dataset \"" + name + "\": \"" + field + "\"");
        }

        // Otherwise, use the single value available.
        return Iterables.getOnlyElement(equivalent);
    }

    @Nullable
    public MetricMetadata resolveMetric(final String typedName) {
        final Set<String> equivalent = dimensionEquivalenceSets.getOrDefault(typedName, Collections.emptySet());

        // Prefer an exact match
        if (equivalent.contains(typedName)) {
            return Objects.requireNonNull(fieldToDimension.get(typedName));
        }

        // Ambiguous, no exact match
        if (equivalent.size() > 1) {
            throw new IqlKnownException.UnknownFieldException("Multiple dimension metrics match, and none are an exact match: " + equivalent + ", seeking \"" + typedName + "\"");
        }

        // None found
        if (equivalent.isEmpty()) {
            return null;
        }

        // Otherwise, use the single value available.
        return Objects.requireNonNull(fieldToDimension.get(Iterables.getOnlyElement(equivalent)));
    }

    @Nonnull
    public Set<String> getIql1IntImhotepFieldSet() {
        return iql1IntImhotepFieldSet;
    }

    @Nonnull
    public Map<String, String> getIql1ExpressionAliases() {
        return iql1ExpressionAliases;
    }

    public boolean isDeprecatedOrDescriptionDeprecated() {
        return deprecated || Strings.nullToEmpty(description).toLowerCase().contains("deprecated");
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

        for(final Map.Entry<String, MetricMetadata> entry : fieldToDimension.entrySet()) {
            final MetricMetadata metric = entry.getValue();
            if(!Strings.isNullOrEmpty(metric.getExpression()) && !metric.getExpression().equals(metric.getName())) {
                iql1ExpressionAliases.put(metric.getName(), metric.getExpression());
            }
            dimensionEquivalenceSets
                    .computeIfAbsent(entry.getKey(), ignored -> new HashSet<>())
                    .add(entry.getKey());
        }
        iql1ExpressionAliases = Collections.unmodifiableMap(iql1ExpressionAliases);

        final Set<String> intFieldNames = intFields.stream().map(FieldMetadata::getName).collect(Collectors.toSet());
        final Set<String> stringFieldNames = stringFields.stream().map(FieldMetadata::getName).collect(Collectors.toSet());
        conflictFieldNames = Sets.intersection(intFieldNames, stringFieldNames);

        for (final String fieldName : Iterables.concat(intFieldNames, stringFieldNames)) {
            fieldNameEquivalenceSets.computeIfAbsent(fieldName, x -> new HashSet<>()).add(fieldName);
        }
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
        if (!Strings.isNullOrEmpty(certification)) {
            jsonNode.put("certification", certification);
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
            if(metric.isHidden() || metric.isAlias() || Strings.isNullOrEmpty(metric.getExpression())) {
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
