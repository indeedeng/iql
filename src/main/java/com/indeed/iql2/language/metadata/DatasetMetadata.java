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

package com.indeed.iql2.language.metadata;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.indeed.iql.metadata.FieldMetadata;
import com.indeed.iql.metadata.FieldType;
import com.indeed.iql1.metadata.MetricMetadata;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * all fields are case insensitive
 */
public class DatasetMetadata {
    public final String datasetName;
    public final String description;
    public final boolean deprecated;
    public final TreeSet<FieldMetadata> intFields;
    public final TreeSet<FieldMetadata> stringFields;
    public final Map<String, MetricMetadata> fieldToDimension;

    public DatasetMetadata(String datasetName, String description, boolean deprecated) {
        this.datasetName = datasetName;
        this.deprecated = deprecated;
        intFields = new TreeSet<FieldMetadata>(FieldMetadata.CASE_INSENSITIVE_ORDER);
        stringFields = new TreeSet<FieldMetadata>(FieldMetadata.CASE_INSENSITIVE_ORDER);
        fieldToDimension = ImmutableMap.of();
        this.description = description;
    }

    public DatasetMetadata(final String datasetName, final String description, boolean deprecated, final Set<FieldMetadata> intFields, final Set<FieldMetadata> stringFields,
                           final Map<String, MetricMetadata> fieldToDimension) {
        this.datasetName = datasetName;
        this.deprecated = deprecated;
        this.intFields = toCaseInsensitive(intFields);
        this.stringFields = toCaseInsensitive(stringFields);

        final Map<String, MetricMetadata> caseInsensitiveMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        caseInsensitiveMap.putAll(fieldToDimension);
        this.fieldToDimension = caseInsensitiveMap;
        this.description = description;
    }

    private TreeSet<FieldMetadata> toCaseInsensitive(final Set<FieldMetadata> set) {
        final TreeSet<FieldMetadata> caseInsensitiveSet = new TreeSet<>(FieldMetadata.CASE_INSENSITIVE_ORDER);
        caseInsensitiveSet.addAll(set);
        return caseInsensitiveSet;
    }

    public TreeSet<String> getIntFieldsStringFromMetadata() {
        return intFields.stream().map(fieldMetadata -> fieldMetadata.getName()).collect(
                Collectors.toCollection(()->new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
    }

    public Set<String> getStrFieldsStringFromMetadata() {
        return stringFields.stream().map(fieldMetadata -> fieldMetadata.getName()).collect(
                Collectors.toCollection(()->new TreeSet<>(String.CASE_INSENSITIVE_ORDER)));
    }

    public String getFieldDescription(final String field) {
        final FieldMetadata fakeFieldMetadata = new FieldMetadata(field, FieldType.Integer);
        final FieldMetadata ceilingIntObject = intFields.ceiling(fakeFieldMetadata);
        if (FieldMetadata.CASE_INSENSITIVE_ORDER.compare(fakeFieldMetadata, ceilingIntObject) == 0) {
            return Strings.nullToEmpty(ceilingIntObject.getDescription());
        } else {
            final FieldMetadata ceilingStringObject = stringFields.ceiling(fakeFieldMetadata);
            if (FieldMetadata.CASE_INSENSITIVE_ORDER.compare(fakeFieldMetadata, ceilingStringObject) == 0) {
                return Strings.nullToEmpty(ceilingStringObject.getDescription());
            }
            return "";
        }
    }
}
