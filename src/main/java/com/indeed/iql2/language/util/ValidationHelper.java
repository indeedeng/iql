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

package com.indeed.iql2.language.util;

import com.indeed.iql.metadata.FieldMetadata;
import com.indeed.iql.metadata.FieldType;
import com.indeed.iql1.metadata.MetricMetadata;
import com.indeed.iql2.language.metadata.DatasetsMetadata;
import com.indeed.iql2.language.Validator;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class ValidationHelper {
    public final boolean useLegacy;
    private final DatasetsMetadata datasetsMetadata;
    private final Map<String, Set<String>> datasetAliasIntFields;
    private final Map<String, Set<String>> datasetAliasStringFields;

    public ValidationHelper(final DatasetsMetadata datasetsMetadata,
                            final Map<String, Set<String>> datasetAliasIntFields,
                            final Map<String, Set<String>> datasetAliasStringFields,
                            final boolean useLegacy) {
        this.useLegacy = useLegacy;
        this.datasetsMetadata = datasetsMetadata;
        this.datasetAliasIntFields = toCaseInsensitive(datasetAliasIntFields);
        this.datasetAliasStringFields = toCaseInsensitive(datasetAliasStringFields);
    }

    private Map<String, Set<String>> toCaseInsensitive(final Map<String, Set<String>> map) {
        final Map<String, Set<String>> caseInsensitiveMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
            caseInsensitiveMap.put(entry.getKey(), new TreeSet<>(String.CASE_INSENSITIVE_ORDER));
            caseInsensitiveMap.get(entry.getKey()).addAll(entry.getValue());
        }
        return caseInsensitiveMap;
    }


    public boolean containsStringField(String dataset, String field) {
        FieldMetadata fieldMetadata = new FieldMetadata(field, FieldType.String);
        return (datasetsMetadata.getMetadata(dataset).isPresent() && datasetsMetadata.getMetadata(dataset).get().stringFields.contains(fieldMetadata)) ||
                (datasetAliasStringFields.containsKey(dataset) && datasetAliasStringFields.get(dataset).contains(field));
    }

    public boolean containsIntField(String dataset, String field) {
        FieldMetadata fieldMetadata = new FieldMetadata(field, FieldType.Integer);
        return (datasetsMetadata.getMetadata(dataset).isPresent() && datasetsMetadata.getMetadata(dataset).get().intFields.contains(fieldMetadata)) ||
                (datasetAliasIntFields.containsKey(dataset) && datasetAliasIntFields.get(dataset).contains(field));
    }

    public void validateIntField(String dataset, String datasetField, Validator validator, Object context) {
        if (!containsIntOrAliasField(dataset, datasetField)) {
            // special case for page as it is a string field at Imhotep, but it also needs to support int field operation
            if ((dataset.equalsIgnoreCase("jobsearch") || dataset.equalsIgnoreCase("mobsearch"))
                    && (datasetField.equalsIgnoreCase("page") || datasetField.equalsIgnoreCase("vp"))) {
            } else if (containsStringField(dataset, datasetField)) {
                if (!useLegacy) {
                    validator.warn(ErrorMessages.stringFieldMismatch(dataset, datasetField, context));
                }
            } else {
                validator.error(ErrorMessages.missingIntField(dataset, datasetField, context));
            }
        }
    }

    public boolean containsIntOrAliasField(String dataset, String field) {
        return containsIntField(dataset, field) || containsAliasMetricField(dataset, field);
    }

    public boolean containsMetricField(String dataset, String field) {
        return getDimension(dataset, field).isPresent();
    }

    public boolean containsNonAliasMetricField(String dataset, String field) {
        final Optional<MetricMetadata> dimension = getDimension(dataset, field);
        return dimension.isPresent() && !dimension.get().isAlias;
    }

    public boolean containsAliasMetricField(String dataset, String field) {
        final Optional<MetricMetadata> dimension = getDimension(dataset, field);
        return dimension.isPresent() && dimension.get().isAlias;
    }

    // if field is in intFields or stringFields
    public boolean containsField(String dataset, String field) {
        return containsIntField(dataset, field) || containsStringField(dataset, field) || containsAliasMetricField(dataset, field);
    }

    private Optional<MetricMetadata> getDimension(String dataset, String field) {
        if (datasetsMetadata.getMetadata(dataset).isPresent() &&
                datasetsMetadata.getMetadata(dataset).get().fieldToDimension.containsKey(field)) {
            return Optional.of(datasetsMetadata.getMetadata(dataset).get().fieldToDimension.get(field));
        } else {
            return Optional.empty();
        }
    }

    public Set<String> datasets() {
        return datasetsMetadata.getDatasetToMetadata().keySet();
    }
}
