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

import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql.metadata.MetricMetadata;
import com.indeed.iql.web.Limits;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import com.indeed.util.core.Pair;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class ValidationHelper {
    public final Limits limits;
    private final boolean useLegacy;
    private final DatasetsMetadata datasetsMetadata;
    private final Map<String, Pair<Long, Long>> datasetsTimeRange;
    private final Set<String> definedComputations = new HashSet<>();

    public ValidationHelper(
            final DatasetsMetadata datasetsMetadata,
            final Limits limits,
            final Map<String, Pair<Long, Long>> datasetsTimeRange,
            final boolean useLegacy) {
        this.limits = limits;
        this.useLegacy = useLegacy;
        this.datasetsTimeRange = datasetsTimeRange;
        this.datasetsMetadata = datasetsMetadata;
    }

    public boolean containsStringField(String dataset, String field) {
        return (datasetsMetadata.getMetadata(dataset).isPresent() && datasetsMetadata.getMetadata(dataset).get().hasStringField(field));
    }

    public boolean containsIntField(String dataset, String field) {
        return (datasetsMetadata.getMetadata(dataset).isPresent() && datasetsMetadata.getMetadata(dataset).get().hasIntField(field));
    }

    public void validateIntField(String dataset, String datasetField, ErrorCollector errorCollector, Object context) {
        if (!containsIntOrAliasField(dataset, datasetField)) {
            // special case for page as it is a string field at Imhotep, but it also needs to support int field operation
            if (("jobsearch".equals(dataset) || "mobsearch".equals(dataset))
                    && ("page".equals(datasetField) || "vp".equals(datasetField))) {
            } else if (containsStringField(dataset, datasetField)) {
                if (!useLegacy) {
                    errorCollector.warn(ErrorMessages.stringFieldMismatch(dataset, datasetField, context));
                }
            } else {
                errorCollector.error(ErrorMessages.missingIntField(dataset, datasetField, context));
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
        return dimension.isPresent() && !dimension.get().isAlias();
    }

    public boolean containsAliasMetricField(String dataset, String field) {
        final Optional<MetricMetadata> dimension = getDimension(dataset, field);
        return dimension.isPresent() && dimension.get().isAlias();
    }

    // if field is in intFields or stringFields
    public boolean containsField(String dataset, String field) {
        return containsIntField(dataset, field) || containsStringField(dataset, field) || containsAliasMetricField(dataset, field);
    }

    public void validateSampleParams(
            final long numerator,
            final long denominator,
            final ErrorCollector errorCollector) {
        if ((numerator < 0) || (numerator > denominator)) {
            errorCollector.error(ErrorMessages.incorrectSampleParams(numerator, denominator));
        }
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

    public Map<String, Pair<Long, Long>> datasetTimeRanges() {
        return datasetsTimeRange;
    }

    public void registerComputed(final String name) {
        if (!definedComputations.add(name)) {
            throw new IllegalStateException("Tried to define the same name more than once: " + name);
        }
    }

    public boolean isComputed(final String name) {
        return definedComputations.contains(name);
    }
}
