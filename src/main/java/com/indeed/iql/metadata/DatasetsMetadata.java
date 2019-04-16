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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.web.DatasetTypeConflictFields;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 *
 */
public class DatasetsMetadata {
    private final Map<String, DatasetMetadata> metadata;
    // Case sensitive always
    private final Map<String, Map<String, String>> datasetToDimensionAliasFields;
    // Datasets that share the same case-insensitive name
    // Case insensitive on the keys.
    private final Map<String, Set<String>> datasetEquivalenceSets;
    // typeConflictDatasetFieldNames contains entries of the form "datasetname.fieldname"
    private final Set<String> typeConflictDatasetFieldNames;
    private static final DatasetsMetadata EMPTY_META = new DatasetsMetadata();

    private DatasetsMetadata() {
        metadata = Collections.emptyMap();
        datasetEquivalenceSets = Collections.emptyMap();
        datasetToDimensionAliasFields = Collections.emptyMap();
        typeConflictDatasetFieldNames = Collections.emptySet();
    }

    public Set<String> getTypeConflictDatasetFieldNames() {
        return typeConflictDatasetFieldNames;
    }

    public List<DatasetTypeConflictFields> getTypeConflictFields() {
        final List<DatasetTypeConflictFields> result = Lists.newArrayList();
        for (DatasetMetadata datasetMetadata : metadata.values()) {
            if (!datasetMetadata.conflictFieldNames.isEmpty()) {
                result.add(new DatasetTypeConflictFields(datasetMetadata.name, datasetMetadata.conflictFieldNames));
            }
        }
        return result;
    }

    public DatasetsMetadata(final Map<String, DatasetMetadata> metadata) {
        this.metadata = Maps.newHashMap(metadata);

        datasetEquivalenceSets = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (final String datasetName : metadata.keySet()) {
            datasetEquivalenceSets
                    .computeIfAbsent(datasetName, x -> new HashSet<>())
                    .add(datasetName);
        }

        datasetToDimensionAliasFields = new HashMap<>();
        metadata.forEach((dataset, meta) -> {
            datasetToDimensionAliasFields.put(dataset, meta.fieldToDimension.entrySet()
                    .stream().filter(dimension -> dimension.getValue().isAlias())
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getAliasActualField().get())));
        });
        typeConflictDatasetFieldNames = new HashSet<>();
        metadata.forEach((dataset, meta) -> {
            for (final String conflictFieldName : meta.conflictFieldNames) {
                typeConflictDatasetFieldNames.add(meta.name + "." + conflictFieldName);
            }
        });
    }

    public static DatasetsMetadata empty() {
        return EMPTY_META;
    }

    public String resolveDatasetName(final String dataset) {
        final Set<String> equivalenceSet = datasetEquivalenceSets.get(dataset);
        if (equivalenceSet == null) {
            throw new IqlKnownException.UnknownDatasetException("Dataset not found: \"" + dataset + "\"");
        }

        // Use exact match if present.
        if (equivalenceSet.contains(dataset)) {
            return dataset;
        }

        // Ambiguous, no exact matches
        if (equivalenceSet.size() > 1) {
            throw new IqlKnownException.UnknownDatasetException("Multiple datasets match, and none are an exact match: " + equivalenceSet + ", seeking \"" + dataset + "\"");
        }

        // Otherwise, use the single value available.
        Preconditions.checkState(equivalenceSet.size() == 1);
        return Iterables.getOnlyElement(equivalenceSet);
    }

    public Map<String, DatasetMetadata> getDatasetToMetadata() {
        return metadata;
    }

    public Optional<DatasetMetadata> getMetadata(@Nullable final String dataset) {
        if (dataset == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(metadata.get(dataset));
    }

    public Map<String, Map<String, String>> getDatasetToDimensionAliasFields() {
        return datasetToDimensionAliasFields;
    }

    public boolean fieldHasDescription(final String dataset, final String field, final boolean useLegacy) {
        final Optional<DatasetMetadata> datasetMetadata = getMetadata(dataset);
        if (datasetMetadata.isPresent()) {
            return !Strings.isNullOrEmpty(datasetMetadata.get().getFieldDescription(field, useLegacy));
        }
        return false;
    }
}
