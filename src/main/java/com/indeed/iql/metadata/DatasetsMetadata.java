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

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.indeed.iql.web.DatasetTypeConflictFields;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 *
 */
public class DatasetsMetadata {
    private final Map<String, DatasetMetadata> metadata;
    private final Map<String, Map<String, String>> datasetToDimensionAliasFields;
    private final Set<String> typeConflictFieldNames;
    private static final DatasetsMetadata EMPTY_META = new DatasetsMetadata();

    private DatasetsMetadata() {
        metadata = Collections.emptyMap();
        datasetToDimensionAliasFields = Collections.emptyMap();
        typeConflictFieldNames = Collections.emptySet();
    }

    public Set<String> getTypeConflictFieldNames() {
        return typeConflictFieldNames;
    }

    public List<DatasetTypeConflictFields> getTypeConflictFields() {
        final List<DatasetTypeConflictFields> result = Lists.newArrayList();
        for (DatasetMetadata datasetMetadata : metadata.values()) {
            if (datasetMetadata.conflictFieldNames.size() > 0) {
                result.add(new DatasetTypeConflictFields(datasetMetadata.name, datasetMetadata.conflictFieldNames));
            }
        }
        return result;
    }

    public DatasetsMetadata(final boolean caseInsensitiveNames, final Map<String, DatasetMetadata> metadata) {
        final Comparator<String> fieldNameComparator = caseInsensitiveNames ? String.CASE_INSENSITIVE_ORDER : null;
        this.metadata = new TreeMap<>(fieldNameComparator);
        this.metadata.putAll(metadata);
        datasetToDimensionAliasFields = new TreeMap<>(fieldNameComparator);
        metadata.forEach((dataset, meta) -> {
            datasetToDimensionAliasFields.put(dataset, meta.fieldToDimension.entrySet()
                    .stream().filter(dimension -> dimension.getValue().isAlias)
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getAliasActualField().get())));
        });
        typeConflictFieldNames = new HashSet<>();
        metadata.forEach((dataset, meta) -> {
            for (final String conflictFieldName : meta.conflictFieldNames) {
                typeConflictFieldNames.add(meta.name + "." + conflictFieldName);
            }
        });
    }

    public static DatasetsMetadata empty() {
        return EMPTY_META;
    }

    public Map<String, DatasetMetadata> getDatasetToMetadata() {
        return metadata;
    }

    public Optional<DatasetMetadata> getMetadata(@Nullable final String dataset) {
        if (dataset == null) {
            return Optional.absent();
        }
        return Optional.fromNullable(metadata.get(dataset));
    }

    public Map<String, Map<String, String>> getDatasetToDimensionAliasFields() {
        return datasetToDimensionAliasFields;
    }
}
