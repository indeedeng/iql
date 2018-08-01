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

import com.google.common.base.Optional;
import com.indeed.iql.metadata.DatasetMetadata;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 *
 */
public class DatasetsMetadata {
    private final Map<String, DatasetMetadata> metadata;
    private final Map<String, Map<String, String>> datasetToDimensionAliasFields;
    private static final DatasetsMetadata EMPTY_META = new DatasetsMetadata();

    private DatasetsMetadata() {
        metadata = Collections.emptyMap();
        datasetToDimensionAliasFields = Collections.emptyMap();
    }

    public DatasetsMetadata(final Map<String, DatasetMetadata> metadata) {
        this.metadata = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.metadata.putAll(metadata);
        datasetToDimensionAliasFields = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        metadata.forEach((dataset, meta) -> {
            datasetToDimensionAliasFields.put(dataset, meta.fieldToDimension.entrySet()
                    .stream().filter(dimension -> dimension.getValue().isAlias)
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getAliasActualField().get())));
        });
    }

    public static DatasetsMetadata empty() {
        return EMPTY_META;
    }

    public Map<String, DatasetMetadata> getDatasetToMetadata() {
        return metadata;
    }

    public Optional<DatasetMetadata> getMetadata(final String dataset) {
        return Optional.fromNullable(metadata.get(dataset));
    }

    public Map<String, Map<String, String>> getDatasetToDimensionAliasFields() {
        return datasetToDimensionAliasFields;
    }
}
