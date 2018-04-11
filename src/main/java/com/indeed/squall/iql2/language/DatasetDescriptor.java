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

package com.indeed.squall.iql2.language;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.indeed.squall.iql2.language.dimensions.Dimension;
import com.indeed.squall.iql2.language.metadata.DatasetMetadata;
import com.indeed.squall.iql2.language.metadata.FieldMetadata;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * @author jwolfe
 */
public class DatasetDescriptor {
    private static final Logger log = Logger.getLogger(DatasetDescriptor.class);

    final String name;
    final String description;
    final ImmutableList<FieldMetadata> fields;
    // for IQL1 convention
    final ImmutableList<Dimension> metrics;

    public DatasetDescriptor(String name, String description, List<FieldMetadata> fields, ImmutableList<Dimension> dimensions) {
        this.name = name;
        this.description = description;
        this.fields = ImmutableList.copyOf(fields);
        this.metrics = dimensions;
    }

    public static DatasetDescriptor from(final String dataset, final DatasetMetadata datasetMetadata) {
        final List<FieldMetadata> fields = Lists.newArrayList();

        for (final FieldMetadata field : datasetMetadata.intFields) {
            fields.add(field);
        }
        for (final FieldMetadata field : datasetMetadata.stringFields) {
            fields.add(field);
        }

        return new DatasetDescriptor(dataset, Strings.nullToEmpty(datasetMetadata.description), fields, ImmutableList.copyOf(datasetMetadata.fieldToDimension.values()));
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public List<FieldMetadata> getFields() {
        //noinspection ReturnOfCollectionOrArrayField
        return fields;
    }

    public List<Dimension> getMetrics() {
        return metrics;
    }
}
