package com.indeed.squall.iql2.language;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.indeed.squall.iql2.language.dimensions.Dimension;
import com.indeed.squall.iql2.language.metadata.DatasetMetadata;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * @author jwolfe
 */
public class DatasetDescriptor {
    private static final Logger log = Logger.getLogger(DatasetDescriptor.class);

    final String name;
    final String description;
    final ImmutableList<FieldDescriptor> fields;
    // for IQL1 convention
    final ImmutableList<Dimension> metrics;

    public DatasetDescriptor(String name, String description, List<FieldDescriptor> fields, ImmutableList<Dimension> dimensions) {
        this.name = name;
        this.description = description;
        this.fields = ImmutableList.copyOf(fields);
        this.metrics = dimensions;
    }

    public static DatasetDescriptor from(final String dataset, final DatasetMetadata datasetMetadata) {
        final List<FieldDescriptor> fields = Lists.newArrayList();

        for (final String field : datasetMetadata.intFields) {
            fields.add(new FieldDescriptor(field, "", "Integer"));
        }
        for (final String field : datasetMetadata.stringFields) {
            fields.add(new FieldDescriptor(field, "", "String"));
        }

        return new DatasetDescriptor(dataset, "", fields, ImmutableList.copyOf(datasetMetadata.fieldToDimension.values()));
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public List<FieldDescriptor> getFields() {
        //noinspection ReturnOfCollectionOrArrayField
        return fields;
    }

    public List<Dimension> getMetrics() {
        return metrics;
    }
}
