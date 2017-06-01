package com.indeed.squall.iql2.language;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.squall.iql2.language.dimensions.DatasetDimensions;
import com.indeed.squall.iql2.language.dimensions.Dimension;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author jwolfe
 */
public class DatasetDescriptor {
    private static final Logger log = Logger.getLogger(DatasetDescriptor.class);

    final String name;
    final String description;
    final ImmutableList<FieldDescriptor> fields;
    final ImmutableList<Dimension> dimensions;

    public DatasetDescriptor(String name, String description, List<FieldDescriptor> fields, ImmutableList<Dimension> dimensions) {
        this.name = name;
        this.description = description;
        this.fields = ImmutableList.copyOf(fields);
        this.dimensions = dimensions;
    }


    public static DatasetDescriptor from(DatasetInfo datasetInfo, Optional<DatasetDimensions> datasetDimensions, @Nullable Set<String> intFields) {
        if (intFields == null) {
            intFields = Collections.emptySet();
        }
        final Set<String> seenFields = Sets.newHashSet();
        final List<FieldDescriptor> fields = Lists.newArrayList();

        for (final String field : datasetInfo.getIntFields()) {
            if (!seenFields.contains(field)) {
                fields.add(new FieldDescriptor(field, "", "Integer"));
                seenFields.add(field);
            }
        }

        for (final String field : datasetInfo.getStringFields()) {
            if (!seenFields.contains(field)) {
                if (intFields.contains(field)) {
                    fields.add(new FieldDescriptor(field, "", "Integer"));
                }
                fields.add(new FieldDescriptor(field, "", "String"));
                seenFields.add(field);
            }
        }
        final ImmutableList<Dimension> dimensions;
        if (datasetDimensions.isPresent()) {
            final ImmutableList.Builder<Dimension> builder = new ImmutableList.Builder();
            final DatasetDimensions dimension = datasetDimensions.get();
            for (String uppercasedField : dimension.uppercasedFields()) {
                builder.add(dimension.getDimension(uppercasedField).get());
            }
            dimensions = builder.build();
        } else {
            dimensions = ImmutableList.of();
        }

        return new DatasetDescriptor(datasetInfo.getDataset(), "", fields, dimensions);
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

    public List<Dimension> getDimensions() {
        return dimensions;
    }
}
