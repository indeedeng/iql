package com.indeed.squall.iql2.language.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.indeed.squall.iql2.language.dimensions.Dimension;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * all fields are case insensitive
 */
public class DatasetMetadata {
    public final String datasetName;
    public final String description;
    public final Set<FieldMetadata> intFields;
    public final Set<FieldMetadata> stringFields;
    public final Map<String, Dimension> fieldToDimension;

    public DatasetMetadata(String datasetName, String description) {
        this.datasetName = datasetName;
        intFields = ImmutableSet.of();
        stringFields = ImmutableSet.of();
        fieldToDimension = ImmutableMap.of();
        this.description = description;
    }

    public DatasetMetadata(final String datasetName, final String description, final Set<FieldMetadata> intFields, final Set<FieldMetadata> stringFields,
                           final Map<String, Dimension> fieldToDimension) {
        this.datasetName = datasetName;
        this.intFields = toCaseInsensitive(intFields);
        this.stringFields = toCaseInsensitive(stringFields);

        final Map<String, Dimension> caseInsensitiveMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        caseInsensitiveMap.putAll(fieldToDimension);
        this.fieldToDimension = caseInsensitiveMap;
        this.description = description;
    }

    private Set<FieldMetadata> toCaseInsensitive(final Set<FieldMetadata> set) {
        final TreeSet<FieldMetadata> caseInsensitiveSet = new TreeSet<>(FieldMetadata.CASE_INSENSITIVE_ORDER);
        caseInsensitiveSet.addAll(set);
        return caseInsensitiveSet;
    }
}
