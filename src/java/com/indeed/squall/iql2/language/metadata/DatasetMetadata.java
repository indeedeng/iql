package com.indeed.squall.iql2.language.metadata;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.language.dimensions.Dimension;

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
    public final TreeSet<FieldMetadata> intFields;
    public final TreeSet<FieldMetadata> stringFields;
    public final Map<String, Dimension> fieldToDimension;

    public DatasetMetadata(String datasetName, String description) {
        this.datasetName = datasetName;
        intFields = new TreeSet<FieldMetadata>(FieldMetadata.CASE_INSENSITIVE_ORDER);
        stringFields = new TreeSet<FieldMetadata>(FieldMetadata.CASE_INSENSITIVE_ORDER);
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
        final FieldMetadata fakeFieldMetadata = ImmutableFieldMetadata.builder().setName(field).setType(FieldMetadata.Type.Integer).build();
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
