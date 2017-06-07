package com.indeed.squall.iql2.language.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.indeed.squall.iql2.language.dimensions.Dimension;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 *
 */
public class DatasetMetadata {
    public final String datasetName;
    public final Set<String> intFields;
    public final Set<String> stringFields;
    public final Map<String, Dimension> fieldToDimension;
    public final Set<String> keywordAnaylzerWhitelist;

    public DatasetMetadata(String datasetName) {
        this.datasetName = datasetName;
        intFields = ImmutableSet.of();
        stringFields = ImmutableSet.of();
        fieldToDimension = ImmutableMap.of();
        keywordAnaylzerWhitelist = ImmutableSet.of();
    }

    public DatasetMetadata(final String datasetName, final Set<String> intFields, final Set<String> stringFields,
                           final Set<String> keywordAnaylzerWhitelist, final Map<String, Dimension> fieldToDimension) {
        this.datasetName = datasetName;
        this.intFields = toCaseInsensitive(intFields);
        this.stringFields = toCaseInsensitive(stringFields);
        this.keywordAnaylzerWhitelist = toCaseInsensitive(keywordAnaylzerWhitelist);

        final Map<String, Dimension> caseInsensitiveMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        caseInsensitiveMap.putAll(fieldToDimension);
        this.fieldToDimension = caseInsensitiveMap;
    }

    private Set<String> toCaseInsensitive(final Set<String> set) {
        final TreeSet<String> caseInsensitiveSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        caseInsensitiveSet.addAll(set);
        return caseInsensitiveSet;
    }
}
