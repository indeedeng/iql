package com.indeed.squall.iql2.language.util;

import com.google.common.collect.ImmutableSet;
import com.indeed.flamdex.query.Query;
import com.indeed.squall.iql2.language.actions.StringOrAction;
import com.indeed.squall.iql2.language.compat.Consumer;

import java.util.Map;

public class ValidationUtil {
    public static DatasetsFields findFieldsUsed(Map<String, Query> perDatasetQuery) {
        final DatasetsFields.Builder builder = DatasetsFields.builder();
        for (final Map.Entry<String, Query> entry : perDatasetQuery.entrySet()) {
            final String dataset = entry.getKey();
            final Query query = entry.getValue();
            final DatasetsFields fieldsUsed = findFieldsUsed(dataset, query);
            for (final String field : fieldsUsed.getIntFields(dataset)) {
                builder.addIntField(dataset, field);
            }
            for (final String field : fieldsUsed.getStringFields(dataset)) {
                builder.addStringField(dataset, field);
            }
        }
        return builder.build();
    }

    // TODO: DatasetsFields is overkill, but convenient..
    private static DatasetsFields findFieldsUsed(String datasetToUse, Query query) {
        final DatasetsFields.Builder builder = DatasetsFields.builder();
        findFieldsUsed(datasetToUse, query, builder);
        return builder.build();
    }

    private static void findFieldsUsed(String dataset, Query query, DatasetsFields.Builder builder) {
        switch (query.getQueryType()) {
            case TERM:
            case RANGE:
                final String fieldName = query.getStartTerm().getFieldName();
                final boolean isIntField = query.getStartTerm().isIntField();
                if (isIntField) {
                    builder.addIntField(dataset, fieldName);
                } else {
                    builder.addStringField(dataset, fieldName);
                }
                break;
            case BOOLEAN:
                for (final Query q : query.getOperands()) {
                    findFieldsUsed(dataset, q, builder);
                }
                break;
        }
    }

    public static void ensureSubset(DatasetsFields superset, DatasetsFields subset, Consumer<String> errorConsumer, Object source) {
        for (final String dataset : subset.datasets()) {
            final ImmutableSet<String> expectedStringFields = subset.getStringFields(dataset);
            final ImmutableSet<String> actualStringFields = superset.getStringFields(dataset);
            for (final String field : expectedStringFields) {
                if (!actualStringFields.contains(field)) {
                    errorConsumer.accept("Dataset \"" + dataset + "\" does not contain expected string field \"" + field + "\" in ["  + source + "]");
                }
            }

            final ImmutableSet<String> expectedIntFields = subset.getIntFields(dataset);
            final ImmutableSet<String> actualIntFields = superset.getIntFields(dataset);
            for (final String field : expectedIntFields) {
                if (!actualIntFields.contains(field)) {
                    errorConsumer.accept("Dataset \"" + dataset + "\" does not contain expected int field \"" + field + "\" in ["  + source + "]");
                }
            }
        }
    }
}
