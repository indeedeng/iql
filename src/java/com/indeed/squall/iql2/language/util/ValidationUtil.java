package com.indeed.squall.iql2.language.util;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.indeed.common.util.StringUtils;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.automaton.RegExp;
import com.indeed.imhotep.automaton.RegexTooComplexException;
import com.indeed.squall.iql2.language.Validator;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

public class ValidationUtil {
    private static void findFieldsUsed(Query query, Set<String> intFields, Set<String> stringFields) {
        switch (query.getQueryType()) {
            case TERM:
            case RANGE:
                final String field = query.getStartTerm().getFieldName();
                final boolean isIntField = query.getStartTerm().isIntField();
                if (isIntField) {
                    intFields.add(field);
                } else {
                    stringFields.add(field);
                }
                break;
            case BOOLEAN:
                for (final Query q : query.getOperands()) {
                    findFieldsUsed(q, intFields, stringFields);
                }
                break;
        }
    }

    public static void validateQuery(DatasetsFields datasetFields, Map<String, Query> perDatasetQuery, Validator validator, Object source) {
        final Map<String, Set<String>> datasetToIntFields = new HashMap<>();
        final Map<String, Set<String>> datasetToStringFields = new HashMap<>();
        for (final Map.Entry<String, Query> datasetEntry : perDatasetQuery.entrySet()) {
            final String dataset = datasetEntry.getKey();
            final Set<String> intFields = new HashSet<>();
            final Set<String> stringFields = new HashSet<>();
            findFieldsUsed(datasetEntry.getValue(), intFields, stringFields);
            datasetToIntFields.put(dataset, intFields);
            datasetToStringFields.put(dataset, stringFields);
        }
        for (final String dataset : perDatasetQuery.keySet()) {
            for (final String field : datasetToStringFields.get(dataset)) {
                if (!datasetFields.containsStringField(dataset, field)) {
                    if (datasetFields.containsMetricField(dataset, field)) {
                        validator.error(ErrorMessages.metricFieldIsNotSupported(field, source));
                        continue;
                    }
                    validator.error("Dataset \"" + dataset + "\" does not contain expected string field \"" + field + "\" in [" + source + "]");
                }
            }

            for (final String field : datasetToIntFields.get(dataset)) {
                if (!datasetFields.containsIntOrAliasField(dataset, field)) {
                    // special case for page as it is a string field at Imhotep, but it also needs to support int field operation
                    if ((dataset.equalsIgnoreCase("jobsearch") || dataset.equalsIgnoreCase("mobsearch"))
                            && field.equalsIgnoreCase("page")) {
                    } else if (datasetFields.containsStringField(dataset, field)) {
                        validator.warn(ErrorMessages.stringFieldMismatch(dataset, field, source));
                    } else if (datasetFields.containsMetricField(dataset, field)) {
                        validator.error(ErrorMessages.metricFieldIsNotSupported(field, source));
                    } else {
                        validator.error(ErrorMessages.missingField(dataset, field, source));
                    }
                }
            }
        }
    }

    public static void validateScope(Collection<String> scope, DatasetsFields datasetsFields, Validator validator) {
        for (final String s : scope) {
            if (!datasetsFields.datasets().contains(s.toUpperCase())) {
                validator.error(ErrorMessages.missingDataset(s));
            }
        }
    }

    public static void validateDataset(String dataset, DatasetsFields datasetsFields, Validator validator) {
        if (!datasetsFields.datasets().contains(dataset)) {
            validator.error(ErrorMessages.missingDataset(dataset));
        }
    }

    public static void validateSameScopeThrowException(List<String> scope1, List<String> scope2) {
        if (!Objects.equal(scope1, scope2)) {
            throw new IllegalArgumentException(ErrorMessages.scopeMismatch(StringUtils.join(scope1, "."), StringUtils.join(scope2, ".")));
        }
    }

    public static void validateExistenceAndSameFieldType(String dataset, String field1, String field2, DatasetsFields datasetsFields, Validator validator) {
        final FieldType type1 = getFieldType(datasetsFields, dataset, field1);
        final FieldType type2 = getFieldType(datasetsFields, dataset, field2);
        if (type1 == FieldType.NULL || type2 == FieldType.NULL || type1 != type2) {
            validator.error(String.format("incompatible fields found in fieldequal: [%s -> %s], [%s -> %s]", field1, type1, field2, type2));
        }
    }

    public static void validateIntField(
            final Set<String> scope, final String field, final DatasetsFields datasetsFields, final Validator validator, final Object context) {
        validateField(scope, field, datasetsFields, datasetsFields::containsIntOrAliasField, validator, context);
    }

    public static void validateStringField(
            final Set<String> scope, final String field, final DatasetsFields datasetsFields, final Validator validator, final Object context) {
        validateField(scope, field, datasetsFields, datasetsFields::containsStringField, validator, context);
    }

    public static void validateField(
            final Set<String> scope, final String field, final DatasetsFields datasetsFields, final Validator validator, final Object context) {
        validateField(scope, field, datasetsFields, datasetsFields::containsField, validator, context);
    }

    private static void validateField(final Set<String> scope, final String field, final DatasetsFields datasetsFields,
                                      final BiPredicate<String, String> containsFieldPredicate, final Validator validator, final Object context) {
        scope.forEach(dataset -> {
            if (!containsFieldPredicate.test(dataset, field)) {
                if (datasetsFields.containsNonAliasMetricField(dataset, field)) {
                    validator.error(ErrorMessages.nonAliasMetricInFTGS(field, context));
                    return;
                }
                validator.error(ErrorMessages.missingField(dataset, field, context));
            }
        });
    }

    private static FieldType getFieldType(DatasetsFields datasetsFields, String dataset, String field) {
        final boolean isIntField = datasetsFields.containsIntOrAliasField(dataset, field);
        final boolean isStrField = datasetsFields.containsStringField(dataset, field);
        if (isIntField) {
            return FieldType.INT;
        } else if (isStrField) {
            return FieldType.STR;
        } else {
            return FieldType.NULL;
        }
    }

    private enum FieldType {
        INT, STR, NULL
    }

    public static void compileRegex(String regex) {
        try {
            new RegExp(regex).toAutomaton();
        } catch (Exception e) {
            Throwables.propagateIfInstanceOf(e, RegexTooComplexException.class);
            throw new IllegalArgumentException(
                    "The provided regex filter [" + regex + "] failed to parse."
                            + "\nError was: " + e.getMessage()
                            + "\nThe supported regex syntax can be seen here: http://www.brics.dk/automaton/doc/index.html?dk/brics/automaton/RegExp.html"
            );
        }
    }
}
