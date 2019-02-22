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

package com.indeed.iql2.language.util;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.automaton.Automaton;
import com.indeed.imhotep.automaton.RegExp;
import com.indeed.imhotep.automaton.RegexTooComplexException;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.JQLParser;
import com.indeed.iql2.language.TimePeriods;
import com.indeed.iql2.language.passes.ExtractQualifieds;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import com.indeed.util.core.Pair;
import org.antlr.v4.runtime.RuleContext;
import org.apache.commons.lang.StringUtils;
import org.joda.time.format.DateTimeFormat;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

public class ValidationUtil {
    private ValidationUtil() {
    }

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

    public static void validateQuery(ValidationHelper validationHelper, Map<String, Query> perDatasetQuery, ErrorCollector errorCollector, Object source) {
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
                if (!validationHelper.containsStringField(dataset, field)) {
                    if (validationHelper.containsMetricField(dataset, field)) {
                        errorCollector.error(ErrorMessages.metricFieldIsNotSupported(field, source));
                        continue;
                    }
                    errorCollector.error("Dataset \"" + dataset + "\" does not contain expected string field \"" + field + "\" in [" + source + "]");
                }
            }

            for (final String field : datasetToIntFields.get(dataset)) {
                validationHelper.validateIntField(dataset, field, errorCollector, source);
            }
        }
    }

    public static void validateScope(Collection<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
        for (final String s : scope) {
            if (!validationHelper.datasets().contains(s)) {
                errorCollector.error(ErrorMessages.missingDataset(s));
            }
        }
    }

    public static void validateDataset(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
        if (!validationHelper.datasets().contains(dataset)) {
            errorCollector.error(ErrorMessages.missingDataset(dataset));
        }
    }

    public static void validateSameScopeThrowException(List<String> scope1, List<String> scope2) {
        if (!Objects.equal(scope1, scope2)) {
            throw new IqlKnownException.ParseErrorException(ErrorMessages.scopeMismatch(StringUtils.join(scope1, "."), StringUtils.join(scope2, ".")));
        }
    }

    public static void validateSameScopeThrowException(FieldSet scope1, FieldSet scope2) {
        if (!Objects.equal(scope1.datasets(), scope2.datasets())) {
            throw new IqlKnownException.ParseErrorException(ErrorMessages.scopeMismatch(StringUtils.join(scope1.datasets(), "."), StringUtils.join(scope2.datasets(), ".")));
        }
    }

    public static void validateSameQualifieds(RuleContext context, DocMetric m1, DocMetric m2) {
        final Set<String> qualifications = Sets.union(ExtractQualifieds.extractDocMetricDatasets(m1), ExtractQualifieds.extractDocMetricDatasets(m2));
        if (qualifications.size() > 1) {
            throw new IqlKnownException.ParseErrorException("Different qualifieds found in different branches. Context = [" + context.toStringTree(Arrays.asList(JQLParser.ruleNames)) + "], qualifications = [" + qualifications + "]");
        }
    }

    public static void validateExistenceAndSameFieldType(String dataset, String field1, String field2, ValidationHelper validationHelper, ErrorCollector errorCollector) {
        final FieldType type1 = getFieldType(validationHelper, dataset, field1);
        final FieldType type2 = getFieldType(validationHelper, dataset, field2);
        if (type1 == FieldType.NULL || type2 == FieldType.NULL || type1 != type2) {
            errorCollector.error(String.format("incompatible fields found in fieldequal: [%s -> %s], [%s -> %s]", field1, type1, field2, type2));
        }
    }

    public static void validateIntField(
            final Set<String> scope, final String field, final ValidationHelper validationHelper, final ErrorCollector errorCollector, final Object context) {
        validateField(scope, field, validationHelper, validationHelper::containsIntOrAliasField, errorCollector, context);
    }

    public static void validateStringField(
            final Set<String> scope, final String field, final ValidationHelper validationHelper, final ErrorCollector errorCollector, final Object context) {
        validateField(scope, field, validationHelper, validationHelper::containsStringField, errorCollector, context);
    }

    public static void validateField(final Set<String> scope, final String field, final ValidationHelper validationHelper, final ErrorCollector errorCollector, final Object context) {
        validateField(scope, field, validationHelper, validationHelper::containsField, errorCollector, context);
    }

    public static void validateIntField(final FieldSet field, final ValidationHelper validationHelper, final ErrorCollector errorCollector, final Object context) {
        validateField(field, validationHelper, validationHelper::containsIntOrAliasField, errorCollector, context);
    }

    public static void validateStringField(final FieldSet field, final ValidationHelper validationHelper, final ErrorCollector errorCollector, final Object context) {
        validateField(field, validationHelper, validationHelper::containsStringField, errorCollector, context);
    }

    public static void validateField(final FieldSet field, final ValidationHelper validationHelper, final ErrorCollector errorCollector, final Object context) {
        validateField(field, validationHelper, validationHelper::containsField, errorCollector, context);
    }

    private static void validateField(final FieldSet field, final ValidationHelper validationHelper, final BiPredicate<String, String> predicate, final ErrorCollector errorCollector, final Object context) {
        field.datasets().forEach(dataset -> {
            final String fieldName = field.datasetFieldName(dataset);
            if (!predicate.test(dataset, fieldName)) {
                if (validationHelper.containsNonAliasMetricField(dataset, fieldName)) {
                    errorCollector.error(ErrorMessages.nonAliasMetricInFTGS(fieldName, context));
                    return;
                }
                errorCollector.error(ErrorMessages.missingField(dataset, fieldName, context));
            }
        });
    }

    public static void validateDoubleFormatString(final String formatString, final ErrorCollector errorCollector) {
        // Don't know how to check format string.
        // Format string will be used to output doubles, so try to output any double and check for exceptions.
        // Not sure that we can catch all format errors with this approach, but believe that almost all will be caught.
        try {
            final String ignored = String.format(formatString, 0.0d);
        } catch (final Throwable t) {
            errorCollector.error("Incorrect format string: <" + formatString + ">");
        }
    }

    public static boolean isValidDateTimeFormat(final String formatString) {
        try {
            // Creating string representation of a current time to see if formatString is correct.
            final String ignored = DateTimeFormat.forPattern(formatString).withLocale(Locale.US).print(System.currentTimeMillis());
            return true;
        } catch (final Throwable t) {
            return false;
        }
    }

    public static void validateDateTimeFormat(final String formatString, final ErrorCollector errorCollector) {
        if (!isValidDateTimeFormat(formatString)) {
            errorCollector.error("Incorrect DateTime format string: <" + formatString + ">");
        }
    }

    private static void validateField(final Set<String> scope, final String field, final ValidationHelper validationHelper,
                                      final BiPredicate<String, String> containsFieldPredicate, final ErrorCollector errorCollector, final Object context) {
        scope.forEach(dataset -> {
            if (!containsFieldPredicate.test(dataset, field)) {
                if (validationHelper.containsNonAliasMetricField(dataset, field)) {
                    errorCollector.error(ErrorMessages.nonAliasMetricInFTGS(field, context));
                    return;
                }
                errorCollector.error(ErrorMessages.missingField(dataset, field, context));
            }
        });
    }

    public static void validateGroupByTimeRange(final ValidationHelper validationHelper, final long periodSeconds, final ErrorCollector errorCollector) {
        for(Map.Entry<String, Pair<Long, Long>> datasetTimeRange: validationHelper.datasetTimeRanges().entrySet()) {
            final long datasetTimePeriodSeconds = (datasetTimeRange.getValue().getSecond() - datasetTimeRange.getValue().getFirst())/1000;
            if (datasetTimePeriodSeconds%periodSeconds != 0) {
                    final StringBuilder exceptionBuilder = new StringBuilder("You requested a time period (");
                    TimePeriods.appendTimePeriod(datasetTimePeriodSeconds, exceptionBuilder);
                    exceptionBuilder.append(") for dataset ").append(datasetTimeRange.getKey());
                    exceptionBuilder.append(" not evenly divisible by the bucket size (");
                    TimePeriods.appendTimePeriod(periodSeconds, exceptionBuilder);
                    exceptionBuilder.append("). To correct, increase the time range by ");
                    TimePeriods.appendTimePeriod(periodSeconds - datasetTimePeriodSeconds%periodSeconds, exceptionBuilder);
                    exceptionBuilder.append(" or reduce the time range by ");
                    TimePeriods.appendTimePeriod(datasetTimePeriodSeconds%periodSeconds, exceptionBuilder);
                    errorCollector.error(exceptionBuilder.toString());
            }
        }
    }

    private static FieldType getFieldType(ValidationHelper validationHelper, String dataset, String field) {
        final boolean isIntField = validationHelper.containsIntOrAliasField(dataset, field);
        final boolean isStrField = validationHelper.containsStringField(dataset, field);
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

    public static Automaton compileRegex(String regex) {
        try {
            return new RegExp(regex).toAutomaton();
        } catch (Exception e) {
            Throwables.propagateIfInstanceOf(e, RegexTooComplexException.class);
            throw new IqlKnownException.ParseErrorException(
                    "The provided regex filter [" + regex + "] failed to parse."
                            + "\nError was: " + e.getMessage()
                            + "\nThe supported regex syntax can be seen here: http://www.brics.dk/automaton/doc/index.html?dk/brics/automaton/RegExp.html"
            );
        }
    }
}
