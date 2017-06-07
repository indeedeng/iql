package com.indeed.squall.iql2.language.util;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.indeed.common.util.StringUtils;
import com.indeed.flamdex.lucene.LuceneQueryTranslator;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.automaton.RegExp;
import com.indeed.imhotep.automaton.RegexTooComplexException;
import com.indeed.squall.iql2.language.Validator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;

import java.util.Collection;
import java.util.Collections;
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

    public static void validateQuery(DatasetsFields datasetFields, Map<String, Query> perDatasetQuery, Validator validator, Object source, boolean allowStringFieldsForInts) {
        Map<String, Set<String>> datasetToIntFields = new HashMap<>();
        Map<String, Set<String>> datasetToStringFields = new HashMap<>();
        for (Map.Entry<String, Query> datasetEntry : perDatasetQuery.entrySet()) {
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
                    if (!(allowStringFieldsForInts && datasetFields.containsStringField(dataset, field))) {
                        if (datasetFields.containsMetricField(dataset, field)) {
                            validator.error(ErrorMessages.metricFieldIsNotSupported(field, source));
                            continue;
                        }
                        validator.error("Dataset \"" + dataset + "\" does not contain expected int field \"" + field + "\" in [" + source + "]");
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
                validator.error(ErrorMessages.missingIntField(dataset, field, context));
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

    private static Term uppercaseTerm(Term term) {
        if (term.isIntField()) {
            return Term.intTerm(term.getFieldName().toUpperCase(), term.getTermIntVal());
        } else {
            return Term.stringTerm(term.getFieldName().toUpperCase(), term.getTermStringVal());
        }
    }

    public static Query uppercaseTermQuery(Query query) {
        if (query.getOperands() == null) {
            final Term startTerm = query.getStartTerm();
            final Term endTerm = query.getEndTerm();
            if (endTerm == null) {
                return Query.newTermQuery(uppercaseTerm(startTerm));
            } else {
                return Query.newRangeQuery(
                        uppercaseTerm(startTerm), uppercaseTerm(endTerm), query.isMaxInclusive());
            }
        } else {
            List<Query> upperOperands = Lists.newArrayListWithCapacity(query.getOperands().size());
            for (Query operand : query.getOperands()) {
                upperOperands.add(uppercaseTermQuery(operand));
            }
            return Query.newBooleanQuery(query.getOperator(), upperOperands);
        }
    }

    public static Query getFlamdexQuery(final String query, final String dataset,
                                        final Map<String, Set<String>> keywordAnalyzerFields,
                                        final Map<String, Set<String>> datasetToIntFields) {
        final Analyzer analyzer;
        // TODO: Detect if imhotep index and use KeywordAnalyzer always in that case..?
        if (keywordAnalyzerFields.containsKey(dataset)) {
            final KeywordAnalyzer kwAnalyzer = new KeywordAnalyzer();
            final Set<String> whitelist = keywordAnalyzerFields.get(dataset);
            if (whitelist.contains("*")) {
                analyzer = kwAnalyzer;
            } else {
                final PerFieldAnalyzerWrapper perFieldAnalyzerWrapper = new PerFieldAnalyzerWrapper(new WhitespaceAnalyzer());
                for (final String field : whitelist) {
                    perFieldAnalyzerWrapper.addAnalyzer(field, kwAnalyzer);
                }
                analyzer = perFieldAnalyzerWrapper;
            }
        } else {
            analyzer = new WhitespaceAnalyzer();
        }
        final QueryParser qp = new QueryParser("foo", analyzer);
        qp.setDefaultOperator(QueryParser.Operator.AND);
        final org.apache.lucene.search.Query parsed;
        try {
            parsed = qp.parse(query);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Could not parse lucene term: " + query, e);
        }
        if (!datasetToIntFields.containsKey(dataset)) {
            return LuceneQueryTranslator.rewrite(parsed, Collections.<String>emptySet());
        } else {
            return LuceneQueryTranslator.rewrite(parsed, datasetToIntFields.get(dataset));
        }
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
