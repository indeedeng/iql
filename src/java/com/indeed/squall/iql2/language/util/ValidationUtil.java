package com.indeed.squall.iql2.language.util;


import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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

    public static void ensureSubset(DatasetsFields superset, DatasetsFields subset, Validator validator, Object source, boolean allowStringFieldsForInts) {
        for (final String dataset : subset.datasets()) {
            final ImmutableSet<String> expectedStringFields = subset.getStringFields(dataset);
            final ImmutableSet<String> actualStringFields = superset.getStringFields(dataset);
            for (final String field : expectedStringFields) {
                if (!actualStringFields.contains(field)) {
                    validator.error("Dataset \"" + dataset + "\" does not contain expected string field \"" + field + "\" in ["  + source + "]");
                }
            }

            final ImmutableSet<String> expectedIntFields = subset.getIntFields(dataset);
            final ImmutableSet<String> actualIntFields = superset.getIntFields(dataset);
            for (final String field : expectedIntFields) {
                if (!actualIntFields.contains(field)) {
                    if (!(allowStringFieldsForInts && actualStringFields.contains(field))) {
                        validator.error("Dataset \"" + dataset + "\" does not contain expected int field \"" + field + "\" in ["  + source + "]");
                    }
                }
            }
        }
    }

    public static void validateScope(Collection<String> scope, DatasetsFields datasetsFields, Validator validator) {
        for (final String s : scope) {
            if (!datasetsFields.datasets().contains(s)) {
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
        if (type1 == FieldType.NULL || type2 == FieldType.NULL || type1 != type2 ) {
            validator.error(String.format("incompatible fields found in fieldequal: [%s -> %s], [%s -> %s]", field1, type1, field2, type2));
        }
    }

    private static FieldType getFieldType(DatasetsFields datasetsFields, String dataset, String field) {
        final boolean isIntField = datasetsFields.getIntFields(dataset).contains(field);
        final boolean isStrField = datasetsFields.getStringFields(dataset).contains(field);
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

    public static Query getFlamdexQuery(final String query, final String dataset, final Map<String, Set<String>> datasetToKeywordAnalyzerFields, final Map<String, Set<String>> datasetToIntFields) {
        final Analyzer analyzer;
        // TODO: Detect if imhotep index and use KeywordAnalyzer always in that case..?
        if (datasetToKeywordAnalyzerFields.containsKey(dataset)) {
            final KeywordAnalyzer kwAnalyzer = new KeywordAnalyzer();
            final Set<String> whitelist = datasetToKeywordAnalyzerFields.get(dataset);
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
            final Set<String> caseInsensitiveIntFields = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            caseInsensitiveIntFields.addAll(datasetToIntFields.get(dataset));
            return LuceneQueryTranslator.rewrite(parsed, caseInsensitiveIntFields);
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
