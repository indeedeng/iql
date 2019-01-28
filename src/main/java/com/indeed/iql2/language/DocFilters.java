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

package com.indeed.iql2.language;

import com.google.common.base.Optional;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.metadata.DatasetMetadata;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql.metadata.FieldType;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.query.fieldresolution.ScopedFieldResolver;
import com.indeed.iql2.language.util.ValidationUtil;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.indeed.iql2.language.DocMetrics.extractPlainDimensionDocMetric;

public class DocFilters {
    public static DocFilter parseDocFilter(
            final JQLParser.DocFilterContext docFilterContext,
            final Query.Context context) {
        if (docFilterContext.jqlDocFilter() != null) {
            return parseJQLDocFilter(docFilterContext.jqlDocFilter(), context);
        }
        if (docFilterContext.legacyDocFilter() != null) {
            return parseLegacyDocFilter(docFilterContext.legacyDocFilter(), context.fieldResolver, context.datasetsMetadata);
        }
        throw new UnsupportedOperationException("What do?!");
    }

    public static DocFilter parseLegacyDocFilter(
            final JQLParser.LegacyDocFilterContext legacyDocFilterContext,
            final ScopedFieldResolver fieldResolver,
            final DatasetsMetadata datasetsMetadata) {
        final DocFilter[] ref = new DocFilter[1];

        legacyDocFilterContext.enterRule(new JQLBaseListener() {
            public void accept(DocFilter value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            @Override
            public void enterLegacyDocBetween(JQLParser.LegacyDocBetweenContext ctx) {
                final long lowerBound = Long.parseLong(ctx.lowerBound.getText());
                final long upperBound = Long.parseLong(ctx.upperBound.getText());
                accept(fieldResolver.resolveDocFilter(ctx.field, new ScopedFieldResolver.BetweenCallback(lowerBound, upperBound, true)));
            }

            @Override
            public void enterLegacyDocFieldIn(JQLParser.LegacyDocFieldInContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.field);
                final List<JQLParser.LegacyTermValContext> terms = ctx.terms;
                final boolean negate = ctx.not != null;
                final ArrayList<Term> termsList = new ArrayList<>();
                for (final JQLParser.LegacyTermValContext term : terms) {
                    termsList.add(Term.parseLegacyTerm(term));
                }
                accept(docInHelper(datasetsMetadata, field, negate, termsList, true));
            }

            @Override
            public void enterLegacyDocFieldIsnt(JQLParser.LegacyDocFieldIsntContext ctx) {
                final Term term = Term.parseLegacyTerm(ctx.legacyTermVal());
                accept(fieldResolver.resolveDocFilter(ctx.field, new ScopedFieldResolver.FieldIsntCallback(term)));
            }

            @Override
            public void enterLegacyDocSample(JQLParser.LegacyDocSampleContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.field);
                final long numerator = Long.parseLong(ctx.numerator.getText());
                final long denominator;
                if (ctx.denominator != null) {
                    denominator = Long.parseLong(ctx.denominator.getText());
                } else {
                    denominator = 100;
                }
                final String seed;
                if (ctx.seed != null) {
                    seed = ParserCommon.unquote(ctx.seed.getText());
                } else {
                    seed = String.valueOf(Math.random());
                }
                final FieldType fieldType = fieldResolver.fieldType(field);
                accept(new DocFilter.Sample(field, fieldType == FieldType.Integer, numerator, denominator, seed));
            }

            @Override
            public void enterLegacyDocNot(JQLParser.LegacyDocNotContext ctx) {
                accept(new DocFilter.Not(parseLegacyDocFilter(ctx.legacyDocFilter(), fieldResolver, datasetsMetadata)));
            }

            @Override
            public void enterLegacyDocRegex(JQLParser.LegacyDocRegexContext ctx) {
                accept(new DocFilter.Regex(fieldResolver.resolve(ctx.field), ParserCommon.unquote(ctx.STRING_LITERAL().getText())));
            }

            @Override
            public void enterLegacyDocFieldIs(JQLParser.LegacyDocFieldIsContext ctx) {
                final Term term = Term.parseLegacyTerm(ctx.legacyTermVal());
                accept(fieldResolver.resolveDocFilter(ctx.field, new ScopedFieldResolver.FieldIsCallback(term)));
            }

            @Override
            public void enterLegacyDocLuceneFieldIs(JQLParser.LegacyDocLuceneFieldIsContext ctx) {
                final DocFilter.FieldIs fieldIs = new DocFilter.FieldIs(fieldResolver.resolve(ctx.field), Term.parseLegacyTerm(ctx.legacyTermVal()));
                if (ctx.negate == null) {
                    accept(fieldIs);
                } else {
                    accept(new DocFilter.Not(fieldIs));
                }
            }

            @Override
            public void enterLegacyDocOr(final JQLParser.LegacyDocOrContext ctx) {
                final DocFilter left = parseLegacyDocFilter(ctx.legacyDocFilter(0), fieldResolver, datasetsMetadata);
                final DocFilter right = parseLegacyDocFilter(ctx.legacyDocFilter(1), fieldResolver, datasetsMetadata);
                accept(DocFilter.Or.create(left, right));
            }

            @Override
            public void enterLegacyDocTrue(JQLParser.LegacyDocTrueContext ctx) {
                accept(new DocFilter.Always());
            }

            @Override
            public void enterLegacyDocMetricInequality(JQLParser.LegacyDocMetricInequalityContext ctx) {
                final String op = ctx.op.getText();
                final DocMetric arg1 = DocMetrics.parseLegacyDocMetric(ctx.legacyDocMetric(0), fieldResolver, datasetsMetadata);
                final DocMetric arg2 = DocMetrics.parseLegacyDocMetric(ctx.legacyDocMetric(1), fieldResolver, datasetsMetadata);
                final DocFilter result;
                switch (op) {
                    case "=": {
                        result = new DocFilter.MetricEqual(arg1, arg2);
                        break;
                    }
                    case "!=": {
                        result = new DocFilter.MetricNotEqual(arg1, arg2);
                        break;
                    }
                    case "<": {
                        result = new DocFilter.MetricLt(arg1, arg2);
                        break;
                    }
                    case "<=": {
                        result = new DocFilter.MetricLte(arg1, arg2);
                        break;
                    }
                    case ">": {
                        result = new DocFilter.MetricGt(arg1, arg2);
                        break;
                    }
                    case ">=": {
                        result = new DocFilter.MetricGte(arg1, arg2);
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Unknown doc metric operator: " + op);
                }
                accept(result);
            }

            @Override
            public void enterLegacyDocAnd(final JQLParser.LegacyDocAndContext ctx) {
                final DocFilter left = parseLegacyDocFilter(ctx.legacyDocFilter(0), fieldResolver, datasetsMetadata);
                final DocFilter right = parseLegacyDocFilter(ctx.legacyDocFilter(1), fieldResolver, datasetsMetadata);
                accept(DocFilter.And.create(left, right));
            }

            @Override
            public void enterLegacyLucene(JQLParser.LegacyLuceneContext ctx) {
                accept(new DocFilter.Lucene(ParserCommon.unquote(ctx.STRING_LITERAL().getText()), fieldResolver, datasetsMetadata));
            }

            @Override
            public void enterLegacyDocNotRegex(JQLParser.LegacyDocNotRegexContext ctx) {
                accept(new DocFilter.NotRegex(fieldResolver.resolve(ctx.field), ParserCommon.unquote(ctx.STRING_LITERAL().getText())));
            }

            @Override
            public void enterLegacyDocFilterParens(JQLParser.LegacyDocFilterParensContext ctx) {
                accept(parseLegacyDocFilter(ctx.legacyDocFilter(), fieldResolver, datasetsMetadata));
            }

            @Override
            public void enterLegacyDocFalse(JQLParser.LegacyDocFalseContext ctx) {
                accept(new DocFilter.Never());
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled doc filter: [" + legacyDocFilterContext.getText() + "]");
        }

        ref[0].copyPosition(legacyDocFilterContext);

        return ref[0];
    }

    public static DocFilter parseJQLDocFilter(
            final JQLParser.JqlDocFilterContext docFilterContext,
            final Query.Context context) {
        final DocFilter[] ref = new DocFilter[1];
        final ScopedFieldResolver fieldResolver = context.fieldResolver;

        docFilterContext.enterRule(new JQLBaseListener() {
            public void accept(DocFilter value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            @Override
            public void enterDocBetween(JQLParser.DocBetweenContext ctx) {
                final long lowerBound = Long.parseLong(ctx.lowerBound.getText());
                final long upperBound = Long.parseLong(ctx.upperBound.getText());
                accept(fieldResolver.resolveDocFilter(ctx.singlyScopedField(), new ScopedFieldResolver.BetweenCallback(lowerBound, upperBound, false)));
            }

            @Override
            public void enterDocFieldIn(JQLParser.DocFieldInContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.singlyScopedField());
                final List<JQLParser.JqlTermValContext> terms = ctx.terms;
                final boolean negate = ctx.not != null;
                final ArrayList<Term> termsList = new ArrayList<>();
                for (final JQLParser.JqlTermValContext term : terms) {
                    termsList.add(Term.parseJqlTerm(term));
                }
                accept(field.wrap(docInHelper(context.datasetsMetadata, field, negate, termsList, false)));
            }

            @Override
            public void enterDocFieldInQuery(JQLParser.DocFieldInQueryContext ctx) {
                final JQLParser.QueryNoSelectContext queryCtx = ctx.queryNoSelect();
                final Query query = Query.parseSubquery(queryCtx, context);
                final FieldSet field = fieldResolver.resolve(ctx.singlyScopedField());
                accept(field.wrap(new DocFilter.FieldInQuery(query, field, ctx.not != null)));
            }

            @Override
            public void enterDocSample(JQLParser.DocSampleContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.singlyScopedField());
                final long numerator = Long.parseLong(ctx.numerator.getText());
                final long denominator;
                if (ctx.denominator != null) {
                    denominator = Long.parseLong(ctx.denominator.getText());
                } else {
                    denominator = 100;
                }
                final String seed;
                if (ctx.seed != null) {
                    seed = ParserCommon.unquote(ctx.seed.getText());
                } else {
                    seed = String.valueOf(Math.random());
                }
                final FieldType fieldType = fieldResolver.fieldType(field);
                accept(field.wrap(new DocFilter.Sample(field, fieldType == FieldType.Integer, numerator, denominator, seed)));
            }

            @Override
            public void enterDocSampleMetric(final JQLParser.DocSampleMetricContext ctx) {
                final DocMetric metric = DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(), context);
                final long numerator = Long.parseLong(ctx.numerator.getText());
                final long denominator;
                if (ctx.denominator != null) {
                    denominator = Long.parseLong(ctx.denominator.getText());
                } else {
                    denominator = 100;
                }
                final String seed;
                if (ctx.seed != null) {
                    seed = ParserCommon.unquote(ctx.seed.getText());
                } else {
                    seed = String.valueOf(Math.random());
                }
                accept(new DocFilter.SampleDocMetric(metric, numerator, denominator, seed));
            }

            @Override
            public void enterDocNot(JQLParser.DocNotContext ctx) {
                accept(new DocFilter.Not(parseJQLDocFilter(ctx.jqlDocFilter(), context)));
            }

            @Override
            public void enterDocRegex(JQLParser.DocRegexContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.singlyScopedField());
                accept(field.wrap(new DocFilter.Regex(field, ParserCommon.unquote(ctx.STRING_LITERAL().getText()))));
            }

            @Override
            public void enterDocFieldEqual(final JQLParser.DocFieldEqualContext ctx) {
                final DocMetric metric1 = extractPlainDimensionDocMetric(ctx.singlyScopedField(0), fieldResolver);
                final DocMetric metric2 = extractPlainDimensionDocMetric(ctx.singlyScopedField(1), fieldResolver);

                final FieldSet plainField1 = extractPlainField(metric1);
                final FieldSet plainField2 = extractPlainField(metric2);

                final DocFilter result;
                if (plainField1 != null && plainField2 != null) {
                    ValidationUtil.validateSameScopeThrowException(plainField1, plainField2);
                    result = plainField1.wrap(new DocFilter.FieldEqual(plainField1, plainField2));
                } else {
                    ValidationUtil.validateSameQualifieds(ctx, metric1, metric2);
                    result = new DocFilter.MetricEqual(metric1, metric2);
                }

                accept(result);
            }

            @Override
            public void enterDocNotFieldEqual(final JQLParser.DocNotFieldEqualContext ctx) {
                final DocMetric metric1 = extractPlainDimensionDocMetric(ctx.singlyScopedField(0), fieldResolver);
                final DocMetric metric2 = extractPlainDimensionDocMetric(ctx.singlyScopedField(1), fieldResolver);

                final FieldSet plainField1 = extractPlainField(metric1);
                final FieldSet plainField2 = extractPlainField(metric2);

                final DocFilter result;
                if (plainField1 != null && plainField2 != null) {
                    ValidationUtil.validateSameScopeThrowException(plainField1, plainField2);
                    result = plainField1.wrap(new DocFilter.Not(new DocFilter.FieldEqual(plainField1, plainField2)));
                } else {
                    ValidationUtil.validateSameQualifieds(ctx, metric1, metric2);
                    result = new DocFilter.MetricNotEqual(metric1, metric2);
                }

                accept(result);
            }

            @Override
            public void enterDocFieldIs(JQLParser.DocFieldIsContext ctx) {
                final Term term = Term.parseJqlTerm(ctx.jqlTermVal());
                accept(fieldResolver.resolveDocFilter(ctx.singlyScopedField(), new ScopedFieldResolver.FieldIsCallback(term)));
            }

            @Override
            public void enterDocFieldIsnt(JQLParser.DocFieldIsntContext ctx) {
                final Term term = Term.parseJqlTerm(ctx.jqlTermVal());
                accept(fieldResolver.resolveDocFilter(ctx.singlyScopedField(), new ScopedFieldResolver.FieldIsntCallback(term)));
            }

            @Override
            public void enterDocOr(final JQLParser.DocOrContext ctx) {
                final DocFilter left = parseJQLDocFilter(ctx.jqlDocFilter(0), context);
                final DocFilter right = parseJQLDocFilter(ctx.jqlDocFilter(1), context);
                accept(DocFilter.Or.create(left, right));
            }

            @Override
            public void enterDocTrue(JQLParser.DocTrueContext ctx) {
                accept(new DocFilter.Always());
            }

            @Override
            public void enterDocMetricInequality(JQLParser.DocMetricInequalityContext ctx) {
                final String op = ctx.op.getText();
                final DocMetric arg1 = DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(0), context);
                final DocMetric arg2 = DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(1), context);
                final DocFilter result;
                switch (op) {
                    case "=": {
                        result = new DocFilter.MetricEqual(arg1, arg2);
                        break;
                    }
                    case "!=": {
                        result = new DocFilter.MetricNotEqual(arg1, arg2);
                        break;
                    }
                    case "<": {
                        result = new DocFilter.MetricLt(arg1, arg2);
                        break;
                    }
                    case "<=": {
                        result = new DocFilter.MetricLte(arg1, arg2);
                        break;
                    }
                    case ">": {
                        result = new DocFilter.MetricGt(arg1, arg2);
                        break;
                    }
                    case ">=": {
                        result = new DocFilter.MetricGte(arg1, arg2);
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Unknown doc metric operator: " + op);
                }
                accept(result);
            }

            @Override
            public void enterDocAnd(JQLParser.DocAndContext ctx) {
                final DocFilter left = parseJQLDocFilter(ctx.jqlDocFilter(0), context);
                final DocFilter right = parseJQLDocFilter(ctx.jqlDocFilter(1), context);
                accept(DocFilter.And.create(left, right));
            }

            @Override
            public void enterLucene(JQLParser.LuceneContext ctx) {
                accept(new DocFilter.Lucene(ParserCommon.unquote(ctx.STRING_LITERAL().getText()), context.fieldResolver, context.datasetsMetadata));
            }

            @Override
            public void enterDocNotRegex(JQLParser.DocNotRegexContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.singlyScopedField());
                accept(field.wrap(new DocFilter.NotRegex(field, ParserCommon.unquote(ctx.STRING_LITERAL().getText()))));
            }

            @Override
            public void enterDocFilterParens(JQLParser.DocFilterParensContext ctx) {
                accept(parseJQLDocFilter(ctx.jqlDocFilter(), context));
            }

            @Override
            public void enterDocFalse(JQLParser.DocFalseContext ctx) {
                accept(new DocFilter.Never());
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled doc filter: [" + docFilterContext.getText() + "], " + docFilterContext.toStringTree(new JQLParser(null)));
        }

        ref[0].copyPosition(docFilterContext);

        return ref[0];
    }

    @Nullable
    static FieldSet extractPlainField(DocMetric docMetric) {
        if (docMetric instanceof DocMetric.Field) {
            return ((DocMetric.Field) docMetric).field;
        }
        if (docMetric instanceof DocMetric.Qualified) {
            final DocMetric.Qualified qualified = (DocMetric.Qualified) docMetric;
            final FieldSet innerResult = extractPlainField(qualified.metric);
            if (innerResult != null) {
                return innerResult.subset(Collections.singleton(qualified.dataset));
            }
        }
        return null;
    }

    public static DocFilter docInHelper(
            final DatasetsMetadata datasetsMetadata,
            final FieldSet field,
            final boolean negate,
            final List<Term> termsList,
            final boolean isLegacy) {
        // In legacy mode we determine field type by actual dataset metadata.
        // In IQL2 mode we determine field type by terms type.
        final boolean isStringField = isLegacy ? isStringField(field, datasetsMetadata, termsList) : anyIsString(termsList);
        final DocFilter filter;
        if (isStringField) {
            final Set<String> termSet = new HashSet<>();
            for (final Term term : termsList) {
                if (term.isIntTerm) {
                    termSet.add(String.valueOf(term.intTerm));
                } else {
                    termSet.add(term.stringTerm);
                }
            }
            filter = new DocFilter.StringFieldIn(datasetsMetadata, field, termSet);
        } else {
            final Set<Long> termSet = new LongOpenHashSet();
            for (final Term term : termsList) {
                if (term.isIntTerm) {
                    termSet.add(term.intTerm);
                } else {
                    try {
                        final long longTerm = Long.parseLong(term.stringTerm);
                        termSet.add(longTerm);
                    } catch (final NumberFormatException ignored) {
                        throw new IqlKnownException.FieldTypeMismatchException(
                                "A non integer value '" + term.stringTerm +
                                "' specified for an integer field: " + field);
                    }
                }
            }
            filter = new DocFilter.IntFieldIn(datasetsMetadata, field, termSet);
        }
        if (negate) {
            return new DocFilter.Not(filter);
        } else {
            return filter;
        }
    }

    private static boolean anyIsString(final List<Term> terms) {
        return terms.stream().anyMatch(t -> !t.isIntTerm);
    }

    private static boolean isStringField(
            final FieldSet field,
            final DatasetsMetadata datasetsMetadata,
            final List<Term> terms) {
        boolean hasStringField = false;
        boolean hasIntField = false;
        for (final String dataset : field.datasets()) {
            final String fieldName = field.datasetFieldName(dataset);
            final Optional<DatasetMetadata> metadata = datasetsMetadata.getMetadata(dataset);
            if (!metadata.isPresent()) {
                throw new IllegalStateException("Can't find metadata for dataset " + dataset);
            }
            if (metadata.get().hasStringField(fieldName)) {
                hasStringField = true;
            }
            if (metadata.get().hasIntField(fieldName)) {
                hasIntField = true;
            }
        }
        if (hasStringField ^ hasIntField) {
            // one is true and another is false.
            return hasStringField;
        }

        // conflicting field or no field found,
        // determining type by terms.
        return anyIsString(terms);
    }
}
