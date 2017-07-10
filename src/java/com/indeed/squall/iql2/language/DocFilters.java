package com.indeed.squall.iql2.language;

import com.google.common.base.Optional;
import com.indeed.common.util.time.WallClock;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.query.Query;
import com.indeed.squall.iql2.language.metadata.DatasetsMetadata;
import com.indeed.squall.iql2.language.util.ValidationUtil;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.indeed.squall.iql2.language.Identifiers.parseIdentifier;

public class DocFilters {
    public static DocFilter and(List<DocFilter> filters) {
        if (filters == null || filters.isEmpty()) {
            return new DocFilter.Always();
        }
        if (filters.size() == 1) {
            return filters.get(0);
        }
        DocFilter result = filters.get(0);
        for (int i = 1; i < filters.size(); i++) {
            result = new DocFilter.And(result, filters.get(i));
        }
        return result;
    }

    public static DocFilter parseDocFilter(JQLParser.DocFilterContext docFilterContext, DatasetsMetadata datasetsMetadata, JQLParser.FromContentsContext fromContents, Consumer<String> warn, WallClock clock) {
        if (docFilterContext.jqlDocFilter() != null) {
            return parseJQLDocFilter(docFilterContext.jqlDocFilter(), datasetsMetadata, fromContents, warn, clock);
        }
        if (docFilterContext.legacyDocFilter() != null) {
            return parseLegacyDocFilter(docFilterContext.legacyDocFilter(), datasetsMetadata);
        }
        throw new UnsupportedOperationException("What do?!");
    }

    public static DocFilter parseLegacyDocFilter(JQLParser.LegacyDocFilterContext legacyDocFilterContext, final DatasetsMetadata datasetsMetadata) {
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
                final Positioned<String> field = parseIdentifier(ctx.field);
                final long lowerBound = Long.parseLong(ctx.lowerBound.getText());
                final long upperBound = Long.parseLong(ctx.upperBound.getText());
                accept(new DocFilter.Between(field, lowerBound, upperBound));
            }

            @Override
            public void enterLegacyDocFieldIn(JQLParser.LegacyDocFieldInContext ctx) {
                final Positioned<String> field = parseIdentifier(ctx.field);
                final List<JQLParser.LegacyTermValContext> terms = ctx.terms;
                final boolean negate = ctx.not != null;
                final ArrayList<Term> termsList = new ArrayList<>();
                for (final JQLParser.LegacyTermValContext term : terms) {
                    termsList.add(Term.parseLegacyTerm(term));
                }
                accept(docInHelper(datasetsMetadata, field, negate, termsList));
            }

            @Override
            public void enterLegacyDocFieldIsnt(JQLParser.LegacyDocFieldIsntContext ctx) {
                accept(new DocFilter.FieldIsnt(datasetsMetadata, parseIdentifier(ctx.field), Term.parseLegacyTerm(ctx.legacyTermVal())));
            }

            @Override
            public void enterLegacyDocSample(JQLParser.LegacyDocSampleContext ctx) {
                final Positioned<String> field = parseIdentifier(ctx.field);
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
                accept(new DocFilter.Sample(field, numerator, denominator, seed));
            }

            @Override
            public void enterLegacyDocNot(JQLParser.LegacyDocNotContext ctx) {
                accept(new DocFilter.Not(parseLegacyDocFilter(ctx.legacyDocFilter(), datasetsMetadata)));
            }

            @Override
            public void enterLegacyDocRegex(JQLParser.LegacyDocRegexContext ctx) {
                accept(new DocFilter.Regex(parseIdentifier(ctx.field), ParserCommon.unquote(ctx.STRING_LITERAL().getText())));
            }

            @Override
            public void enterLegacyDocFieldIs(JQLParser.LegacyDocFieldIsContext ctx) {
                accept(new DocFilter.FieldIs(datasetsMetadata, parseIdentifier(ctx.field), Term.parseLegacyTerm(ctx.legacyTermVal())));
            }

            @Override
            public void enterLegacyDocLuceneFieldIs(JQLParser.LegacyDocLuceneFieldIsContext ctx) {
                final DocFilter.FieldIs fieldIs = new DocFilter.FieldIs(datasetsMetadata, parseIdentifier(ctx.field), Term.parseLegacyTerm(ctx.legacyTermVal()));
                if (ctx.negate == null) {
                    accept(fieldIs);
                } else {
                    accept(new DocFilter.Not(fieldIs));
                }
            }

            @Override
            public void enterLegacyDocOr(JQLParser.LegacyDocOrContext ctx) {
                accept(new DocFilter.Or(parseLegacyDocFilter(ctx.legacyDocFilter(0), datasetsMetadata), parseLegacyDocFilter(ctx.legacyDocFilter(1), datasetsMetadata)));
            }

            @Override
            public void enterLegacyDocTrue(JQLParser.LegacyDocTrueContext ctx) {
                accept(new DocFilter.Always());
            }

            @Override
            public void enterLegacyDocMetricInequality(JQLParser.LegacyDocMetricInequalityContext ctx) {
                final String op = ctx.op.getText();
                final DocMetric arg1 = DocMetrics.parseLegacyDocMetric(ctx.legacyDocMetric(0), datasetsMetadata);
                final DocMetric arg2 = DocMetrics.parseLegacyDocMetric(ctx.legacyDocMetric(1), datasetsMetadata);
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
            public void enterLegacyDocAnd(JQLParser.LegacyDocAndContext ctx) {
                accept(new DocFilter.And(parseLegacyDocFilter(ctx.legacyDocFilter(0), datasetsMetadata), parseLegacyDocFilter(ctx.legacyDocFilter(1), datasetsMetadata)));
            }

            @Override
            public void enterLegacyLucene(JQLParser.LegacyLuceneContext ctx) {
                accept(new DocFilter.Lucene(ParserCommon.unquote(ctx.STRING_LITERAL().getText()), datasetsMetadata));
            }

            @Override
            public void enterLegacyDocNotRegex(JQLParser.LegacyDocNotRegexContext ctx) {
                accept(new DocFilter.NotRegex(parseIdentifier(ctx.field), ParserCommon.unquote(ctx.STRING_LITERAL().getText())));
            }

            @Override
            public void enterLegacyDocFilterParens(JQLParser.LegacyDocFilterParensContext ctx) {
                accept(parseLegacyDocFilter(ctx.legacyDocFilter(), datasetsMetadata));
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
            JQLParser.JqlDocFilterContext docFilterContext,
            final DatasetsMetadata datasetsMetadata,
            final JQLParser.FromContentsContext fromContents,
            final Consumer<String> warn, final WallClock clock
    ) {
        final DocFilter[] ref = new DocFilter[1];

        docFilterContext.enterRule(new JQLBaseListener() {
            public void accept(DocFilter value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            @Override
            public void enterDocBetween(JQLParser.DocBetweenContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.singlyScopedField());
                final long lowerBound = Long.parseLong(ctx.lowerBound.getText());
                final long upperBound = Long.parseLong(ctx.upperBound.getText());
                accept(scopedField.wrap(new DocFilter.Between(scopedField.field, lowerBound, upperBound)));
            }

            @Override
            public void enterDocFieldIn(JQLParser.DocFieldInContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.singlyScopedField());
                final List<JQLParser.JqlTermValContext> terms = ctx.terms;
                final boolean negate = ctx.not != null;
                final ArrayList<Term> termsList = new ArrayList<>();
                for (final JQLParser.JqlTermValContext term : terms) {
                    termsList.add(Term.parseJqlTerm(term));
                }
                accept(scopedField.wrap(docInHelper(datasetsMetadata, scopedField.field, negate, termsList)));
            }

            @Override
            public void enterDocFieldInQuery(JQLParser.DocFieldInQueryContext ctx) {
                final JQLParser.QueryNoSelectContext queryCtx = ctx.queryNoSelect();
                final JQLParser.FromContentsContext fromUsed = queryCtx.same == null ? queryCtx.fromContents() : fromContents;
                if (fromUsed == null) {
                    throw new IllegalArgumentException("Can't use 'FROM SAME' outside of WHERE");
                }
                final Query query = Query.parseQuery(
                        fromUsed,
                        Optional.fromNullable(queryCtx.whereContents()),
                        Optional.of(queryCtx.groupByContents()),
                        Collections.<JQLParser.SelectContentsContext>emptyList(),
                        null,
                        datasetsMetadata,
                        warn,
                        clock,
                        queryCtx.fromContents().useLegacy
                );
                final ScopedField scopedField = ScopedField.parseFrom(ctx.singlyScopedField());
                accept(new DocFilter.FieldInQuery(query, scopedField, ctx.not != null));
            }

            @Override
            public void enterDocFieldIsnt(JQLParser.DocFieldIsntContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.singlyScopedField());
                accept(scopedField.wrap(new DocFilter.FieldIsnt(datasetsMetadata, scopedField.field, Term.parseJqlTerm(ctx.jqlTermVal()))));
            }

            @Override
            public void enterDocSample(JQLParser.DocSampleContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.singlyScopedField());
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
                accept(scopedField.wrap(new DocFilter.Sample(scopedField.field, numerator, denominator, seed)));
            }

            @Override
            public void enterDocNot(JQLParser.DocNotContext ctx) {
                accept(new DocFilter.Not(parseJQLDocFilter(ctx.jqlDocFilter(), datasetsMetadata, fromContents, warn, clock)));
            }

            @Override
            public void enterDocRegex(JQLParser.DocRegexContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.singlyScopedField());
                accept(scopedField.wrap(new DocFilter.Regex(scopedField.field, ParserCommon.unquote(ctx.STRING_LITERAL().getText()))));
            }

            @Override
            public void enterDocFieldEqual(final JQLParser.DocFieldEqualContext ctx) {
                final ScopedField scopedField1 = ScopedField.parseFrom(ctx.singlyScopedField(0));
                final ScopedField scopedField2 = ScopedField.parseFrom(ctx.singlyScopedField(1));
                ValidationUtil.validateSameScopeThrowException(scopedField1.scope, scopedField2.scope);
                accept(scopedField1.wrap(new DocFilter.FieldEqual(scopedField1.field, scopedField2.field)));
            }

            @Override
            public void enterDocNotFieldEqual(final JQLParser.DocNotFieldEqualContext ctx) {
                final ScopedField scopedField1 = ScopedField.parseFrom(ctx.singlyScopedField(0));
                final ScopedField scopedField2 = ScopedField.parseFrom(ctx.singlyScopedField(1));
                ValidationUtil.validateSameScopeThrowException(scopedField1.scope, scopedField2.scope);
                accept(scopedField1.wrap(new DocFilter.Not(new DocFilter.FieldEqual(scopedField1.field, scopedField2.field))));
            }

            @Override
            public void enterDocFieldIs(JQLParser.DocFieldIsContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.singlyScopedField());
                accept(scopedField.wrap(new DocFilter.FieldIs(datasetsMetadata, scopedField.field, Term.parseJqlTerm(ctx.jqlTermVal()))));
            }

            @Override
            public void enterDocOr(JQLParser.DocOrContext ctx) {
                accept(new DocFilter.Or(parseJQLDocFilter(ctx.jqlDocFilter(0), datasetsMetadata, fromContents, warn, clock), parseJQLDocFilter(ctx.jqlDocFilter(1), datasetsMetadata, fromContents, warn, clock)));
            }

            @Override
            public void enterDocTrue(JQLParser.DocTrueContext ctx) {
                accept(new DocFilter.Always());
            }

            @Override
            public void enterDocMetricInequality(JQLParser.DocMetricInequalityContext ctx) {
                final String op = ctx.op.getText();
                final DocMetric arg1 = DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(0), datasetsMetadata, warn, clock);
                final DocMetric arg2 = DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(1), datasetsMetadata, warn, clock);
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
                accept(new DocFilter.And(parseJQLDocFilter(ctx.jqlDocFilter(0), datasetsMetadata, fromContents, warn, clock), parseJQLDocFilter(ctx.jqlDocFilter(1), datasetsMetadata, fromContents, warn, clock)));
            }

            @Override
            public void enterLucene(JQLParser.LuceneContext ctx) {
                accept(new DocFilter.Lucene(ParserCommon.unquote(ctx.STRING_LITERAL().getText()), datasetsMetadata));
            }

            @Override
            public void enterDocNotRegex(JQLParser.DocNotRegexContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.singlyScopedField());
                accept(scopedField.wrap(new DocFilter.NotRegex(scopedField.field, ParserCommon.unquote(ctx.STRING_LITERAL().getText()))));
            }

            @Override
            public void enterDocFilterParens(JQLParser.DocFilterParensContext ctx) {
                accept(parseJQLDocFilter(ctx.jqlDocFilter(), datasetsMetadata, fromContents, warn, clock));
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

    public static DocFilter docInHelper(DatasetsMetadata datasetsMetadata, Positioned<String> field, boolean negate, List<Term> termsList) {
        final boolean isStringField = anyIsString(termsList);
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
                termSet.add(term.intTerm);
            }
            filter = new DocFilter.IntFieldIn(datasetsMetadata, field, termSet);
        }
        if (negate) {
            return new DocFilter.Not(filter);
        } else {
            return filter;
        }
    }

    private static boolean anyIsString(List<Term> terms) {
        for (final Term term : terms) {
            if (!term.isIntTerm) return true;
        }
        return false;
    }
}
