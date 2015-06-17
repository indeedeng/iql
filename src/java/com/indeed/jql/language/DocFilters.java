package com.indeed.jql.language;

import com.indeed.jql.language.actions.Action;
import com.indeed.jql.language.actions.QueryAction;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.antlr.v4.runtime.misc.NotNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
            result = new DocFilter.And(filters.get(i), result);
        }
        return result;
    }

    public static DocFilter parseDocFilter(JQLParser.DocFilterContext docFilterContext, Map<String, Set<String>> datasetToKeywordAnalyzerFields) {
        if (docFilterContext.jqlDocFilter() != null) {
            return parseJQLDocFilter(docFilterContext.jqlDocFilter(), datasetToKeywordAnalyzerFields);
        }
        if (docFilterContext.legacyDocFilter() != null) {
            return parseLegacyDocFilter(docFilterContext.legacyDocFilter(), datasetToKeywordAnalyzerFields);
        }
        throw new UnsupportedOperationException("What do?!");
    }

    public static DocFilter parseLegacyDocFilter(JQLParser.LegacyDocFilterContext legacyDocFilterContext, final Map<String, Set<String>> datasetToKeywordAnalyzerFields) {
        final DocFilter[] ref = new DocFilter[1];

        legacyDocFilterContext.enterRule(new JQLBaseListener() {
            public void accept(DocFilter value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            @Override
            public void enterLegacyDocBetween(@NotNull JQLParser.LegacyDocBetweenContext ctx) {
                final String field = ctx.field.getText();
                final long lowerBound = Long.parseLong(ctx.lowerBound.getText());
                final long upperBound = Long.parseLong(ctx.upperBound.getText());
                accept(new DocFilter.Between(field, lowerBound, upperBound));
            }

            @Override
            public void enterLegacyDocFieldIn(@NotNull JQLParser.LegacyDocFieldInContext ctx) {
                String field = ctx.field.getText();
                final List<JQLParser.TermValContext> terms = ctx.terms;
                final boolean negate = ctx.not != null;
                accept(docInHelper(field, terms, negate));
            }

            @Override
            public void enterLegacyDocFieldIsnt(@NotNull JQLParser.LegacyDocFieldIsntContext ctx) {
                accept(new DocFilter.FieldIsnt(ctx.field.getText(), Term.parseTerm(ctx.termVal())));
            }

            @Override
            public void enterLegacyDocSample(@NotNull JQLParser.LegacyDocSampleContext ctx) {
                final String field = ctx.field.getText();
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
            public void enterLegacyDocNot(@NotNull JQLParser.LegacyDocNotContext ctx) {
                accept(new DocFilter.Not(parseLegacyDocFilter(ctx.legacyDocFilter(), datasetToKeywordAnalyzerFields)));
            }

            @Override
            public void enterLegacyDocRegex(@NotNull JQLParser.LegacyDocRegexContext ctx) {
                accept(new DocFilter.Regex(ctx.field.getText(), ParserCommon.unquote(ctx.STRING_LITERAL().getText())));
            }

            @Override
            public void enterLegacyDocFieldIs(@NotNull JQLParser.LegacyDocFieldIsContext ctx) {
                accept(new DocFilter.FieldIs(ctx.field.getText(), Term.parseTerm(ctx.termVal())));
            }

            @Override
            public void enterLegacyDocOr(@NotNull JQLParser.LegacyDocOrContext ctx) {
                accept(new DocFilter.Or(parseLegacyDocFilter(ctx.legacyDocFilter(0), datasetToKeywordAnalyzerFields), parseLegacyDocFilter(ctx.legacyDocFilter(1), datasetToKeywordAnalyzerFields)));
            }

            @Override
            public void enterLegacyDocTrue(@NotNull JQLParser.LegacyDocTrueContext ctx) {
                accept(new DocFilter.Always());
            }

            @Override
            public void enterLegacyDocMetricInequality(@NotNull JQLParser.LegacyDocMetricInequalityContext ctx) {
                final String op = ctx.op.getText();
                final DocMetric arg1 = DocMetrics.parseLegacyDocMetric(ctx.legacyDocMetric(0), datasetToKeywordAnalyzerFields);
                final DocMetric arg2 = DocMetrics.parseLegacyDocMetric(ctx.legacyDocMetric(1), datasetToKeywordAnalyzerFields);
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
            public void enterLegacyDocAnd(@NotNull JQLParser.LegacyDocAndContext ctx) {
                accept(new DocFilter.And(parseLegacyDocFilter(ctx.legacyDocFilter(0), datasetToKeywordAnalyzerFields), parseLegacyDocFilter(ctx.legacyDocFilter(1), datasetToKeywordAnalyzerFields)));
            }

            @Override
            public void enterLegacyLucene(@NotNull JQLParser.LegacyLuceneContext ctx) {
                accept(new DocFilter.Lucene(ParserCommon.unquote(ctx.STRING_LITERAL().getText()), datasetToKeywordAnalyzerFields));
            }

            @Override
            public void enterLegacyDocNotRegex(@NotNull JQLParser.LegacyDocNotRegexContext ctx) {
                accept(new DocFilter.NotRegex(ctx.field.getText(), ParserCommon.unquote(ctx.STRING_LITERAL().getText())));
            }

            @Override
            public void enterLegacyDocFilterParens(@NotNull JQLParser.LegacyDocFilterParensContext ctx) {
                accept(parseLegacyDocFilter(ctx.legacyDocFilter(), datasetToKeywordAnalyzerFields));
            }

            @Override
            public void enterLegacyDocFalse(@NotNull JQLParser.LegacyDocFalseContext ctx) {
                accept(new DocFilter.Never());
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled doc filter: [" + legacyDocFilterContext.getText() + "]");
        }
        return ref[0];
    }

    public static DocFilter parseJQLDocFilter(JQLParser.JqlDocFilterContext docFilterContext, final Map<String, Set<String>> datasetToKeywordAnalyzerFields) {
        final DocFilter[] ref = new DocFilter[1];

        docFilterContext.enterRule(new JQLBaseListener() {
            public void accept(DocFilter value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            @Override
            public void enterDocBetween(@NotNull JQLParser.DocBetweenContext ctx) {
                final String field = ctx.field.getText();
                final long lowerBound = Long.parseLong(ctx.lowerBound.getText());
                final long upperBound = Long.parseLong(ctx.upperBound.getText());
                accept(new DocFilter.Between(field, lowerBound, upperBound));
            }

            @Override
            public void enterDocFieldIn(@NotNull JQLParser.DocFieldInContext ctx) {
                String field = ctx.field.getText();
                final List<JQLParser.TermValContext> terms = ctx.terms;
                final boolean negate = ctx.not != null;
                accept(docInHelper(field, terms, negate));
            }

            @Override
            public void enterDocFieldIsnt(@NotNull JQLParser.DocFieldIsntContext ctx) {
                accept(new DocFilter.FieldIsnt(ctx.field.getText(), Term.parseTerm(ctx.termVal())));
            }

            @Override
            public void enterDocSample(@NotNull JQLParser.DocSampleContext ctx) {
                final String field = ctx.field.getText();
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
            public void enterDocNot(@NotNull JQLParser.DocNotContext ctx) {
                accept(new DocFilter.Not(parseJQLDocFilter(ctx.jqlDocFilter(), datasetToKeywordAnalyzerFields)));
            }

            @Override
            public void enterDocRegex(@NotNull JQLParser.DocRegexContext ctx) {
                accept(new DocFilter.Regex(ctx.field.getText(), ParserCommon.unquote(ctx.STRING_LITERAL().getText())));
            }

            @Override
            public void enterDocFieldIs(@NotNull JQLParser.DocFieldIsContext ctx) {
                accept(new DocFilter.FieldIs(ctx.field.getText(), Term.parseTerm(ctx.termVal())));
            }

            @Override
            public void enterDocOr(@NotNull JQLParser.DocOrContext ctx) {
                accept(new DocFilter.Or(parseJQLDocFilter(ctx.jqlDocFilter(0), datasetToKeywordAnalyzerFields), parseJQLDocFilter(ctx.jqlDocFilter(1), datasetToKeywordAnalyzerFields)));
            }

            @Override
            public void enterDocTrue(@NotNull JQLParser.DocTrueContext ctx) {
                accept(new DocFilter.Always());
            }

            @Override
            public void enterDocMetricInequality(@NotNull JQLParser.DocMetricInequalityContext ctx) {
                final String op = ctx.op.getText();
                final DocMetric arg1 = DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(0), datasetToKeywordAnalyzerFields);
                final DocMetric arg2 = DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(1), datasetToKeywordAnalyzerFields);
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
            public void enterDocAnd(@NotNull JQLParser.DocAndContext ctx) {
                accept(new DocFilter.And(parseJQLDocFilter(ctx.jqlDocFilter(0), datasetToKeywordAnalyzerFields), parseJQLDocFilter(ctx.jqlDocFilter(1), datasetToKeywordAnalyzerFields)));
            }

            @Override
            public void enterLucene(@NotNull JQLParser.LuceneContext ctx) {
                accept(new DocFilter.Lucene(ParserCommon.unquote(ctx.STRING_LITERAL().getText()), datasetToKeywordAnalyzerFields));
            }

            @Override
            public void enterDocNotRegex(@NotNull JQLParser.DocNotRegexContext ctx) {
                accept(new DocFilter.NotRegex(ctx.field.getText(), ParserCommon.unquote(ctx.STRING_LITERAL().getText())));
            }

            @Override
            public void enterDocFilterParens(@NotNull JQLParser.DocFilterParensContext ctx) {
                accept(parseJQLDocFilter(ctx.jqlDocFilter(), datasetToKeywordAnalyzerFields));
            }

            @Override
            public void enterDocFalse(@NotNull JQLParser.DocFalseContext ctx) {
                accept(new DocFilter.Never());
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled doc filter: [" + docFilterContext.getText() + "], " + docFilterContext.toStringTree(new JQLParser(null)));
        }
        return ref[0];
    }

    public static DocFilter docInHelper(String field, List<JQLParser.TermValContext> terms, boolean negate) {
        final List<Term> termsList = new ArrayList<>();
        for (final JQLParser.TermValContext term : terms) {
            termsList.add(Term.parseTerm(term));
        }
        final boolean isStringField = anyIsString(termsList);
        if (isStringField) {
            final Set<String> termSet = new HashSet<>();
            for (final Term term : termsList) {
                if (term.isIntTerm) {
                    termSet.add(String.valueOf(term.intTerm));
                } else {
                    termSet.add(term.stringTerm);
                }
            }
            return new DocFilter.StringFieldIn(field, termSet);
        } else {
            final Set<Long> termSet = new LongOpenHashSet();
            for (final Term term : termsList) {
                termSet.add(term.intTerm);
            }
            return new DocFilter.IntFieldIn(field, termSet);
        }
    }

    private static boolean anyIsString(List<Term> terms) {
        for (final Term term : terms) {
            if (!term.isIntTerm) return true;
        }
        return false;
    }
}
