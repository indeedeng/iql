package com.indeed.jql.language;

import org.antlr.v4.runtime.misc.NotNull;

import java.util.List;

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

    public static DocFilter parseDocFilter(JQLParser.DocFilterContext docFilterContext) {
        if (docFilterContext.jqlDocFilter() != null) {
            return parseJQLDocFilter(docFilterContext.jqlDocFilter());
        }
        if (docFilterContext.legacyDocFilter() != null) {
            return parseLegacyDocFilter(docFilterContext.legacyDocFilter());
        }
        throw new UnsupportedOperationException("What do?!");
    }

    public static DocFilter parseLegacyDocFilter(JQLParser.LegacyDocFilterContext legacyDocFilterContext) {
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
                final long denominator = Long.parseLong(ctx.denominator.getText());
                final String seed = ctx.seed.getText();
                accept(new DocFilter.Sample(field, numerator, denominator, seed));
            }

            @Override
            public void enterLegacyDocNot(@NotNull JQLParser.LegacyDocNotContext ctx) {
                accept(new DocFilter.Not(parseLegacyDocFilter(ctx.legacyDocFilter())));
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
                accept(new DocFilter.Or(parseLegacyDocFilter(ctx.legacyDocFilter(0)), parseLegacyDocFilter(ctx.legacyDocFilter(1))));
            }

            @Override
            public void enterLegacyDocTrue(@NotNull JQLParser.LegacyDocTrueContext ctx) {
                accept(new DocFilter.Always());
            }

            @Override
            public void enterLegacyDocMetricInequality(@NotNull JQLParser.LegacyDocMetricInequalityContext ctx) {
                final String op = ctx.op.getText();
                final DocMetric arg1 = DocMetrics.parseLegacyDocMetric(ctx.legacyDocMetric(0));
                final DocMetric arg2 = DocMetrics.parseLegacyDocMetric(ctx.legacyDocMetric(1));
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
                accept(new DocFilter.And(parseLegacyDocFilter(ctx.legacyDocFilter(0)), parseLegacyDocFilter(ctx.legacyDocFilter(1))));
            }

            @Override
            public void enterLegacyLucene(@NotNull JQLParser.LegacyLuceneContext ctx) {
                accept(new DocFilter.Lucene(ParserCommon.unquote(ctx.STRING_LITERAL().getText())));
            }

            @Override
            public void enterLegacyDocNotRegex(@NotNull JQLParser.LegacyDocNotRegexContext ctx) {
                accept(new DocFilter.NotRegex(ctx.field.getText(), ParserCommon.unquote(ctx.STRING_LITERAL().getText())));
            }

            @Override
            public void enterLegacyDocFilterParens(@NotNull JQLParser.LegacyDocFilterParensContext ctx) {
                accept(parseLegacyDocFilter(ctx.legacyDocFilter()));
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

    public static DocFilter parseJQLDocFilter(JQLParser.JqlDocFilterContext docFilterContext) {
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
                final long denominator = Long.parseLong(ctx.denominator.getText());
                final String seed = ctx.seed.getText();
                accept(new DocFilter.Sample(field, numerator, denominator, seed));
            }

            @Override
            public void enterDocNot(@NotNull JQLParser.DocNotContext ctx) {
                accept(new DocFilter.Not(parseJQLDocFilter(ctx.jqlDocFilter())));
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
                accept(new DocFilter.Or(parseJQLDocFilter(ctx.jqlDocFilter(0)), parseJQLDocFilter(ctx.jqlDocFilter(1))));
            }

            @Override
            public void enterDocTrue(@NotNull JQLParser.DocTrueContext ctx) {
                accept(new DocFilter.Always());
            }

            @Override
            public void enterDocMetricInequality(@NotNull JQLParser.DocMetricInequalityContext ctx) {
                final String op = ctx.op.getText();
                final DocMetric arg1 = DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(0));
                final DocMetric arg2 = DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(1));
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
                accept(new DocFilter.And(parseJQLDocFilter(ctx.jqlDocFilter(0)), parseJQLDocFilter(ctx.jqlDocFilter(1))));
            }

            @Override
            public void enterLucene(@NotNull JQLParser.LuceneContext ctx) {
                accept(new DocFilter.Lucene(ParserCommon.unquote(ctx.STRING_LITERAL().getText())));
            }

            @Override
            public void enterDocNotRegex(@NotNull JQLParser.DocNotRegexContext ctx) {
                accept(new DocFilter.NotRegex(ctx.field.getText(), ParserCommon.unquote(ctx.STRING_LITERAL().getText())));
            }

            @Override
            public void enterDocFilterParens(@NotNull JQLParser.DocFilterParensContext ctx) {
                accept(parseJQLDocFilter(ctx.jqlDocFilter()));
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
        DocFilter filter = null;
        for (final JQLParser.TermValContext term : terms) {
            if (filter == null) {
                filter = new DocFilter.FieldIs(field, Term.parseTerm(term));
            } else {
                filter = new DocFilter.Or(new DocFilter.FieldIs(field, Term.parseTerm(term)), filter);
            }
        }
        if (filter == null) {
            // TODO (optional): Make this new Always() and don't negate if (ctx.not != null).
            // TODO cont:       Alternatively, add optimization pass for Not(Always) and Not(Never).
            filter = new DocFilter.Never();
        }
        if (negate) {
            filter = new DocFilter.Not(filter);
        }
        return filter;
    }
}
