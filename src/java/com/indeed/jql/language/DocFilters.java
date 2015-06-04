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
                new DocFilter.FieldIsnt(ctx.field.getText(), Term.parseTerm(ctx.termVal()));
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
                accept(new DocFilter.Not(parseDocFilter(ctx.docFilter())));
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
                accept(new DocFilter.Or(parseDocFilter(ctx.docFilter(0)), parseDocFilter(ctx.docFilter(1))));
            }

            @Override
            public void enterDocTrue(@NotNull JQLParser.DocTrueContext ctx) {
                accept(new DocFilter.Always());
            }

            @Override
            public void enterDocMetricInequality(@NotNull JQLParser.DocMetricInequalityContext ctx) {
                final String op = ctx.op.getText();
                final DocMetric arg1 = DocMetrics.parseDocMetric(ctx.docMetric(0));
                final DocMetric arg2 = DocMetrics.parseDocMetric(ctx.docMetric(1));
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
                accept(new DocFilter.And(parseDocFilter(ctx.docFilter(0)), parseDocFilter(ctx.docFilter(1))));
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
                accept(parseDocFilter(ctx.docFilter()));
            }

            @Override
            public void enterDocFalse(@NotNull JQLParser.DocFalseContext ctx) {
                accept(new DocFilter.Never());
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled doc filter: [" + docFilterContext.getText() + "]");
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
            filter = new DocFilter.Never();
        }
        if (negate) {
            filter = new DocFilter.Not(filter);
        }
        return filter;
    }
}
