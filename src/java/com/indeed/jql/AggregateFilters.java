package com.indeed.jql;

import org.antlr.v4.runtime.misc.NotNull;

import java.util.List;

public class AggregateFilters {
    public static AggregateFilter aggregateInHelper(List<JQLParser.TermValContext> terms, boolean negate) {
        AggregateFilter filter = null;
        for (final JQLParser.TermValContext term : terms) {
            if (filter == null) {
                filter = new AggregateFilter.TermIs(Term.parseTerm(term));
            } else {
                filter = new AggregateFilter.Or(new AggregateFilter.TermIs(Term.parseTerm(term)), filter);
            }
        }
        if (filter == null) {
            // TODO (optional): Make this new Always() and don't negate if (ctx.not != null).
            filter = new AggregateFilter.Never();
        }
        if (negate) {
            filter = new AggregateFilter.Not(filter);
        }
        return filter;
    }

    public static AggregateFilter parseAggregateFilter(JQLParser.AggregateFilterContext aggregateFilterContext) {
        final AggregateFilter[] ref = new AggregateFilter[1];

        aggregateFilterContext.enterRule(new JQLBaseListener() {
            private void accept(AggregateFilter value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterAggregateRegex(@NotNull JQLParser.AggregateRegexContext ctx) {
                accept(new AggregateFilter.Regex(ctx.field.getText(), ParserCommon.unquote(ctx.STRING_LITERAL().getText())));
            }

            public void enterAggregateFalse(@NotNull JQLParser.AggregateFalseContext ctx) {
                accept(new AggregateFilter.Never());
            }

            public void enterAggregateTermIs(@NotNull JQLParser.AggregateTermIsContext ctx) {
                accept(new AggregateFilter.TermIs(Term.parseTerm(ctx.termVal())));
            }

            public void enterAggregateNotRegex(@NotNull JQLParser.AggregateNotRegexContext ctx) {
                accept(new AggregateFilter.Not(new AggregateFilter.Regex(ctx.field.getText(), ParserCommon.unquote(ctx.STRING_LITERAL().getText()))));
            }

            public void enterAggregateTrue(@NotNull JQLParser.AggregateTrueContext ctx) {
                accept(new AggregateFilter.Always());
            }

            public void enterAggregateFilterParens(@NotNull JQLParser.AggregateFilterParensContext ctx) {
                accept(parseAggregateFilter(ctx.aggregateFilter()));
            }

            public void enterAggregateAnd(@NotNull JQLParser.AggregateAndContext ctx) {
                accept(new AggregateFilter.And(parseAggregateFilter(ctx.aggregateFilter(0)), parseAggregateFilter(ctx.aggregateFilter(1))));
            }

            public void enterAggregateMetricInequality(@NotNull JQLParser.AggregateMetricInequalityContext ctx) {
                final String operation = ctx.op.getText();
                final AggregateMetric arg1 = AggregateMetrics.parseAggregateMetric(ctx.aggregateMetric(0));
                final AggregateMetric arg2 = AggregateMetrics.parseAggregateMetric(ctx.aggregateMetric(1));
                final AggregateFilter result;
                switch (operation) {
                    case "=": {
                        result = new AggregateFilter.MetricIs(arg1, arg2);
                        break;
                    }
                    case "!=": {
                        result = new AggregateFilter.MetricIsnt(arg1, arg2);
                        break;
                    }
                    case "<": {
                        result = new AggregateFilter.Lt(arg1, arg2);
                        break;
                    }
                    case "<=": {
                        result = new AggregateFilter.Lte(arg1, arg2);
                        break;
                    }
                    case ">": {
                        result = new AggregateFilter.Gt(arg1, arg2);
                        break;
                    }
                    case ">=": {
                        result = new AggregateFilter.Gte(arg1, arg2);
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Unhandled inequality operation: [" + operation + "]");
                }
                accept(result);
            }

            public void enterAggregateNot(@NotNull JQLParser.AggregateNotContext ctx) {
                accept(new AggregateFilter.Not(parseAggregateFilter(ctx.aggregateFilter())));
            }

            public void enterAggregateOr(@NotNull JQLParser.AggregateOrContext ctx) {
                accept(new AggregateFilter.Or(parseAggregateFilter(ctx.aggregateFilter(0)), parseAggregateFilter(ctx.aggregateFilter(1))));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled aggregate filter: [" + aggregateFilterContext.getText() + "]");
        }

        return ref[0];
    }
}
