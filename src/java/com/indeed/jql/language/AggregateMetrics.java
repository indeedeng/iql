package com.indeed.jql.language;

import com.google.common.base.Optional;
import org.antlr.v4.runtime.misc.NotNull;

import java.util.ArrayList;
import java.util.List;

public class AggregateMetrics {
    public static AggregateMetric parseAggregateMetric(JQLParser.AggregateMetricContext metricContext) {
        final AggregateMetric[] ref = new AggregateMetric[1];
        metricContext.enterRule(new JQLBaseListener() {
            private void accept(AggregateMetric value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterAggregateParens(@NotNull JQLParser.AggregateParensContext ctx) {
                accept(parseAggregateMetric(ctx.aggregateMetric()));
            }

            public void enterAggregateParent(@NotNull JQLParser.AggregateParentContext ctx) {
                accept(new AggregateMetric.Parent(parseAggregateMetric(ctx.aggregateMetric())));
            }

            public void enterAggregateRawField(@NotNull JQLParser.AggregateRawFieldContext ctx) {
                accept(new AggregateMetric.DocStats(new DocMetric.Field(ctx.identifier().getText())));
            }

            public void enterAggregateLag(@NotNull JQLParser.AggregateLagContext ctx) {
                accept(new AggregateMetric.Lag(Integer.parseInt(ctx.INT().getText()), parseAggregateMetric(ctx.aggregateMetric())));
            }

            public void enterAggregateAvg(@NotNull JQLParser.AggregateAvgContext ctx) {
                accept(new AggregateMetric.Divide(parseAggregateMetric(ctx.aggregateMetric()), new AggregateMetric.DocStats(new DocMetric.Field("count()"))));
            }

            public void enterAggregateQualified(@NotNull JQLParser.AggregateQualifiedContext ctx) {
                final List<String> scope = new ArrayList<>();
                for (final JQLParser.IdentifierContext dataset : ctx.scope().datasets) {
                    scope.add(dataset.getText());
                }
                accept(new AggregateMetric.Qualified(scope, parseAggregateMetric(ctx.aggregateMetric())));
            }

            public void enterAggregateDiv(@NotNull JQLParser.AggregateDivContext ctx) {
                accept(new AggregateMetric.Divide(parseAggregateMetric(ctx.aggregateMetric(0)), parseAggregateMetric(ctx.aggregateMetric(1))));
            }

            public void enterAggregateLog(@NotNull JQLParser.AggregateLogContext ctx) {
                accept(new AggregateMetric.Log(parseAggregateMetric(ctx.aggregateMetric())));
            }

            public void enterAggregateStandardDeviation(@NotNull JQLParser.AggregateStandardDeviationContext ctx) {
                accept(new AggregateMetric.Power(variance(DocMetrics.parseDocMetric(ctx.docMetric())), new AggregateMetric.Constant(0.5)));
            }

            public void enterAggregatePower(@NotNull JQLParser.AggregatePowerContext ctx) {
                accept(new AggregateMetric.Power(parseAggregateMetric(ctx.aggregateMetric(0)), parseAggregateMetric(ctx.aggregateMetric(1))));
            }

            public void enterAggregateMod(@NotNull JQLParser.AggregateModContext ctx) {
                accept(new AggregateMetric.Modulus(parseAggregateMetric(ctx.aggregateMetric(0)), parseAggregateMetric(ctx.aggregateMetric(1))));
            }

            public void enterAggregatePDiff(@NotNull JQLParser.AggregatePDiffContext ctx) {
                final AggregateMetric actual = parseAggregateMetric(ctx.actual);
                final AggregateMetric expected = parseAggregateMetric(ctx.expected);
                // 100 * (actual - expected) / expected
                accept(new AggregateMetric.Multiply(new AggregateMetric.Constant(100), new AggregateMetric.Divide(new AggregateMetric.Subtract(actual, expected), expected)));
            }

            public void enterAggregateSum(@NotNull JQLParser.AggregateSumContext ctx) {
                accept(new AggregateMetric.DocStats(DocMetrics.parseDocMetric(ctx.docMetric())));
            }

            public void enterAggregateMinus(@NotNull JQLParser.AggregateMinusContext ctx) {
                accept(new AggregateMetric.Subtract(parseAggregateMetric(ctx.aggregateMetric(0)), parseAggregateMetric(ctx.aggregateMetric(1))));
            }

            public void enterAggregateMult(@NotNull JQLParser.AggregateMultContext ctx) {
                accept(new AggregateMetric.Multiply(parseAggregateMetric(ctx.aggregateMetric(0)), parseAggregateMetric(ctx.aggregateMetric(1))));
            }

            public void enterAggregateConstant(@NotNull JQLParser.AggregateConstantContext ctx) {
                accept(new AggregateMetric.Constant(Double.parseDouble(ctx.number().getText())));
            }

            public void enterAggregateVariance(@NotNull JQLParser.AggregateVarianceContext ctx) {
                accept(variance(DocMetrics.parseDocMetric(ctx.docMetric())));
            }

            public void enterAggregateAbs(@NotNull JQLParser.AggregateAbsContext ctx) {
                accept(new AggregateMetric.Abs(parseAggregateMetric(ctx.aggregateMetric())));
            }

            public void enterAggregateWindow(@NotNull JQLParser.AggregateWindowContext ctx) {
                accept(new AggregateMetric.Window(Integer.parseInt(ctx.INT().getText()), parseAggregateMetric(ctx.aggregateMetric())));
            }

            public void enterAggregatePlus(@NotNull JQLParser.AggregatePlusContext ctx) {
                accept(new AggregateMetric.Add(parseAggregateMetric(ctx.aggregateMetric(0)), parseAggregateMetric(ctx.aggregateMetric(1))));
            }

            public void enterAggregateDistinctWindow(@NotNull JQLParser.AggregateDistinctWindowContext ctx) {
                final Optional<AggregateFilter> filter;
                if (ctx.aggregateFilter() == null) {
                    filter = Optional.absent();
                } else {
                    filter = Optional.of(AggregateFilters.parseAggregateFilter(ctx.aggregateFilter()));
                }
                accept(new AggregateMetric.Distinct(ctx.identifier().getText(), filter, Optional.of(Integer.parseInt(ctx.INT().getText()))));
            }

            public void enterAggregateNegate(@NotNull JQLParser.AggregateNegateContext ctx) {
                accept(new AggregateMetric.Negate(parseAggregateMetric(ctx.aggregateMetric())));
            }

            public void enterAggregatePercentile(@NotNull JQLParser.AggregatePercentileContext ctx) {
                accept(new AggregateMetric.Percentile(ctx.identifier().getText(), Double.parseDouble(ctx.DOUBLE().getText())));
            }

            public void enterAggregateRunning(@NotNull JQLParser.AggregateRunningContext ctx) {
                accept(new AggregateMetric.Running(parseAggregateMetric(ctx.aggregateMetric())));
            }

            public void enterAggregateCounts(@NotNull JQLParser.AggregateCountsContext ctx) {
                accept(new AggregateMetric.DocStats(new DocMetric.Field("count()")));
            }

            public void enterAggregateDistinct(@NotNull JQLParser.AggregateDistinctContext ctx) {
                final Optional<AggregateFilter> filter;
                if (ctx.aggregateFilter() == null) {
                    filter = Optional.absent();
                } else {
                    filter = Optional.of(AggregateFilters.parseAggregateFilter(ctx.aggregateFilter()));
                }
                accept(new AggregateMetric.Distinct(ctx.identifier().getText(), filter, Optional.<Integer>absent()));
            }

            @Override
            public void enterAggregateNamed(@NotNull JQLParser.AggregateNamedContext ctx) {
                final AggregateMetric metric = parseAggregateMetric(ctx.aggregateMetric());
                final String name = ctx.name.getText();
                accept(new AggregateMetric.Named(metric, name));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled aggregate metric: [" + metricContext.getText() + "]");
        }

        return ref[0];
    }

    public static AggregateMetric variance(DocMetric docMetric) {
        // [m * m] / count()
        final AggregateMetric firstHalf = new AggregateMetric.Divide(new AggregateMetric.DocStats(new DocMetric.Multiply(docMetric, docMetric)), new AggregateMetric.DocStats(new DocMetric.Field("count()")));
        // [m] / count()
        final AggregateMetric halfOfSecondHalf = new AggregateMetric.Divide(new AggregateMetric.DocStats(docMetric), new AggregateMetric.DocStats(new DocMetric.Field("count()")));
        // ([m] / count()) ^ 2
        final AggregateMetric secondHalf = new AggregateMetric.Multiply(halfOfSecondHalf, halfOfSecondHalf);
        // E(m^2) - E(m)^2
        return new AggregateMetric.Subtract(firstHalf, secondHalf);
    }
}
