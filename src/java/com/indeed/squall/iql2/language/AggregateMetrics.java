package com.indeed.squall.iql2.language;

import com.google.common.base.Optional;
import com.indeed.squall.iql2.language.query.GroupBy;
import com.indeed.squall.iql2.language.query.GroupBys;
import com.indeed.squall.iql2.language.util.ParseUtil;
import org.antlr.v4.runtime.misc.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AggregateMetrics {
    public static AggregateMetric parseAggregateMetric(JQLParser.AggregateMetricContext metricContext, Map<String, Set<String>> datasetToKeywordAnalyzerFields, Map<String, Set<String>> datasetToIntFields) {
        if (metricContext.jqlAggregateMetric() != null) {
            return parseJQLAggregateMetric(metricContext.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields);
        }
        if (metricContext.legacyAggregateMetric() != null) {
            return parseLegacyAggregateMetric(metricContext.legacyAggregateMetric(), datasetToKeywordAnalyzerFields);
        }
        throw new UnsupportedOperationException("This should be unreachable");
    }

    public static AggregateMetric parseLegacyAggregateMetric(JQLParser.LegacyAggregateMetricContext metricContext, final Map<String, Set<String>> datasetToKeywordAnalyzerFields) {
        final AggregateMetric[] ref = new AggregateMetric[1];
        metricContext.enterRule(new JQLBaseListener() {
            private void accept(AggregateMetric value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            @Override
            public void enterLegacyAggregateDivByConstant(@NotNull JQLParser.LegacyAggregateDivByConstantContext ctx) {
                accept(new AggregateMetric.Divide(parseLegacyAggregateMetric(ctx.legacyAggregateMetric(), datasetToKeywordAnalyzerFields), new AggregateMetric.Constant(Double.parseDouble(ctx.number().getText()))));
            }

            @Override
            public void enterLegacyAggregatePercentile(@NotNull JQLParser.LegacyAggregatePercentileContext ctx) {
                accept(new AggregateMetric.Percentile(ctx.identifier().getText(), Double.parseDouble(ctx.number().getText())));
            }

            @Override
            public void enterLegacyAggregateDiv(@NotNull JQLParser.LegacyAggregateDivContext ctx) {
                final DocMetric divisor = DocMetrics.parseLegacyDocMetric(ctx.legacyDocMetric(1), datasetToKeywordAnalyzerFields);
                final AggregateMetric aggDivisor;
                if (divisor instanceof DocMetric.Constant) {
                    final DocMetric.Constant constant = (DocMetric.Constant) divisor;
                    aggDivisor = new AggregateMetric.Constant(constant.value);
                } else {
                    aggDivisor = new AggregateMetric.DocStats(divisor);
                }
                accept(new AggregateMetric.Divide(
                        new AggregateMetric.DocStats(DocMetrics.parseLegacyDocMetric(ctx.legacyDocMetric(0), datasetToKeywordAnalyzerFields)),
                        aggDivisor
                ));
            }

            @Override
            public void enterLegacyAggregateDistinct(@NotNull JQLParser.LegacyAggregateDistinctContext ctx) {
                accept(new AggregateMetric.Distinct(ctx.identifier().getText(), Optional.<AggregateFilter>absent(), Optional.<Integer>absent()));
            }

            @Override
            public void enterLegacyImplicitSum(@NotNull JQLParser.LegacyImplicitSumContext ctx) {
                accept(new AggregateMetric.ImplicitDocStats(DocMetrics.parseLegacyDocMetric(ctx.legacyDocMetric(), datasetToKeywordAnalyzerFields)));
            }

            @Override
            public void enterLegacyAggregateParens(@NotNull JQLParser.LegacyAggregateParensContext ctx) {
                accept(parseLegacyAggregateMetric(ctx.legacyAggregateMetric(), datasetToKeywordAnalyzerFields));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled legacy aggregate metric: [" + metricContext.getText() + "]");
        }

        return ref[0];
    }

    public static AggregateMetric parseJQLAggregateMetric(JQLParser.JqlAggregateMetricContext metricContext, final Map<String, Set<String>> datasetToKeywordAnalyzerFields, final Map<String, Set<String>> datasetToIntFields) {
        final AggregateMetric[] ref = new AggregateMetric[1];
        metricContext.enterRule(new JQLBaseListener() {
            private void accept(AggregateMetric value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterAggregateParens(@NotNull JQLParser.AggregateParensContext ctx) {
                accept(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields));
            }

            public void enterAggregateParent(@NotNull JQLParser.AggregateParentContext ctx) {
                accept(new AggregateMetric.Parent(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterAggregateLag(@NotNull JQLParser.AggregateLagContext ctx) {
                accept(new AggregateMetric.Lag(Integer.parseInt(ctx.INT().getText()), parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterAggregateAvg(@NotNull JQLParser.AggregateAvgContext ctx) {
                accept(new AggregateMetric.Divide(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields), new AggregateMetric.DocStats(new DocMetric.Field("count()"))));
            }

            public void enterAggregateQualified(@NotNull JQLParser.AggregateQualifiedContext ctx) {
                final List<String> scope = ParseUtil.parseScope(ctx.scope());
                accept(new AggregateMetric.Qualified(scope, parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterAggregateDiv(@NotNull JQLParser.AggregateDivContext ctx) {
                accept(new AggregateMetric.Divide(parseJQLAggregateMetric(ctx.jqlAggregateMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields), parseJQLAggregateMetric(ctx.jqlAggregateMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterAggregateLog(@NotNull JQLParser.AggregateLogContext ctx) {
                accept(new AggregateMetric.Log(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterAggregateStandardDeviation(@NotNull JQLParser.AggregateStandardDeviationContext ctx) {
                accept(new AggregateMetric.Power(variance(DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)), new AggregateMetric.Constant(0.5)));
            }

            public void enterAggregatePower(@NotNull JQLParser.AggregatePowerContext ctx) {
                accept(new AggregateMetric.Power(parseJQLAggregateMetric(ctx.jqlAggregateMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields), parseJQLAggregateMetric(ctx.jqlAggregateMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterAggregateMod(@NotNull JQLParser.AggregateModContext ctx) {
                accept(new AggregateMetric.Modulus(parseJQLAggregateMetric(ctx.jqlAggregateMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields), parseJQLAggregateMetric(ctx.jqlAggregateMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterAggregatePDiff(@NotNull JQLParser.AggregatePDiffContext ctx) {
                final AggregateMetric actual = parseJQLAggregateMetric(ctx.actual, datasetToKeywordAnalyzerFields, datasetToIntFields);
                final AggregateMetric expected = parseJQLAggregateMetric(ctx.expected, datasetToKeywordAnalyzerFields, datasetToIntFields);
                // 100 * (actual - expected) / expected
                accept(new AggregateMetric.Multiply(new AggregateMetric.Constant(100), new AggregateMetric.Divide(new AggregateMetric.Subtract(actual, expected), expected)));
            }

            public void enterAggregateSum(@NotNull JQLParser.AggregateSumContext ctx) {
                accept(new AggregateMetric.DocStats(DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterAggregateMinus(@NotNull JQLParser.AggregateMinusContext ctx) {
                accept(new AggregateMetric.Subtract(parseJQLAggregateMetric(ctx.jqlAggregateMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields), parseJQLAggregateMetric(ctx.jqlAggregateMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterAggregateMult(@NotNull JQLParser.AggregateMultContext ctx) {
                accept(new AggregateMetric.Multiply(parseJQLAggregateMetric(ctx.jqlAggregateMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields), parseJQLAggregateMetric(ctx.jqlAggregateMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterAggregateConstant(@NotNull JQLParser.AggregateConstantContext ctx) {
                accept(new AggregateMetric.Constant(Double.parseDouble(ctx.number().getText())));
            }

            public void enterAggregateVariance(@NotNull JQLParser.AggregateVarianceContext ctx) {
                accept(variance(DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterAggregateAbs(@NotNull JQLParser.AggregateAbsContext ctx) {
                accept(new AggregateMetric.Abs(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterAggregateWindow(@NotNull JQLParser.AggregateWindowContext ctx) {
                accept(new AggregateMetric.Window(Integer.parseInt(ctx.INT().getText()), parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterAggregatePlus(@NotNull JQLParser.AggregatePlusContext ctx) {
                accept(new AggregateMetric.Add(parseJQLAggregateMetric(ctx.jqlAggregateMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields), parseJQLAggregateMetric(ctx.jqlAggregateMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterAggregateDistinctWindow(@NotNull JQLParser.AggregateDistinctWindowContext ctx) {
                final Optional<AggregateFilter> filter;
                if (ctx.jqlAggregateFilter() == null) {
                    filter = Optional.absent();
                } else {
                    filter = Optional.of(AggregateFilters.parseJQLAggregateFilter(ctx.jqlAggregateFilter(), datasetToKeywordAnalyzerFields, datasetToIntFields));
                }
                accept(new AggregateMetric.Distinct(ctx.identifier().getText(), filter, Optional.of(Integer.parseInt(ctx.INT().getText()))));
            }

            public void enterAggregateNegate(@NotNull JQLParser.AggregateNegateContext ctx) {
                accept(new AggregateMetric.Negate(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterAggregatePercentile(@NotNull JQLParser.AggregatePercentileContext ctx) {
                accept(new AggregateMetric.Percentile(ctx.identifier().getText(), Double.parseDouble(ctx.number().getText())));
            }

            public void enterAggregateRunning(@NotNull JQLParser.AggregateRunningContext ctx) {
                accept(new AggregateMetric.Running(1, parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            public void enterAggregateCounts(@NotNull JQLParser.AggregateCountsContext ctx) {
                accept(new AggregateMetric.DocStats(new DocMetric.Field("count()")));
            }

            public void enterAggregateDistinct(@NotNull JQLParser.AggregateDistinctContext ctx) {
                final Optional<AggregateFilter> filter;
                if (ctx.jqlAggregateFilter() == null) {
                    filter = Optional.absent();
                } else {
                    filter = Optional.of(AggregateFilters.parseJQLAggregateFilter(ctx.jqlAggregateFilter(), datasetToKeywordAnalyzerFields, datasetToIntFields));
                }
                accept(new AggregateMetric.Distinct(ctx.identifier().getText(), filter, Optional.<Integer>absent()));
            }

            @Override
            public void enterAggregateNamed(@NotNull JQLParser.AggregateNamedContext ctx) {
                final AggregateMetric metric = parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields);
                final String name = ctx.name.getText();
                accept(new AggregateMetric.Named(metric, name));
            }

            @Override
            public void enterAggregateSumAcross(@NotNull JQLParser.AggregateSumAcrossContext ctx) {
                final JQLParser.JqlSumOverMetricContext x = ctx.jqlSumOverMetric();
                accept(new AggregateMetric.SumAcross(GroupBys.parseGroupBy(x.groupByElement(), datasetToKeywordAnalyzerFields, datasetToIntFields), parseJQLAggregateMetric(x.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)));
            }

            @Override
            public void enterAggregateAverageAcross(@NotNull JQLParser.AggregateAverageAcrossContext ctx) {
                final Optional<AggregateFilter> filter;
                if (ctx.jqlAggregateFilter() != null) {
                    filter = Optional.of(AggregateFilters.parseJQLAggregateFilter(ctx.jqlAggregateFilter(), datasetToKeywordAnalyzerFields, datasetToIntFields));
                } else {
                    filter = Optional.absent();
                }
                final GroupBy groupBy = new GroupBy.GroupByField(ctx.field.getText(), filter, Optional.<Long>absent(), Optional.<AggregateMetric>absent(), false, false);
                accept(new AggregateMetric.Divide(
                        new AggregateMetric.SumAcross(groupBy, AggregateMetrics.parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields)),
                        new AggregateMetric.Distinct(ctx.field.getText(), filter, Optional.<Integer>absent())
                ));
            }

            @Override
            public void enterAggregateDocMetricAtom(@NotNull JQLParser.AggregateDocMetricAtomContext ctx) {
                accept(new AggregateMetric.ImplicitDocStats(DocMetrics.parseDocMetricAtom(ctx.docMetricAtom())));
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
