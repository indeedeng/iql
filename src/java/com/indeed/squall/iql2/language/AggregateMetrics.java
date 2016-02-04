package com.indeed.squall.iql2.language;

import com.google.common.base.Optional;
import com.indeed.common.util.time.WallClock;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.query.GroupBy;
import com.indeed.squall.iql2.language.query.GroupBys;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AggregateMetrics {
    public static AggregateMetric parseAggregateMetric(JQLParser.AggregateMetricContext metricContext, Map<String, Set<String>> datasetToKeywordAnalyzerFields, Map<String, Set<String>> datasetToIntFields, Consumer<String> warn, WallClock clock) {
        if (metricContext.jqlAggregateMetric() != null) {
            return parseJQLAggregateMetric(metricContext.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
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
            public void enterLegacyAggregateDivByConstant(JQLParser.LegacyAggregateDivByConstantContext ctx) {
                accept(new AggregateMetric.Divide(parseLegacyAggregateMetric(ctx.legacyAggregateMetric(), datasetToKeywordAnalyzerFields), new AggregateMetric.Constant(Double.parseDouble(ctx.number().getText()))));
            }

            @Override
            public void enterLegacyAggregatePercentile(JQLParser.LegacyAggregatePercentileContext ctx) {
                accept(new AggregateMetric.Percentile(ctx.identifier().getText(), Double.parseDouble(ctx.number().getText())));
            }

            @Override
            public void enterLegacyAggregateDiv(JQLParser.LegacyAggregateDivContext ctx) {
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
            public void enterLegacyAggregateDistinct(JQLParser.LegacyAggregateDistinctContext ctx) {
                accept(new AggregateMetric.Distinct(ctx.identifier().getText().toUpperCase(), Optional.<AggregateFilter>absent(), Optional.<Integer>absent()));
            }

            @Override
            public void enterLegacyImplicitSum(JQLParser.LegacyImplicitSumContext ctx) {
                accept(new AggregateMetric.ImplicitDocStats(DocMetrics.parseLegacyDocMetric(ctx.legacyDocMetric(), datasetToKeywordAnalyzerFields)));
            }

            @Override
            public void enterLegacyAggregateParens(JQLParser.LegacyAggregateParensContext ctx) {
                accept(parseLegacyAggregateMetric(ctx.legacyAggregateMetric(), datasetToKeywordAnalyzerFields));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled legacy aggregate metric: [" + metricContext.getText() + "]");
        }

        return ref[0];
    }

    public static AggregateMetric parseSyntacticallyAtomicJQLAggregateMetric(JQLParser.SyntacticallyAtomicJqlAggregateMetricContext ctx, final Map<String, Set<String>> datasetToKeywordAnalyzerFields, final Map<String, Set<String>> datasetToIntFields) {
        final AggregateMetric[] ref = new AggregateMetric[1];
        ctx.enterRule(new JQLBaseListener() {
            private void accept(AggregateMetric value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            @Override
            public void enterAggregateConstant(JQLParser.AggregateConstantContext ctx) {
                accept(new AggregateMetric.Constant(Double.parseDouble(ctx.number().getText())));
            }

            @Override
            public void enterAggregateCounts(JQLParser.AggregateCountsContext ctx) {
                accept(new AggregateMetric.DocStats(new DocMetric.Count()));
            }

            @Override
            public void enterAggregateDocMetricAtom2(JQLParser.AggregateDocMetricAtom2Context ctx) {
                accept(new AggregateMetric.ImplicitDocStats(DocMetrics.parseJQLSyntacticallyAtomicDocMetricAtom(ctx.jqlSyntacticallyAtomicDocMetricAtom())));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled aggregate metric: [" + ctx.getText() + "]");
        }

        return ref[0];
    }

    public static AggregateMetric parseJQLAggregateMetric(JQLParser.JqlAggregateMetricContext metricContext, final Map<String, Set<String>> datasetToKeywordAnalyzerFields, final Map<String, Set<String>> datasetToIntFields, final Consumer<String> warn, final WallClock clock) {
        final AggregateMetric[] ref = new AggregateMetric[1];
        metricContext.enterRule(new JQLBaseListener() {
            private void accept(AggregateMetric value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterAggregateQualified(JQLParser.AggregateQualifiedContext ctx) {
                final List<String> scope = Collections.singletonList(ctx.field.getText().toUpperCase());
                final AggregateMetric metric;
                if (ctx.syntacticallyAtomicJqlAggregateMetric() != null) {
                    metric = parseSyntacticallyAtomicJQLAggregateMetric(ctx.syntacticallyAtomicJqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields);
                } else {
                    throw new IllegalStateException();
                }
                accept(new AggregateMetric.Qualified(scope, metric));
            }

            @Override
            public void enterAggregateMultiplyOrDivideOrModulus(JQLParser.AggregateMultiplyOrDivideOrModulusContext ctx) {
                final AggregateMetric left = parseJQLAggregateMetric(ctx.jqlAggregateMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
                final AggregateMetric right = parseJQLAggregateMetric(ctx.jqlAggregateMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
                if (ctx.divide != null) {
                    accept(new AggregateMetric.Divide(left, right));
                } else if (ctx.multiply != null) {
                    accept(new AggregateMetric.Multiply(left, right));
                } else if (ctx.modulus != null) {
                    accept(new AggregateMetric.Modulus(left, right));
                }
            }

            @Override
            public void enterAggregatePlusOrMinus(JQLParser.AggregatePlusOrMinusContext ctx) {
                final AggregateMetric left = parseJQLAggregateMetric(ctx.jqlAggregateMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
                final AggregateMetric right = parseJQLAggregateMetric(ctx.jqlAggregateMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
                if (ctx.plus != null) {
                    accept(new AggregateMetric.Add(left, right));
                } else if (ctx.minus != null) {
                    accept(new AggregateMetric.Subtract(left, right));
                }
            }

            public void enterAggregatePower(JQLParser.AggregatePowerContext ctx) {
                accept(new AggregateMetric.Power(parseJQLAggregateMetric(ctx.jqlAggregateMetric(0), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock), parseJQLAggregateMetric(ctx.jqlAggregateMetric(1), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock)));
            }

            public void enterAggregateNegate(JQLParser.AggregateNegateContext ctx) {
                accept(new AggregateMetric.Negate(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock)));
            }

            @Override
            public void enterAggregateNamed(JQLParser.AggregateNamedContext ctx) {
                final AggregateMetric metric = parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
                final String name = ctx.name.getText().toUpperCase();
                accept(new AggregateMetric.Named(metric, name));
            }

            @Override
            public void enterAggregateIfThenElse(JQLParser.AggregateIfThenElseContext ctx) {
                accept(new AggregateMetric.IfThenElse(
                        AggregateFilters.parseJQLAggregateFilter(ctx.jqlAggregateFilter(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock),
                        AggregateMetrics.parseJQLAggregateMetric(ctx.trueCase, datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock),
                        AggregateMetrics.parseJQLAggregateMetric(ctx.falseCase, datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock)
                ));
            }

            @Override
            public void enterAggregateParens(JQLParser.AggregateParensContext ctx) {
                accept(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock));
            }

            @Override
            public void enterAggregateParent(JQLParser.AggregateParentContext ctx) {
                accept(new AggregateMetric.Parent(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock)));
            }

            @Override
            public void enterAggregateLag(JQLParser.AggregateLagContext ctx) {
                accept(new AggregateMetric.Lag(Integer.parseInt(ctx.NAT().getText()), parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock)));
            }

            @Override
            public void enterAggregateAvg(JQLParser.AggregateAvgContext ctx) {
                accept(new AggregateMetric.Divide(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock), new AggregateMetric.DocStats(new DocMetric.Count())));
            }

            @Override
            public void enterAggregateLog(JQLParser.AggregateLogContext ctx) {
                accept(new AggregateMetric.Log(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock)));
            }

            @Override
            public void enterAggregateStandardDeviation(JQLParser.AggregateStandardDeviationContext ctx) {
                accept(new AggregateMetric.Power(variance(DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock)), new AggregateMetric.Constant(0.5)));
            }

            @Override
            public void enterAggregatePDiff(JQLParser.AggregatePDiffContext ctx) {
                final AggregateMetric actual = parseJQLAggregateMetric(ctx.actual, datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
                final AggregateMetric expected = parseJQLAggregateMetric(ctx.expected, datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
                // 100 * (actual - expected) / expected
                accept(new AggregateMetric.Multiply(new AggregateMetric.Constant(100), new AggregateMetric.Divide(new AggregateMetric.Subtract(actual, expected), expected)));
            }

            @Override
            public void enterAggregateSum(JQLParser.AggregateSumContext ctx) {
                accept(new AggregateMetric.DocStats(DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock)));
            }

            @Override
            public void enterAggregateVariance(JQLParser.AggregateVarianceContext ctx) {
                accept(variance(DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock)));
            }

            @Override
            public void enterAggregateAbs(JQLParser.AggregateAbsContext ctx) {
                accept(new AggregateMetric.Abs(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock)));
            }

            @Override
            public void enterAggregateWindow(JQLParser.AggregateWindowContext ctx) {
                if (ctx.old != null) {
                    warn.accept("Using WINDOW instead of WINDOW_SUM. WINDOW is deprecated because it is deceptive.");
                }
                accept(new AggregateMetric.Window(Integer.parseInt(ctx.NAT().getText()), parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock)));
            }

            @Override
            public void enterAggregateDistinctWindow(JQLParser.AggregateDistinctWindowContext ctx) {
                final Optional<AggregateFilter> filter;
                if (ctx.jqlAggregateFilter() == null) {
                    filter = Optional.absent();
                } else {
                    filter = Optional.of(AggregateFilters.parseJQLAggregateFilter(ctx.jqlAggregateFilter(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock));
                }
                final ScopedField scopedField = ScopedField.parseFrom(ctx.scopedField());
                accept(scopedField.wrap(new AggregateMetric.Distinct(scopedField.field, filter, Optional.of(Integer.parseInt(ctx.NAT().getText())))));
            }

            @Override
            public void enterAggregatePercentile(JQLParser.AggregatePercentileContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.scopedField());
                accept(scopedField.wrap(new AggregateMetric.Percentile(scopedField.field, Double.parseDouble(ctx.number().getText()))));
            }

            @Override
            public void enterAggregateRunning(JQLParser.AggregateRunningContext ctx) {
                accept(new AggregateMetric.Running(1, parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock)));
            }


            @Override
            public void enterAggregateDistinct(JQLParser.AggregateDistinctContext ctx) {
                final Optional<AggregateFilter> filter;
                if (ctx.jqlAggregateFilter() == null) {
                    filter = Optional.absent();
                } else {
                    filter = Optional.of(AggregateFilters.parseJQLAggregateFilter(ctx.jqlAggregateFilter(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock));
                }
                final ScopedField scopedField = ScopedField.parseFrom(ctx.scopedField());
                accept(scopedField.wrap(new AggregateMetric.Distinct(scopedField.field, filter, Optional.<Integer>absent())));
            }

            @Override
            public void enterAggregateSumAcross(JQLParser.AggregateSumAcrossContext ctx) {
                accept(new AggregateMetric.SumAcross(GroupBys.parseGroupBy(ctx.groupByElement(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock), parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock)));
            }

            @Override
            public void enterAggregateAverageAcross(JQLParser.AggregateAverageAcrossContext ctx) {
                final Optional<AggregateFilter> filter;
                if (ctx.jqlAggregateFilter() != null) {
                    filter = Optional.of(AggregateFilters.parseJQLAggregateFilter(ctx.jqlAggregateFilter(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock));
                } else {
                    filter = Optional.absent();
                }
                if (ctx.havingBrackets != null) {
                    warn.accept("Used square brackets in AVG_OVER HAVING. This is no longer necessary and is deprecated.");
                }
                final String field = ctx.field.getText().toUpperCase();
                final GroupBy groupBy = new GroupBy.GroupByField(field, filter, Optional.<Long>absent(), Optional.<AggregateMetric>absent(), false, false);
                accept(new AggregateMetric.Divide(
                        new AggregateMetric.SumAcross(groupBy, AggregateMetrics.parseJQLAggregateMetric(ctx.jqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock)),
                        new AggregateMetric.Distinct(field, filter, Optional.<Integer>absent())
                ));
            }

            @Override
            public void enterAggregateFieldMin(JQLParser.AggregateFieldMinContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.scopedField());
                accept(scopedField.wrap(new AggregateMetric.FieldMin(scopedField.field)));
            }

            @Override
            public void enterAggregateFieldMax(JQLParser.AggregateFieldMaxContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.scopedField());
                accept(scopedField.wrap(new AggregateMetric.FieldMax(scopedField.field)));
            }

            @Override
            public void enterAggregateMetricMin(JQLParser.AggregateMetricMinContext ctx) {
                final List<AggregateMetric> metrics = new ArrayList<>();
                for (final JQLParser.JqlAggregateMetricContext metric : ctx.metrics) {
                    metrics.add(parseJQLAggregateMetric(metric, datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock));
                }
                accept(new AggregateMetric.Min(metrics));
            }

            @Override
            public void enterAggregateMetricMax(JQLParser.AggregateMetricMaxContext ctx) {
                final List<AggregateMetric> metrics = new ArrayList<>();
                for (final JQLParser.JqlAggregateMetricContext metric : ctx.metrics) {
                    metrics.add(parseJQLAggregateMetric(metric, datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock));
                }
                accept(new AggregateMetric.Max(metrics));
            }

            @Override
            public void enterAggregateDocMetricAtom(JQLParser.AggregateDocMetricAtomContext ctx) {
                accept(new AggregateMetric.ImplicitDocStats(DocMetrics.parseJQLDocMetricAtom(ctx.jqlDocMetricAtom())));
            }

            @Override
            public void enterSyntacticallyAtomicAggregateMetric(JQLParser.SyntacticallyAtomicAggregateMetricContext ctx) {
                accept(parseSyntacticallyAtomicJQLAggregateMetric(ctx.syntacticallyAtomicJqlAggregateMetric(), datasetToKeywordAnalyzerFields, datasetToIntFields));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled aggregate metric: [" + metricContext.getText() + "]");
        }

        return ref[0];
    }

    public static AggregateMetric variance(DocMetric docMetric) {
        // [m * m] / count()
        final AggregateMetric firstHalf = new AggregateMetric.Divide(new AggregateMetric.DocStats(new DocMetric.Multiply(docMetric, docMetric)), new AggregateMetric.DocStats(new DocMetric.Count()));
        // [m] / count()
        final AggregateMetric halfOfSecondHalf = new AggregateMetric.Divide(new AggregateMetric.DocStats(docMetric), new AggregateMetric.DocStats(new DocMetric.Count()));
        // ([m] / count()) ^ 2
        final AggregateMetric secondHalf = new AggregateMetric.Multiply(halfOfSecondHalf, halfOfSecondHalf);
        // E(m^2) - E(m)^2
        return new AggregateMetric.Subtract(firstHalf, secondHalf);
    }

}
