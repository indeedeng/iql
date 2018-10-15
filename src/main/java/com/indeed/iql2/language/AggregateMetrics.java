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
import java.util.function.Consumer;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.query.GroupBys;
import com.indeed.iql2.language.JQLBaseListener;
import com.indeed.iql2.language.JQLParser;
import com.indeed.util.core.time.WallClock;
import org.antlr.v4.runtime.Token;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.indeed.iql2.language.Identifiers.parseIdentifier;

public class AggregateMetrics {
    public static AggregateMetric parseAggregateMetric(JQLParser.AggregateMetricContext metricContext, List<String> options, final DatasetsMetadata datasetsMetadata, Consumer<String> warn, WallClock clock) {
        if (metricContext.jqlAggregateMetric() != null) {
            return parseJQLAggregateMetric(metricContext.jqlAggregateMetric(), options, datasetsMetadata, warn, clock);
        }
        if (metricContext.legacyAggregateMetric() != null) {
            return parseLegacyAggregateMetric(metricContext.legacyAggregateMetric(), datasetsMetadata);
        }
        throw new UnsupportedOperationException("This should be unreachable");
    }

    public static AggregateMetric parseLegacyAggregateMetric(JQLParser.LegacyAggregateMetricContext metricContext, final DatasetsMetadata datasetsMetadata) {
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
                accept(new AggregateMetric.Divide(parseLegacyAggregateMetric(ctx.legacyAggregateMetric(), datasetsMetadata), new AggregateMetric.Constant(Double.parseDouble(ctx.number().getText()))));
            }

            @Override
            public void enterLegacyAggregatePercentile(JQLParser.LegacyAggregatePercentileContext ctx) {
                accept(new AggregateMetric.Percentile(parseIdentifier(ctx.identifier()), Double.parseDouble(ctx.number().getText())));
            }

            @Override
            public void enterLegacyAggregateDiv(JQLParser.LegacyAggregateDivContext ctx) {
                final DocMetric divisor = DocMetrics.parseLegacyDocMetric(ctx.legacyDocMetric(1), datasetsMetadata);
                final AggregateMetric aggDivisor;
                if (divisor instanceof DocMetric.Constant) {
                    final DocMetric.Constant constant = (DocMetric.Constant) divisor;
                    aggDivisor = new AggregateMetric.Constant(constant.value);
                } else {
                    aggDivisor = new AggregateMetric.DocStats(divisor);
                }
                accept(new AggregateMetric.Divide(
                        new AggregateMetric.DocStats(DocMetrics.parseLegacyDocMetric(ctx.legacyDocMetric(0), datasetsMetadata)),
                        aggDivisor
                ));
            }

            @Override
            public void enterLegacyAggregateDistinct(JQLParser.LegacyAggregateDistinctContext ctx) {
                accept(new AggregateMetric.Distinct(parseIdentifier(ctx.identifier()), Optional.<AggregateFilter>absent(), Optional.<Integer>absent()));
            }

            @Override
            public void enterLegacyImplicitSum(JQLParser.LegacyImplicitSumContext ctx) {
                accept(new AggregateMetric.DocStats(DocMetrics.parseLegacyDocMetric(ctx.legacyDocMetric(), datasetsMetadata)));
            }

            @Override
            public void enterLegacyAggregateParens(JQLParser.LegacyAggregateParensContext ctx) {
                accept(parseLegacyAggregateMetric(ctx.legacyAggregateMetric(), datasetsMetadata));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled legacy aggregate metric: [" + metricContext.getText() + "]");
        }

        ref[0].copyPosition(metricContext);

        return ref[0];
    }

    public static AggregateMetric parseSyntacticallyAtomicJQLAggregateMetric(JQLParser.SyntacticallyAtomicJqlAggregateMetricContext ctx, final DatasetsMetadata datasetsMetadata) {
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
                accept(new AggregateMetric.DocStats(DocMetrics.parseJQLSyntacticallyAtomicDocMetricAtom(ctx.jqlSyntacticallyAtomicDocMetricAtom())));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled aggregate metric: [" + ctx.getText() + "]");
        }

        ref[0].copyPosition(ctx);

        return ref[0];
    }

    public static AggregateMetric parseJQLAggregateMetric(JQLParser.JqlAggregateMetricContext metricContext, final List<String> options, final DatasetsMetadata datasetsMetadata, final Consumer<String> warn, final WallClock clock) {
        final AggregateMetric[] ref = new AggregateMetric[1];
        metricContext.enterRule(new JQLBaseListener() {
            private void accept(AggregateMetric value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterAggregateQualified(JQLParser.AggregateQualifiedContext ctx) {
                final List<String> scope = Collections.singletonList(parseIdentifier(ctx.field).unwrap());
                final AggregateMetric metric;
                if (ctx.syntacticallyAtomicJqlAggregateMetric() != null) {
                    metric = parseSyntacticallyAtomicJQLAggregateMetric(ctx.syntacticallyAtomicJqlAggregateMetric(), datasetsMetadata);
                } else {
                    throw new IllegalStateException();
                }
                accept(new AggregateMetric.Qualified(scope, metric));
            }

            @Override
            public void enterAggregateMultiplyOrDivideOrModulus(JQLParser.AggregateMultiplyOrDivideOrModulusContext ctx) {
                final AggregateMetric left = parseJQLAggregateMetric(ctx.jqlAggregateMetric(0), options, datasetsMetadata, warn, clock);
                final AggregateMetric right = parseJQLAggregateMetric(ctx.jqlAggregateMetric(1), options, datasetsMetadata, warn, clock);
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
                final AggregateMetric left = parseJQLAggregateMetric(ctx.jqlAggregateMetric(0), options, datasetsMetadata, warn, clock);
                final AggregateMetric right = parseJQLAggregateMetric(ctx.jqlAggregateMetric(1), options, datasetsMetadata, warn, clock);
                if (ctx.plus != null) {
                    accept(new AggregateMetric.Add(left, right));
                } else if (ctx.minus != null) {
                    accept(new AggregateMetric.Subtract(left, right));
                }
            }

            public void enterAggregatePower(JQLParser.AggregatePowerContext ctx) {
                accept(new AggregateMetric.Power(parseJQLAggregateMetric(ctx.jqlAggregateMetric(0), options, datasetsMetadata, warn, clock), parseJQLAggregateMetric(ctx.jqlAggregateMetric(1), options, datasetsMetadata, warn, clock)));
            }

            public void enterAggregateNegate(JQLParser.AggregateNegateContext ctx) {
                accept(new AggregateMetric.Negate(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), options, datasetsMetadata, warn, clock)));
            }

            @Override
            public void enterAggregateNamed(JQLParser.AggregateNamedContext ctx) {
                final AggregateMetric metric = parseJQLAggregateMetric(ctx.jqlAggregateMetric(), options, datasetsMetadata, warn, clock);
                final Positioned<String> name = parseIdentifier(ctx.name);
                accept(new AggregateMetric.Named(metric, name));
            }

            @Override
            public void enterAggregateIfThenElse(JQLParser.AggregateIfThenElseContext ctx) {
                accept(new AggregateMetric.IfThenElse(
                        AggregateFilters.parseJQLAggregateFilter(ctx.jqlAggregateFilter(), options, datasetsMetadata, warn, clock),
                        AggregateMetrics.parseJQLAggregateMetric(ctx.trueCase, options, datasetsMetadata, warn, clock),
                        AggregateMetrics.parseJQLAggregateMetric(ctx.falseCase, options, datasetsMetadata, warn, clock)
                ));
            }

            @Override
            public void enterAggregateParens(JQLParser.AggregateParensContext ctx) {
                accept(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), options, datasetsMetadata, warn, clock));
            }

            @Override
            public void enterAggregateParent(JQLParser.AggregateParentContext ctx) {
                accept(new AggregateMetric.Parent(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), options, datasetsMetadata, warn, clock)));
            }

            @Override
            public void enterAggregateLag(JQLParser.AggregateLagContext ctx) {
                accept(new AggregateMetric.Lag(Integer.parseInt(ctx.NAT().getText()), parseJQLAggregateMetric(ctx.jqlAggregateMetric(), options, datasetsMetadata, warn, clock)));
            }

            @Override
            public void enterAggregateAvg(JQLParser.AggregateAvgContext ctx) {
                accept(new AggregateMetric.DivideByCount(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), options, datasetsMetadata, warn, clock)));
            }

            @Override
            public void enterAggregateLog(JQLParser.AggregateLogContext ctx) {
                accept(new AggregateMetric.Log(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), options, datasetsMetadata, warn, clock)));
            }

            @Override
            public void enterAggregateStandardDeviation(JQLParser.AggregateStandardDeviationContext ctx) {
                accept(new AggregateMetric.Power(variance(DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(), options, datasetsMetadata, warn, clock)), new AggregateMetric.Constant(0.5)));
            }

            @Override
            public void enterAggregatePDiff(JQLParser.AggregatePDiffContext ctx) {
                final AggregateMetric actual = parseJQLAggregateMetric(ctx.actual, options, datasetsMetadata, warn, clock);
                final AggregateMetric expected = parseJQLAggregateMetric(ctx.expected, options, datasetsMetadata, warn, clock);
                // 100 * (actual - expected) / expected
                accept(new AggregateMetric.Multiply(new AggregateMetric.Constant(100), new AggregateMetric.Divide(new AggregateMetric.Subtract(actual, expected), expected)));
            }

            @Override
            public void enterAggregateDiff(JQLParser.AggregateDiffContext ctx) {

                final AggregateMetric controlGrp = parseJQLAggregateMetric(ctx.controlGrp, options, datasetsMetadata, warn, clock);
                final AggregateMetric testGrp = parseJQLAggregateMetric(ctx.testGrp, options, datasetsMetadata, warn, clock);

                accept(new AggregateMetric.Abs(new AggregateMetric.Subtract(controlGrp, testGrp)));
            }

            @Override
            public void enterAggregateRatioDiff(JQLParser.AggregateRatioDiffContext ctx) {

                final AggregateMetric controlClcMetric = parseJQLAggregateMetric(ctx.controlClcMetric, options, datasetsMetadata, warn, clock);
                final AggregateMetric controlImpMetric = parseJQLAggregateMetric(ctx.controlImpMetric, options, datasetsMetadata, warn, clock);
                final AggregateMetric testClcMetric = parseJQLAggregateMetric(ctx.testClcMetric, options, datasetsMetadata, warn, clock);
                final AggregateMetric testImpMetric = parseJQLAggregateMetric(ctx.testImpMetric, options, datasetsMetadata, warn, clock);

                final AggregateMetric controlRatio = new AggregateMetric.Divide(controlClcMetric,controlImpMetric);
                final AggregateMetric testRatio = new AggregateMetric.Divide(testClcMetric,testImpMetric);

                accept(new AggregateMetric.Abs(new AggregateMetric.Subtract(controlRatio, testRatio)));
            }

            @Override
            public void enterAggregateSingleScorer(JQLParser.AggregateSingleScorerContext ctx){
                final AggregateMetric controlGrp = parseJQLAggregateMetric(ctx.controlGrp, options, datasetsMetadata, warn, clock);
                final AggregateMetric testGrp = parseJQLAggregateMetric(ctx.testGrp, options, datasetsMetadata, warn, clock);
                final AggregateMetric controlParent = new AggregateMetric.Parent(controlGrp);
                final AggregateMetric testParent = new AggregateMetric.Parent(testGrp);

                final AggregateFilter singleCondition = new AggregateFilter.Lt(controlGrp, new AggregateMetric.Subtract(controlParent, controlGrp));

                final AggregateMetric actualDiff = new AggregateMetric.Subtract(new AggregateMetric.Subtract(controlParent,controlGrp), new AggregateMetric.Subtract(testParent,testGrp));

                final AggregateMetric defaultFraction = new AggregateMetric.Divide(new AggregateMetric.Subtract(controlParent,controlGrp), controlParent);
                final AggregateMetric totalDiff = new AggregateMetric.Subtract(controlParent, testParent);
                final AggregateMetric expectedDiff = new AggregateMetric.Multiply(defaultFraction,totalDiff);

                final AggregateMetric trueCase = new AggregateMetric.Abs(new AggregateMetric.Subtract(actualDiff,expectedDiff));
                final AggregateMetric falseCase = new AggregateMetric.Constant(0);

                accept(new AggregateMetric.IfThenElse(singleCondition, trueCase, falseCase));
            }



            @Override
            public void enterAggregateRatioScorer(JQLParser.AggregateRatioScorerContext ctx){

                final AggregateMetric controlClcMetric = parseJQLAggregateMetric(ctx.controlClcMetric, options, datasetsMetadata, warn, clock);
                final AggregateMetric controlImpMetric = parseJQLAggregateMetric(ctx.controlImpMetric, options, datasetsMetadata, warn, clock);
                final AggregateMetric testClcMetric = parseJQLAggregateMetric(ctx.testClcMetric, options, datasetsMetadata, warn, clock);
                final AggregateMetric testImpMetric = parseJQLAggregateMetric(ctx.testImpMetric, options, datasetsMetadata, warn, clock);

                final AggregateMetric parentControlClcMetric = new AggregateMetric.Parent(controlClcMetric);
                final AggregateMetric parentControlImpMetric = new AggregateMetric.Parent(controlImpMetric);
                final AggregateMetric parentTestClcMetric = new AggregateMetric.Parent(testClcMetric);
                final AggregateMetric parentTestImpMetric = new AggregateMetric.Parent(testImpMetric);

                final AggregateFilter ratioCondition = new AggregateFilter.Lt(controlImpMetric, new AggregateMetric.Subtract(parentControlImpMetric, controlImpMetric));

                final AggregateMetric controlDefaultRatio = new AggregateMetric.Divide(new AggregateMetric.Subtract(parentControlClcMetric,controlClcMetric), new AggregateMetric.Subtract(parentControlImpMetric,controlImpMetric));
                final AggregateMetric testDefaultRatio = new AggregateMetric.Divide(new AggregateMetric.Subtract(parentTestClcMetric,testClcMetric), new AggregateMetric.Subtract(parentTestImpMetric,testImpMetric));
                final AggregateMetric actualDiff = new AggregateMetric.Subtract(controlDefaultRatio, testDefaultRatio);

                final AggregateMetric controlTotalRatio = new AggregateMetric.Divide(parentControlClcMetric, parentControlImpMetric);
                final AggregateMetric testTotalRatio = new AggregateMetric.Divide(parentTestClcMetric, parentTestImpMetric);
                final AggregateMetric expectedDiff = new AggregateMetric.Subtract(controlTotalRatio, testTotalRatio);

                final AggregateMetric trueCase = new AggregateMetric.Abs(new AggregateMetric.Subtract(actualDiff, expectedDiff));
                final AggregateMetric falseCase = new AggregateMetric.Constant(0);

                accept(new AggregateMetric.IfThenElse(ratioCondition, trueCase, falseCase));
            }

            @Override
            public void enterAggregateRMSError(JQLParser.AggregateRMSErrorContext ctx){

                final int lowerLimit = Integer.parseInt(ctx.lowerLimit.getText());
                final int upperLimit = Integer.parseInt(ctx.upperLimit.getText());
                final int stepSize = Integer.parseInt(ctx.stepSize.getText());
                final String useRatio = ctx.useRatio!=null ? ctx.useRatio.getText() : null;

                final AggregateMetric predictedVal = parseJQLAggregateMetric(ctx.predictedVal, options, datasetsMetadata, warn, clock);
                final AggregateMetric actualVal = parseJQLAggregateMetric(ctx.actualVal, options, datasetsMetadata, warn, clock);
                final AggregateMetric totalCount = parseJQLAggregateMetric(ctx.total, options, datasetsMetadata, warn, clock);
                final DocMetric groupingMetric = DocMetrics.parseJQLDocMetric(ctx.grouping, options, datasetsMetadata, warn, clock);

                final GroupBy.GroupByMetric modelGrouping = new GroupBy.GroupByMetric(groupingMetric, lowerLimit, upperLimit, stepSize, true, true);;

                final AggregateMetric modelRatio;
                if (useRatio != null && useRatio.toLowerCase().equals("true")){
                    modelRatio = new AggregateMetric.Subtract(new AggregateMetric.Divide(predictedVal,actualVal),new AggregateMetric.Constant(1.0));
                } else {
                    modelRatio = new AggregateMetric.Subtract(predictedVal,actualVal);
                }

                final AggregateMetric squaredDev = new AggregateMetric.Power(modelRatio, new AggregateMetric.Constant(2));
                final AggregateMetric weightedDev = new AggregateMetric.Multiply(totalCount, squaredDev);

                final AggregateMetric totalError = new AggregateMetric.SumAcross(modelGrouping, weightedDev);
                final AggregateMetric meanError = new AggregateMetric.Divide(totalError,totalCount);
                accept(new AggregateMetric.Power(meanError, new AggregateMetric.Constant(0.5)));
            }

            @Override
            public void enterAggregateSum(JQLParser.AggregateSumContext ctx) {
                accept(new AggregateMetric.DocStats(DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(), options, datasetsMetadata, warn, clock)));
            }

            @Override
            public void enterAggregateVariance(JQLParser.AggregateVarianceContext ctx) {
                accept(variance(DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(), options, datasetsMetadata, warn, clock)));
            }

            @Override
            public void enterAggregateAbs(JQLParser.AggregateAbsContext ctx) {
                accept(new AggregateMetric.Abs(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), options, datasetsMetadata, warn, clock)));
            }

            @Override
            public void enterAggregateWindow(JQLParser.AggregateWindowContext ctx) {
                if (ctx.old != null) {
                    warn.accept("Using WINDOW instead of WINDOW_SUM. WINDOW is deprecated because it is deceptive.");
                }
                accept(new AggregateMetric.Window(Integer.parseInt(ctx.NAT().getText()), parseJQLAggregateMetric(ctx.jqlAggregateMetric(), options, datasetsMetadata, warn, clock)));
            }

            @Override
            public void enterAggregateDistinctWindow(JQLParser.AggregateDistinctWindowContext ctx) {
                final Optional<AggregateFilter> filter;
                if (ctx.jqlAggregateFilter() == null) {
                    filter = Optional.absent();
                } else {
                    filter = Optional.of(AggregateFilters.parseJQLAggregateFilter(ctx.jqlAggregateFilter(), options, datasetsMetadata, warn, clock));
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
                accept(new AggregateMetric.Running(1, parseJQLAggregateMetric(ctx.jqlAggregateMetric(), options, datasetsMetadata, warn, clock)));
            }


            @Override
            public void enterAggregateDistinct(JQLParser.AggregateDistinctContext ctx) {
                final Optional<AggregateFilter> filter;
                if (ctx.jqlAggregateFilter() == null) {
                    filter = Optional.absent();
                } else {
                    filter = Optional.of(AggregateFilters.parseJQLAggregateFilter(ctx.jqlAggregateFilter(), options, datasetsMetadata, warn, clock));
                }
                final ScopedField scopedField = ScopedField.parseFrom(ctx.scopedField());
                accept(scopedField.wrap(new AggregateMetric.Distinct(scopedField.field, filter, Optional.<Integer>absent())));
            }

            @Override
            public void enterAggregateSumAcross(JQLParser.AggregateSumAcrossContext ctx) {
                accept(new AggregateMetric.SumAcross(GroupBys.parseGroupBy(ctx.groupByElement(), options, datasetsMetadata, warn, clock), parseJQLAggregateMetric(ctx.jqlAggregateMetric(), options, datasetsMetadata, warn, clock)));
            }

            @Override
            public void enterAggregateAverageAcross(JQLParser.AggregateAverageAcrossContext ctx) {
                final Optional<AggregateFilter> filter;
                if (ctx.jqlAggregateFilter() != null) {
                    filter = Optional.of(AggregateFilters.parseJQLAggregateFilter(ctx.jqlAggregateFilter(), options, datasetsMetadata, warn, clock));
                } else {
                    filter = Optional.absent();
                }
                if (ctx.havingBrackets != null) {
                    warn.accept("Used square brackets in AVG_OVER HAVING. This is no longer necessary and is deprecated.");
                }
                final ScopedField scopedField = ScopedField.parseFrom(ctx.field);
                final GroupBy groupBy = new GroupBy.GroupByField(scopedField.field, filter, Optional.<Long>absent(), Optional.<AggregateMetric>absent(), false, false);
                accept(scopedField.wrap(new AggregateMetric.Divide(
                        new AggregateMetric.SumAcross(groupBy, AggregateMetrics.parseJQLAggregateMetric(ctx.jqlAggregateMetric(), options, datasetsMetadata, warn, clock)),
                        new AggregateMetric.Distinct(scopedField.field, filter, Optional.<Integer>absent())
                )));
            }

            @Override
            public void enterAggregateBootstrap(JQLParser.AggregateBootstrapContext ctx) {
                final ScopedField scopedField = ScopedField.parseFrom(ctx.field);
                final Optional<AggregateFilter> filter;
                if (ctx.filter != null) {
                    filter = Optional.of(AggregateFilters.parseJQLAggregateFilter(ctx.filter, options, datasetsMetadata, warn, clock));
                } else {
                    filter = Optional.absent();
                }
                final AggregateMetric metric = AggregateMetrics.parseJQLAggregateMetric(ctx.metric, options, datasetsMetadata, warn, clock);
                final int numBootstraps = Integer.parseInt(ctx.numBootstraps.getText());
                final List<String> varargs = new ArrayList<>();
                for (final Token vararg : ctx.varargs) {
                    varargs.add(vararg.getText());
                }
                accept(scopedField.wrap(new AggregateMetric.Bootstrap(scopedField.field, filter, ParserCommon.unquote(ctx.seed.getText()), metric, numBootstraps, varargs)));
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
                    metrics.add(parseJQLAggregateMetric(metric, options, datasetsMetadata, warn, clock));
                }
                accept(new AggregateMetric.Min(metrics));
            }

            @Override
            public void enterAggregateMetricMax(JQLParser.AggregateMetricMaxContext ctx) {
                final List<AggregateMetric> metrics = new ArrayList<>();
                for (final JQLParser.JqlAggregateMetricContext metric : ctx.metrics) {
                    metrics.add(parseJQLAggregateMetric(metric, options, datasetsMetadata, warn, clock));
                }
                accept(new AggregateMetric.Max(metrics));
            }

            @Override
            public void enterAggregateDocMetricAtom(JQLParser.AggregateDocMetricAtomContext ctx) {
                accept(new AggregateMetric.DocStats(DocMetrics.parseJQLDocMetricAtom(ctx.jqlDocMetricAtom(), datasetsMetadata)));
            }

            @Override
            public void enterAggregateMetricFilter(JQLParser.AggregateMetricFilterContext ctx) {
                final AggregateFilter filter = AggregateFilters.parseJQLAggregateFilter(ctx.jqlAggregateFilter(), options, datasetsMetadata, warn, clock);
                accept(new AggregateMetric.IfThenElse(filter, new AggregateMetric.Constant(1), new AggregateMetric.Constant(0)));
            }

            @Override
            public void enterSyntacticallyAtomicAggregateMetric(JQLParser.SyntacticallyAtomicAggregateMetricContext ctx) {
                accept(parseSyntacticallyAtomicJQLAggregateMetric(ctx.syntacticallyAtomicJqlAggregateMetric(), datasetsMetadata));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled aggregate metric: [" + metricContext.getText() + "]");
        }

        ref[0].copyPosition(metricContext);

        return ref[0];
    }

    public static AggregateMetric variance(DocMetric docMetric) {
        // [m * m] / count()
        final AggregateMetric firstHalf = new AggregateMetric.DivideByCount(new AggregateMetric.DocStats(new DocMetric.Multiply(docMetric, docMetric)));
        // [m] / count()
        final AggregateMetric halfOfSecondHalf = new AggregateMetric.DivideByCount(new AggregateMetric.DocStats(docMetric));
        // ([m] / count()) ^ 2
        final AggregateMetric secondHalf = new AggregateMetric.Multiply(halfOfSecondHalf, halfOfSecondHalf);
        // E(m^2) - E(m)^2
        return new AggregateMetric.Subtract(firstHalf, secondHalf);
    }

}
