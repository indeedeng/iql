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

import com.google.common.collect.Sets;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.query.GroupBys;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.query.fieldresolution.ScopedFieldResolver;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class AggregateMetrics {
    private AggregateMetrics() {
    }

    // The max digits after decimal point for floor/ceil/round
    private static final int ROUNDING_MAX_DIGITS = 10;

    public static AggregateMetric parseAggregateMetric(
            final JQLParser.AggregateMetricContext metricContext,
            final Query.Context context) {
        if (metricContext.jqlAggregateMetric() != null) {
            return parseJQLAggregateMetric(metricContext.jqlAggregateMetric(), context);
        }
        if (metricContext.legacyAggregateMetric() != null) {
            return parseLegacyAggregateMetric(metricContext.legacyAggregateMetric(), context.fieldResolver, context.datasetsMetadata);
        }
        throw new UnsupportedOperationException("This should be unreachable");
    }

    public static AggregateMetric parseLegacyAggregateMetric(JQLParser.LegacyAggregateMetricContext metricContext, final ScopedFieldResolver fieldResolver, final DatasetsMetadata datasetsMetadata) {
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
                accept(new AggregateMetric.Divide(parseLegacyAggregateMetric(ctx.legacyAggregateMetric(), fieldResolver, datasetsMetadata), new AggregateMetric.Constant(Double.parseDouble(ctx.number().getText()))));
            }

            @Override
            public void enterLegacyAggregatePercentile(JQLParser.LegacyAggregatePercentileContext ctx) {
                accept(new AggregateMetric.Percentile(fieldResolver.resolve(ctx.identifier()), Double.parseDouble(ctx.number().getText())));
            }

            @Override
            public void enterLegacyAggregateDiv(JQLParser.LegacyAggregateDivContext ctx) {
                AggregateMetric aggDivisor = parsePossibleDimensionAggregateMetric(ctx.legacyDocMetric(1), fieldResolver, datasetsMetadata);
                if (aggDivisor instanceof AggregateMetric.DocStats) {
                    final DocMetric docMetric = ((AggregateMetric.DocStats) aggDivisor).docMetric;
                    if (docMetric instanceof DocMetric.Constant) {
                        aggDivisor = new AggregateMetric.Constant(((DocMetric.Constant) docMetric).value);
                    }
                }
                accept(new AggregateMetric.Divide(
                        parsePossibleDimensionAggregateMetric(ctx.legacyDocMetric(0), fieldResolver, datasetsMetadata),
                        aggDivisor
                ));
            }

            @Override
            public void enterLegacyAggregateDistinct(JQLParser.LegacyAggregateDistinctContext ctx) {
                accept(new AggregateMetric.Distinct(fieldResolver.resolve(ctx.identifier()), Optional.empty(), Optional.empty()));
            }

            @Override
            public void enterLegacyImplicitSum(JQLParser.LegacyImplicitSumContext ctx) {
                accept(parsePossibleDimensionAggregateMetric(ctx.legacyDocMetric(), fieldResolver, datasetsMetadata));
            }

            @Override
            public void enterLegacyAggregateParens(JQLParser.LegacyAggregateParensContext ctx) {
                accept(parseLegacyAggregateMetric(ctx.legacyAggregateMetric(), fieldResolver, datasetsMetadata));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled legacy aggregate metric: [" + metricContext.getText() + "]");
        }

        ref[0].copyPosition(metricContext);

        return ref[0];
    }

    private static AggregateMetric parsePossibleDimensionAggregateMetric(final JQLParser.LegacyDocMetricContext ctx, final ScopedFieldResolver fieldResolver, final DatasetsMetadata datasetsMetadata) {
        final JQLParser.IdentifierContext identifier = DocMetrics.asPlainField(ctx);
        if (identifier == null) {
            return new AggregateMetric.DocStats(DocMetrics.parseLegacyDocMetric(ctx, fieldResolver, datasetsMetadata));
        }
        return fieldResolver.resolveAggregateMetric(identifier);
    }

    private static AggregateMetric parsePossibleDimensionAggregateMetric(final JQLParser.JqlSyntacticallyAtomicDocMetricAtomContext ctx, final ScopedFieldResolver fieldResolver) {
        final JQLParser.SinglyScopedFieldContext identifier = DocMetrics.asPlainField(ctx);
        if (identifier == null) {
            return new AggregateMetric.DocStats(DocMetrics.parseJQLSyntacticallyAtomicDocMetricAtom(ctx, fieldResolver));
        }
        return fieldResolver.resolveAggregateMetric(identifier);
    }

    private static AggregateMetric parsePossibleDimensionAggregateMetric(final JQLParser.JqlDocMetricAtomContext ctx, final Query.Context context) {
        final JQLParser.SinglyScopedFieldContext identifier = DocMetrics.asPlainField(ctx);
        if (identifier == null) {
            return new AggregateMetric.DocStats(DocMetrics.parseJQLDocMetricAtom(ctx, context));
        }
        return context.fieldResolver.resolveAggregateMetric(identifier);
    }

    public static AggregateMetric parseSyntacticallyAtomicJQLAggregateMetric(JQLParser.SyntacticallyAtomicJqlAggregateMetricContext ctx, final ScopedFieldResolver fieldResolver) {
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
                final AggregateMetric.NeedsSubstitution alias = parsePossibleMetricSubstitution(ctx.jqlSyntacticallyAtomicDocMetricAtom(), fieldResolver);
                if (alias != null) {
                    accept(alias);
                    return;
                }
                accept(parsePossibleDimensionAggregateMetric(ctx.jqlSyntacticallyAtomicDocMetricAtom(), fieldResolver));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled aggregate metric: [" + ctx.getText() + "]");
        }

        ref[0].copyPosition(ctx);

        return ref[0];
    }

    @Nullable
    private static AggregateMetric.NeedsSubstitution parsePossibleMetricSubstitution(JQLParser.JqlSyntacticallyAtomicDocMetricAtomContext ctx, final ScopedFieldResolver fieldResolver) {
        if (ctx instanceof JQLParser.DocMetricAtomRawFieldContext) {
            final JQLParser.SinglyScopedFieldContext singlyScopedField = ((JQLParser.DocMetricAtomRawFieldContext) ctx).singlyScopedField();
            if (singlyScopedField.oneScope == null) {
                final AggregateMetric.NeedsSubstitution alias = fieldResolver.resolveMetricAlias(singlyScopedField.field);
                if (alias != null) {
                    return alias;
                }
            }
        }
        return null;
    }

    public static AggregateMetric parseJQLAggregateMetric(
            final JQLParser.JqlAggregateMetricContext metricContext,
            final Query.Context context) {
        final AggregateMetric[] ref = new AggregateMetric[1];
        final ScopedFieldResolver fieldResolver = context.fieldResolver;
        metricContext.enterRule(new JQLBaseListener() {
            private void accept(AggregateMetric value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterAggregateQualified(JQLParser.AggregateQualifiedContext ctx) {
                final List<String> scope = Collections.singletonList(fieldResolver.resolveDataset(ctx.field).unwrap());
                final AggregateMetric metric;
                if (ctx.syntacticallyAtomicJqlAggregateMetric() != null) {
                    metric = parseSyntacticallyAtomicJQLAggregateMetric(ctx.syntacticallyAtomicJqlAggregateMetric(), context.fieldResolver.forScope(Sets.newHashSet(scope)));
                } else {
                    throw new IllegalStateException();
                }
                accept(new AggregateMetric.Qualified(scope, metric));
            }

            @Override
            public void enterAggregateMultiplyOrDivideOrModulus(JQLParser.AggregateMultiplyOrDivideOrModulusContext ctx) {
                final AggregateMetric left = parseJQLAggregateMetric(ctx.jqlAggregateMetric(0), context);
                final AggregateMetric right = parseJQLAggregateMetric(ctx.jqlAggregateMetric(1), context);
                if (ctx.divide != null) {
                    accept(new AggregateMetric.Divide(left, right));
                } else if (ctx.multiply != null) {
                    accept(new AggregateMetric.Multiply(left, right));
                } else if (ctx.modulus != null) {
                    accept(new AggregateMetric.Modulus(left, right));
                }
            }

            @Override
            public void enterAggregatePlusOrMinus(final JQLParser.AggregatePlusOrMinusContext ctx) {
                final AggregateMetric left = parseJQLAggregateMetric(ctx.jqlAggregateMetric(0), context);
                final AggregateMetric right = parseJQLAggregateMetric(ctx.jqlAggregateMetric(1), context);
                if (ctx.plus != null) {
                    accept(AggregateMetric.Add.create(left, right));
                } else if (ctx.minus != null) {
                    accept(new AggregateMetric.Subtract(left, right));
                }
            }

            public void enterAggregatePower(JQLParser.AggregatePowerContext ctx) {
                accept(new AggregateMetric.Power(parseJQLAggregateMetric(ctx.jqlAggregateMetric(0), context), parseJQLAggregateMetric(ctx.jqlAggregateMetric(1), context)));
            }

            public void enterAggregateNegate(JQLParser.AggregateNegateContext ctx) {
                accept(new AggregateMetric.Negate(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), context)));
            }

            @Override
            public void enterAggregateNamed(JQLParser.AggregateNamedContext ctx) {
                final AggregateMetric metric = parseJQLAggregateMetric(ctx.jqlAggregateMetric(), context);
                final Positioned<String> name = Identifiers.parseIdentifier(ctx.name);
                accept(new AggregateMetric.Named(metric, name));
            }

            @Override
            public void enterAggregateIfThenElse(JQLParser.AggregateIfThenElseContext ctx) {
                accept(new AggregateMetric.IfThenElse(
                        AggregateFilters.parseJQLAggregateFilter(ctx.jqlAggregateFilter(), context),
                        AggregateMetrics.parseJQLAggregateMetric(ctx.trueCase, context),
                        AggregateMetrics.parseJQLAggregateMetric(ctx.falseCase, context)
                ));
            }

            @Override
            public void enterAggregateParens(JQLParser.AggregateParensContext ctx) {
                accept(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), context));
            }

            @Override
            public void enterAggregateParent(JQLParser.AggregateParentContext ctx) {
                accept(new AggregateMetric.Parent(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), context)));
            }

            @Override
            public void enterAggregateLag(JQLParser.AggregateLagContext ctx) {
                accept(new AggregateMetric.Lag(Integer.parseInt(ctx.NAT().getText()), parseJQLAggregateMetric(ctx.jqlAggregateMetric(), context)));
            }

            @Override
            public void enterAggregateAvg(JQLParser.AggregateAvgContext ctx) {
                accept(new AggregateMetric.DivideByCount(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), context)));
            }

            @Override
            public void enterAggregateLog(JQLParser.AggregateLogContext ctx) {
                accept(new AggregateMetric.Log(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), context)));
            }

            @Override
            public void enterAggregateStandardDeviation(JQLParser.AggregateStandardDeviationContext ctx) {
                accept(new AggregateMetric.Power(variance(DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(), context)), new AggregateMetric.Constant(0.5)));
            }

            @Override
            public void enterAggregatePDiff(JQLParser.AggregatePDiffContext ctx) {
                final AggregateMetric actual = parseJQLAggregateMetric(ctx.actual, context);
                final AggregateMetric expected = parseJQLAggregateMetric(ctx.expected, context);
                // 100 * (actual - expected) / expected
                accept(new AggregateMetric.Multiply(new AggregateMetric.Constant(100), new AggregateMetric.Divide(new AggregateMetric.Subtract(actual, expected), expected)));
            }

            @Override
            public void enterAggregateDiff(JQLParser.AggregateDiffContext ctx) {

                final AggregateMetric controlGrp = parseJQLAggregateMetric(ctx.controlGrp, context);
                final AggregateMetric testGrp = parseJQLAggregateMetric(ctx.testGrp, context);

                accept(new AggregateMetric.Abs(new AggregateMetric.Subtract(controlGrp, testGrp)));
            }

            @Override
            public void enterAggregateRatioDiff(JQLParser.AggregateRatioDiffContext ctx) {

                final AggregateMetric controlClcMetric = parseJQLAggregateMetric(ctx.controlClcMetric, context);
                final AggregateMetric controlImpMetric = parseJQLAggregateMetric(ctx.controlImpMetric, context);
                final AggregateMetric testClcMetric = parseJQLAggregateMetric(ctx.testClcMetric, context);
                final AggregateMetric testImpMetric = parseJQLAggregateMetric(ctx.testImpMetric, context);

                final AggregateMetric controlRatio = new AggregateMetric.Divide(controlClcMetric,controlImpMetric);
                final AggregateMetric testRatio = new AggregateMetric.Divide(testClcMetric,testImpMetric);

                accept(new AggregateMetric.Abs(new AggregateMetric.Subtract(controlRatio, testRatio)));
            }

            @Override
            public void enterAggregateSingleScorer(JQLParser.AggregateSingleScorerContext ctx){
                final AggregateMetric controlGrp = parseJQLAggregateMetric(ctx.controlGrp, context);
                final AggregateMetric testGrp = parseJQLAggregateMetric(ctx.testGrp, context);
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

                final AggregateMetric controlClcMetric = parseJQLAggregateMetric(ctx.controlClcMetric, context);
                final AggregateMetric controlImpMetric = parseJQLAggregateMetric(ctx.controlImpMetric, context);
                final AggregateMetric testClcMetric = parseJQLAggregateMetric(ctx.testClcMetric, context);
                final AggregateMetric testImpMetric = parseJQLAggregateMetric(ctx.testImpMetric, context);

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

                final AggregateMetric predictedVal = parseJQLAggregateMetric(ctx.predictedVal, context);
                final AggregateMetric actualVal = parseJQLAggregateMetric(ctx.actualVal, context);
                final AggregateMetric totalCount = parseJQLAggregateMetric(ctx.total, context);
                final DocMetric groupingMetric = DocMetrics.parseJQLDocMetric(ctx.grouping, context);

                final GroupBy.GroupByMetric modelGrouping = new GroupBy.GroupByMetric(groupingMetric, lowerLimit, upperLimit, stepSize, true, true);

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
            public void enterAggregateLogLoss(final JQLParser.AggregateLogLossContext ctx) {
                final DocFilter label = DocFilters.parseJQLDocFilter(ctx.label, context);
                final DocMetric score = DocMetrics.parseJQLDocMetric(ctx.score, context);
                final int scale = Integer.parseInt(ctx.scale.getText());

                // AVG([if <label> then -log(score, scale) else -log(scale - score, scale)) / scale
                accept(
                        new AggregateMetric.Divide(
                                new AggregateMetric.DivideByCount(
                                        new AggregateMetric.DocStats(
                                                new DocMetric.IfThenElse(
                                                        label,
                                                        new DocMetric.Negate(new DocMetric.Log(score, scale)),
                                                        new DocMetric.Negate(new DocMetric.Log(new DocMetric.Subtract(
                                                                new DocMetric.Constant(scale),
                                                                score
                                                        ), scale))
                                                )
                                        )
                                ),
                                new AggregateMetric.Constant(scale)
                        )
                );
            }

            @Override
            public void enterAggregateSum(JQLParser.AggregateSumContext ctx) {
                accept(new AggregateMetric.DocStats(DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(), context)));
            }

            @Override
            public void enterAggregateVariance(JQLParser.AggregateVarianceContext ctx) {
                accept(variance(DocMetrics.parseJQLDocMetric(ctx.jqlDocMetric(), context)));
            }

            @Override
            public void enterAggregateAbs(JQLParser.AggregateAbsContext ctx) {
                accept(new AggregateMetric.Abs(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), context)));
            }

            @Override
            public void enterAggregateFloor(final JQLParser.AggregateFloorContext ctx) {
                final int digits = ctx.integer() == null ? 0 : Integer.parseInt(ctx.integer().getText());
                if (Math.abs(digits) > ROUNDING_MAX_DIGITS) {
                    throw new IllegalArgumentException("The max digits for FLOOR is " + ROUNDING_MAX_DIGITS);
                }
                accept(new AggregateMetric.Floor(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), context), digits));
            }

            @Override
            public void enterAggregateCeil(final JQLParser.AggregateCeilContext ctx) {
                final int digits = ctx.integer() == null ? 0 : Integer.parseInt(ctx.integer().getText());
                if (Math.abs(digits) > ROUNDING_MAX_DIGITS) {
                    throw new IllegalArgumentException("The max digits for CEIL is " + ROUNDING_MAX_DIGITS);
                }
                accept(new AggregateMetric.Ceil(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), context), digits));
            }

            @Override
            public void enterAggregateRound(final JQLParser.AggregateRoundContext ctx) {
                final int digits = ctx.integer() == null ? 0 : Integer.parseInt(ctx.integer().getText());
                if (Math.abs(digits) > ROUNDING_MAX_DIGITS) {
                    throw new IllegalArgumentException("The max digits for ROUND is " + ROUNDING_MAX_DIGITS);
                }
                accept(new AggregateMetric.Round(parseJQLAggregateMetric(ctx.jqlAggregateMetric(), context), digits));
            }

            @Override
            public void enterAggregateWindow(JQLParser.AggregateWindowContext ctx) {
                if (ctx.old != null) {
                    context.warn.accept("Using WINDOW instead of WINDOW_SUM. WINDOW is deprecated because it is deceptive.");
                }
                accept(new AggregateMetric.Window(Integer.parseInt(ctx.NAT().getText()), parseJQLAggregateMetric(ctx.jqlAggregateMetric(), context)));
            }

            @Override
            public void enterAggregateDistinctWindow(JQLParser.AggregateDistinctWindowContext ctx) {
                final Optional<AggregateFilter> filter;
                if (ctx.jqlAggregateFilter() == null) {
                    filter = Optional.empty();
                } else {
                    filter = Optional.of(AggregateFilters.parseJQLAggregateFilter(ctx.jqlAggregateFilter(), context));
                }
                final FieldSet field = fieldResolver.resolve(ctx.scopedField());
                accept(field.wrap(new AggregateMetric.Distinct(field, filter, Optional.of(Integer.parseInt(ctx.NAT().getText())))));
            }

            @Override
            public void enterAggregatePercentile(JQLParser.AggregatePercentileContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.scopedField());
                accept(field.wrap(new AggregateMetric.Percentile(field, Double.parseDouble(ctx.number().getText()))));
            }

            @Override
            public void enterAggregateMedian(JQLParser.AggregateMedianContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.scopedField());
                accept(field.wrap(new AggregateMetric.Percentile(field, 50)));
            }

            @Override
            public void enterAggregateRunning(JQLParser.AggregateRunningContext ctx) {
                accept(new AggregateMetric.Running(1, parseJQLAggregateMetric(ctx.jqlAggregateMetric(), context)));
            }

            @Override
            public void enterAggregateDistinct(JQLParser.AggregateDistinctContext ctx) {
                final Optional<AggregateFilter> filter;
                if (ctx.jqlAggregateFilter() == null) {
                    filter = Optional.empty();
                } else {
                    filter = Optional.of(AggregateFilters.parseJQLAggregateFilter(ctx.jqlAggregateFilter(), context));
                }
                final FieldSet field = fieldResolver.resolve(ctx.scopedField());
                accept(field.wrap(new AggregateMetric.Distinct(field, filter, Optional.empty())));
            }

            @Override
            public void enterAggregateSumAcross(JQLParser.AggregateSumAcrossContext ctx) {
                accept(new AggregateMetric.SumAcross(
                        GroupBys.parseGroupBy(ctx.groupByElement(), context),
                        parseJQLAggregateMetric(ctx.jqlAggregateMetric(), context)));
            }

            @Override
            public void enterAggregateSumAcross2(final JQLParser.AggregateSumAcross2Context ctx) {
                final Optional<AggregateFilter> filter;
                if (ctx.jqlAggregateFilter() != null) {
                    filter = Optional.of(AggregateFilters.parseJQLAggregateFilter(ctx.jqlAggregateFilter(), context));
                } else {
                    filter = Optional.empty();
                }
                final FieldSet field = fieldResolver.resolve(ctx.field);
                final GroupBy groupBy = new GroupBy.GroupByField(field, filter, Optional.empty(), Optional.empty(), false);
                accept(field.wrap(new AggregateMetric.SumAcross(groupBy, AggregateMetrics.parseJQLAggregateMetric(ctx.jqlAggregateMetric(), context))));
            }

            @Override
            public void enterAggregateAverageAcross(JQLParser.AggregateAverageAcrossContext ctx) {
                final Optional<AggregateFilter> filter;
                if (ctx.jqlAggregateFilter() != null) {
                    filter = Optional.of(AggregateFilters.parseJQLAggregateFilter(ctx.jqlAggregateFilter(), context));
                } else {
                    filter = Optional.empty();
                }
                if (ctx.havingBrackets != null) {
                    context.warn.accept("Used square brackets in AVG_OVER HAVING. This is no longer necessary and is deprecated.");
                }
                final FieldSet field = fieldResolver.resolve(ctx.field);
                final GroupBy groupBy = new GroupBy.GroupByField(field, filter, Optional.empty(), Optional.empty(), false);
                accept(field.wrap(new AggregateMetric.Divide(
                        new AggregateMetric.SumAcross(groupBy, AggregateMetrics.parseJQLAggregateMetric(ctx.jqlAggregateMetric(), context)),
                        new AggregateMetric.Distinct(field, filter, Optional.empty())
                )));
            }

            @Override
            public void enterAggregateFieldMin(JQLParser.AggregateFieldMinContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.scopedField());
                final Optional<AggregateMetric> metric = Optional.ofNullable(ctx.aggregate)
                    .map(m -> AggregateMetrics.parseJQLAggregateMetric(m, context));
                final Optional<AggregateFilter> filter = Optional.ofNullable(ctx.filter)
                    .map(f -> AggregateFilters.parseJQLAggregateFilter(f, context));
                accept(field.wrap(new AggregateMetric.FieldMin(field, metric, filter)));
            }

            @Override
            public void enterAggregateFieldMax(JQLParser.AggregateFieldMaxContext ctx) {
                final FieldSet field = fieldResolver.resolve(ctx.scopedField());
                final Optional<AggregateMetric> metric = Optional.ofNullable(ctx.aggregate)
                    .map(m -> AggregateMetrics.parseJQLAggregateMetric(m, context));
                final Optional<AggregateFilter> filter = Optional.ofNullable(ctx.filter)
                    .map(f -> AggregateFilters.parseJQLAggregateFilter(f, context));
                accept(field.wrap(new AggregateMetric.FieldMax(field, metric, filter)));
            }

            @Override
            public void enterAggregateMetricMin(JQLParser.AggregateMetricMinContext ctx) {
                final List<AggregateMetric> metrics = new ArrayList<>();
                for (final JQLParser.JqlAggregateMetricContext metric : ctx.metrics) {
                    metrics.add(parseJQLAggregateMetric(metric, context));
                }
                accept(new AggregateMetric.Min(metrics));
            }

            @Override
            public void enterAggregateMetricMax(JQLParser.AggregateMetricMaxContext ctx) {
                final List<AggregateMetric> metrics = new ArrayList<>();
                for (final JQLParser.JqlAggregateMetricContext metric : ctx.metrics) {
                    metrics.add(parseJQLAggregateMetric(metric, context));
                }
                accept(new AggregateMetric.Max(metrics));
            }

            @Override
            public void enterAggregateDocMetricAtom(JQLParser.AggregateDocMetricAtomContext ctx) {
                if (ctx.jqlDocMetricAtom() instanceof JQLParser.SyntacticallyAtomicDocMetricAtomContext) {
                    final AggregateMetric.NeedsSubstitution alias = parsePossibleMetricSubstitution(((JQLParser.SyntacticallyAtomicDocMetricAtomContext) ctx.jqlDocMetricAtom()).jqlSyntacticallyAtomicDocMetricAtom(), fieldResolver);
                    if (alias != null) {
                        accept(alias);
                        return;
                    }
                }
                accept(parsePossibleDimensionAggregateMetric(ctx.jqlDocMetricAtom(), context));
            }

            @Override
            public void enterAggregateMetricFilter(JQLParser.AggregateMetricFilterContext ctx) {
                final AggregateFilter filter = AggregateFilters.parseJQLAggregateFilter(ctx.jqlAggregateFilter(), context);
                accept(new AggregateMetric.IfThenElse(filter, new AggregateMetric.Constant(1), new AggregateMetric.Constant(0)));
            }

            @Override
            public void enterSyntacticallyAtomicAggregateMetric(JQLParser.SyntacticallyAtomicAggregateMetricContext ctx) {
                accept(parseSyntacticallyAtomicJQLAggregateMetric(ctx.syntacticallyAtomicJqlAggregateMetric(), fieldResolver));
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
