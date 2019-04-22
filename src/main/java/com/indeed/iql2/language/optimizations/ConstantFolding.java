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

package com.indeed.iql2.language.optimizations;

import com.google.common.collect.Iterables;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.DocMetrics;
import com.indeed.iql2.language.Term;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.util.core.Pair;

import java.util.function.Function;

public class ConstantFolding {
    private ConstantFolding() {
    }

    private static final Function<AggregateMetric, AggregateMetric> AGG_METRIC_OPTIMIZER = new Function<AggregateMetric, AggregateMetric>() {
        @Override
        public AggregateMetric apply(final AggregateMetric input) {
            if (input instanceof AggregateMetric.Negate) {
                final AggregateMetric.Negate negate = (AggregateMetric.Negate) input;
                if (isConstant(negate.m1)) {
                    return new AggregateMetric.Constant(-getConstant(negate.m1));
                }
            }

            // TODO: Fill in some more.

            return input;
        }
    };

    private static final Function<DocMetric, DocMetric> METRIC_OPTIMIZER = new Function<DocMetric, DocMetric>() {
        @Override
        public DocMetric apply(final DocMetric input) {
            if (input instanceof DocMetric.Multiply) {
                // Multiply (Constant x) (Constant y) = Constant (x * y)
                // Multiply (Constant 0) x = Constant 0
                // Multiply x (Constant 0) = Constant 0
                // Multiply (Constant 1) x = x
                // Multiply x (Constant 1) = x
                final DocMetric.Multiply multiply = (DocMetric.Multiply) input;
                final Pair<DocMetric, DocMetric> normalized = normalize(multiply.m1, multiply.m2);
                if (isConstant(normalized.getFirst()) && isConstant(normalized.getSecond())) {
                    return new DocMetric.Constant(getConstant(normalized.getFirst()) * getConstant(normalized.getSecond()));
                } else if (isConstant(normalized.getFirst())) {
                    final long c = getConstant(normalized.getFirst());
                    if (c == 0) {
                        return new DocMetric.Constant(0);
                    } else if (c == 1) {
                        return normalized.getSecond();
                    }
                }
            } else if (input instanceof DocMetric.Subtract) {
                // Subtract (Constant x) (Constant y) = Constant (x - y)
                // Subtract x (Constant 0) = x
                final DocMetric.Subtract subtract = (DocMetric.Subtract) input;
                if (isConstant(subtract.m1) && isConstant(subtract.m2)) {
                    return new DocMetric.Constant(getConstant(subtract.m1) - getConstant(subtract.m2));
                } else if (isConstant(subtract.m2)) {
                    final long c = getConstant(subtract.m2);
                    if (c == 0) {
                        return subtract.m1;
                    }
                }
            } else if (input instanceof DocMetric.Divide) {
                // Divide (Constant x) (Constant y) = Constant (x / y)
                // Divide x (Constant 0) = error "divide by zero"
                // Divide x (Constant 1) = x
                final DocMetric.Divide divide = (DocMetric.Divide) input;
                if (isConstant(divide.m1) && isConstant(divide.m2)) {
                    return new DocMetric.Constant(getConstant(divide.m1) / getConstant(divide.m2));
                } else if (isConstant(divide.m2)) {
                    final long c = getConstant(divide.m2);
                    if (c == 0) {
                        throw new IllegalArgumentException("Unconditional divide by zero!: " + input);
                    } else if (c == 1) {
                        return divide.m1;
                    }
                }
            } else if (input instanceof DocMetric.Abs) {
                // Abs (Constant x) = Constant (abs x)
                final DocMetric.Abs abs = (DocMetric.Abs) input;
                if (isConstant(abs.m1)) {
                    return new DocMetric.Constant(Math.abs(getConstant(abs.m1)));
                } else if (abs.m1 instanceof DocMetric.Abs) {
                    return abs.m1;
                }
            } else if (input instanceof DocMetric.Count) {
                return new DocMetric.Constant(1);
            } else if (input instanceof DocMetric.IfThenElse) {
                final DocMetric.IfThenElse ifThenElse = (DocMetric.IfThenElse) input;
                if (isConstant(ifThenElse.condition)) {
                    final boolean isTrue = getConstant(ifThenElse.condition);
                    if (isTrue) {
                        return ifThenElse.trueCase;
                    } else {
                        return ifThenElse.falseCase;
                    }
                } else if (ifThenElse.trueCase.equals(ifThenElse.falseCase)) {
                    return ifThenElse.trueCase;
                }
            } else if (input instanceof DocMetric.Log) {
                final DocMetric.Log log = (DocMetric.Log) input;
                if (isConstant(log.metric)) {
                    return new DocMetric.Constant((long) ((Math.log(getConstant(log.metric)) - Math.log(log.scaleFactor)) * log.scaleFactor));
                }
            } else if (input instanceof DocMetric.Exponentiate) {
                final DocMetric.Exponentiate exp = (DocMetric.Exponentiate) input;
                if (isConstant(exp.metric)) {
                    double x = getConstant(exp.metric) / (double) exp.scaleFactor;
                    double result = Math.exp(x);
                    return new DocMetric.Constant((long) (result * exp.scaleFactor));
                }
            } else if (input instanceof DocMetric.MetricEqual) {
                final DocMetric.MetricEqual metricEqual = (DocMetric.MetricEqual) input;
                if (isConstant(metricEqual.m1) && isConstant(metricEqual.m2)) {
                    if (getConstant(metricEqual.m1) == getConstant(metricEqual.m2)) {
                        return new DocMetric.Constant(1);
                    } else {
                        return new DocMetric.Constant(0);
                    }
                }
                final Pair<DocMetric, DocMetric> normalized = normalize(metricEqual.m1, metricEqual.m2);
                if (isConstant(normalized.getFirst()) &&  (normalized.getSecond() instanceof DocMetric.Field)) {
                    return new DocMetric.HasInt(((DocMetric.Field) normalized.getSecond()).field, getConstant(normalized.getFirst()));
                }
                if (isConstant(normalized.getFirst()) && (normalized.getSecond() instanceof DocMetric.ZeroOneMetric)) {
                    final long value = getConstant(normalized.getFirst());
                    if (value == 1) {
                        return normalized.getSecond();
                    } else if (value == 0) {
                        final DocMetric inverted = ((DocMetric.ZeroOneMetric) normalized.getSecond()).invert();
                        if (inverted != null) {
                            return inverted;
                        }
                    } else {
                        // ZeroOneMetric is never equal to value if it's not 0 or 1
                        return new DocMetric.Constant(0);
                    }
                }
            } else if (input instanceof DocMetric.MetricNotEqual) {
                final DocMetric.MetricNotEqual metricNotEqual = (DocMetric.MetricNotEqual) input;
                if (isConstant(metricNotEqual.m1) && isConstant(metricNotEqual.m2)) {
                    if (getConstant(metricNotEqual.m1) != getConstant(metricNotEqual.m2)) {
                        return new DocMetric.Constant(1);
                    } else {
                        return new DocMetric.Constant(0);
                    }
                }
                final Pair<DocMetric, DocMetric> normalized = normalize(metricNotEqual.m1, metricNotEqual.m2);
                if (isConstant(normalized.getFirst()) && (normalized.getSecond() instanceof DocMetric.Field)) {
                    // field != constant can be implemented in two ways
                    // 1. pushStat("field', "constant", "!=")
                    // 2. pushStat("1", "hasInt(field, constant)", "-")
                    // Second seems to be better because will use less memory and could require only one term un-inversion
                    return DocMetrics.negateMetric(
                            new DocMetric.HasInt(((DocMetric.Field) normalized.getSecond()).field,
                                    getConstant(normalized.getFirst())));
                }
                if (isConstant(normalized.getFirst()) && (normalized.getSecond() instanceof DocMetric.ZeroOneMetric)) {
                    final long value = getConstant(normalized.getFirst());
                    if (value == 0) {
                        return normalized.getSecond();
                    } else if (value == 1) {
                        final DocMetric inverted = ((DocMetric.ZeroOneMetric) normalized.getSecond()).invert();
                        if (inverted != null) {
                            return inverted;
                        }
                    } else {
                        // ZeroOneMetric is always not equal to value if it's not 0 or 1
                        return new DocMetric.Constant(1);
                    }
                }
            } else if (input instanceof DocMetric.MetricLt) {
                final DocMetric.MetricLt metricLt = (DocMetric.MetricLt) input;
                if (isConstant(metricLt.m1) && isConstant(metricLt.m2)) {
                    if (getConstant(metricLt.m1) < getConstant(metricLt.m2)) {
                        return new DocMetric.Constant(1);
                    } else {
                        return new DocMetric.Constant(0);
                    }
                }
            } else if (input instanceof DocMetric.MetricLte) {
                final DocMetric.MetricLte metricLte = (DocMetric.MetricLte) input;
                if (isConstant(metricLte.m1) && isConstant(metricLte.m2)) {
                    if (getConstant(metricLte.m1) <= getConstant(metricLte.m2)) {
                        return new DocMetric.Constant(1);
                    } else {
                        return new DocMetric.Constant(0);
                    }
                }
            } else if (input instanceof DocMetric.MetricGt) {
                final DocMetric.MetricGt metricGt = (DocMetric.MetricGt) input;
                if (isConstant(metricGt.m1) && isConstant(metricGt.m2)) {
                    if (getConstant(metricGt.m1) > getConstant(metricGt.m2)) {
                        return new DocMetric.Constant(1);
                    } else {
                        return new DocMetric.Constant(0);
                    }
                }
            } else if (input instanceof DocMetric.MetricGte) {
                final DocMetric.MetricGte metricGte = (DocMetric.MetricGte) input;
                if (isConstant(metricGte.m1) && isConstant(metricGte.m2)) {
                    if (getConstant(metricGte.m1) >= getConstant(metricGte.m2)) {
                        return new DocMetric.Constant(1);
                    } else {
                        return new DocMetric.Constant(0);
                    }
                }
            } else if (input instanceof DocMetric.Modulus) {
                final DocMetric.Modulus modulus = (DocMetric.Modulus) input;
                if (isConstant(modulus.m1) && isConstant(modulus.m2)) {
                    return new DocMetric.Constant(getConstant(modulus.m1) % getConstant(modulus.m2));
                }
            } else if (input instanceof DocMetric.Negate) {
                final DocMetric.Negate negate = (DocMetric.Negate) input;
                if (isConstant(negate.m1)) {
                    return new DocMetric.Constant(-getConstant(negate.m1));
                }
            } else if (input instanceof DocMetric.Signum) {
                final DocMetric.Signum signum = (DocMetric.Signum) input;
                if (isConstant(signum.m1)) {
                    return new DocMetric.Constant(Math.round(Math.signum(getConstant(signum.m1))));
                }
            }
            return input;
        }
    };
    private static final Function<DocFilter, DocFilter> FILTER_OPTIMIZER = new Function<DocFilter, DocFilter>() {
        @Override
        public DocFilter apply(final DocFilter input) {
            if (input instanceof DocFilter.MetricEqual) {
                final DocFilter.MetricEqual metricEqual = (DocFilter.MetricEqual) input;
                if (isConstant(metricEqual.m1) && isConstant(metricEqual.m2)) {
                    return makeConstant(getConstant(metricEqual.m1) == getConstant(metricEqual.m2));
                }
                final Pair<DocMetric, DocMetric> normalized = normalize(metricEqual.m1, metricEqual.m2);
                if (isConstant(normalized.getFirst()) && (normalized.getSecond() instanceof DocMetric.ZeroOneMetric)) {
                    final long value = getConstant(normalized.getFirst());
                    // result of binary operation is compared to constant
                    // unwrapping operation
                    if (value == 1) {
                        return ((DocMetric.ZeroOneMetric) normalized.getSecond()).convertToFilter();
                    } else if (value == 0) {
                        DocFilter result = ((DocMetric.ZeroOneMetric) normalized.getSecond()).convertToFilter();
                        result = new DocFilter.Not(result);
                        // applying this method once again to optimize Not.
                        return FILTER_OPTIMIZER.apply(result);
                    } else {
                        return new DocFilter.Never();
                    }
                }
                if (isConstant(normalized.getFirst()) && (normalized.getSecond() instanceof DocMetric.Field)) {
                    final FieldSet field = ((DocMetric.Field) normalized.getSecond()).field;
                    return DocFilter.FieldIs.create(field, Term.term(getConstant(normalized.getFirst())));
                }
            } else if (input instanceof DocFilter.MetricNotEqual) {
                final DocFilter.MetricNotEqual metricNotEqual = (DocFilter.MetricNotEqual) input;
                if (isConstant(metricNotEqual.m1) && isConstant(metricNotEqual.m2)) {
                    return makeConstant(getConstant(metricNotEqual.m1) != getConstant(metricNotEqual.m2));
                }
                final Pair<DocMetric, DocMetric> normalized = normalize(metricNotEqual.m1, metricNotEqual.m2);
                if (isConstant(normalized.getFirst()) && (normalized.getSecond() instanceof DocMetric.ZeroOneMetric)) {
                    final long value = getConstant(normalized.getFirst());
                    // result of binary operation is compared to constant
                    // unwrapping operation
                    if (value == 0) {
                        return ((DocMetric.ZeroOneMetric) normalized.getSecond()).convertToFilter();
                    } else if (value == 1) {
                        DocFilter result = ((DocMetric.ZeroOneMetric) normalized.getSecond()).convertToFilter();
                        result = new DocFilter.Not(result);
                        // applying this method once again to optimize Not.
                        return FILTER_OPTIMIZER.apply(result);
                    } else {
                        return new DocFilter.Always();
                    }
                }
                if (isConstant(normalized.getFirst()) && (normalized.getSecond() instanceof DocMetric.Field)) {
                    final FieldSet field = ((DocMetric.Field) normalized.getSecond()).field;
                    return DocFilter.FieldIsnt.create(field, Term.term(getConstant(normalized.getFirst())));
                }
            } else if (input instanceof DocFilter.MetricGt) {
                final DocFilter.MetricGt metricGt = (DocFilter.MetricGt) input;
                if (isConstant(metricGt.m1) && isConstant(metricGt.m2)) {
                    return makeConstant(getConstant(metricGt.m1) > getConstant(metricGt.m2));
                }
                if (isConstant(metricGt.m2) && (getConstant(metricGt.m2) == Long.MAX_VALUE)) {
                    return new DocFilter.Never(); // always false
                }
            } else if (input instanceof DocFilter.MetricGte) {
                final DocFilter.MetricGte metricGte = (DocFilter.MetricGte) input;
                if (isConstant(metricGte.m1) && isConstant(metricGte.m2)) {
                    return makeConstant(getConstant(metricGte.m1) >= getConstant(metricGte.m2));
                }
                if (isConstant(metricGte.m2) && (getConstant(metricGte.m2) == Long.MIN_VALUE)) {
                    return new DocFilter.Always(); // always true
                }
            } else if (input instanceof DocFilter.MetricLt) {
                final DocFilter.MetricLt metricLt = (DocFilter.MetricLt) input;
                if (isConstant(metricLt.m1) && isConstant(metricLt.m2)) {
                    return makeConstant(getConstant(metricLt.m1) < getConstant(metricLt.m2));
                }
                if (isConstant(metricLt.m2) && (getConstant(metricLt.m2) == Long.MIN_VALUE)) {
                    return new DocFilter.Never(); // always false
                }
            } else if (input instanceof DocFilter.MetricLte) {
                final DocFilter.MetricLte metricLte = (DocFilter.MetricLte) input;
                if (isConstant(metricLte.m1) && isConstant(metricLte.m2)) {
                    return makeConstant(getConstant(metricLte.m1) <= getConstant(metricLte.m2));
                }
                if (isConstant(metricLte.m2) && (getConstant(metricLte.m2) == Long.MAX_VALUE)) {
                    return new DocFilter.Always(); // always true;
                }
            } else if (input instanceof DocFilter.FieldInTermsSet) {
                final DocFilter.FieldInTermsSet fieldIn = (DocFilter.FieldInTermsSet)input;
                if (fieldIn.terms.size() == 1) {
                    return DocFilter.FieldIs.create(fieldIn.field, Iterables.getOnlyElement(fieldIn.terms));
                }
            } else if (input instanceof DocFilter.Not) {
                final DocFilter.Not not = (DocFilter.Not) input;
                if (isConstant(not.filter)) {
                    return makeConstant(!getConstant(not.filter));
                } else if (not.filter instanceof DocFilter.MetricEqual) {
                    final DocFilter.MetricEqual filter = (DocFilter.MetricEqual) not.filter;
                    return new DocFilter.MetricNotEqual(filter.m1, filter.m2);
                } else if (not.filter instanceof DocFilter.MetricNotEqual) {
                    final DocFilter.MetricNotEqual filter = (DocFilter.MetricNotEqual) not.filter;
                    return new DocFilter.MetricEqual(filter.m1, filter.m2);
                } else if (not.filter instanceof DocFilter.MetricLt) {
                    final DocFilter.MetricLt filter = (DocFilter.MetricLt)not.filter;
                    return new DocFilter.MetricGte(filter.m1, filter.m2);
                } else if (not.filter instanceof DocFilter.MetricLte) {
                    final DocFilter.MetricLte filter = (DocFilter.MetricLte)not.filter;
                    return new DocFilter.MetricGt(filter.m1, filter.m2);
                } else if (not.filter instanceof DocFilter.MetricGt) {
                    final DocFilter.MetricGt filter = (DocFilter.MetricGt)not.filter;
                    return new DocFilter.MetricLte(filter.m1, filter.m2);
                } else if (not.filter instanceof DocFilter.MetricGte) {
                    final DocFilter.MetricGte filter = (DocFilter.MetricGte)not.filter;
                    return new DocFilter.MetricLt(filter.m1, filter.m2);
                } else if (not.filter instanceof DocFilter.Not) {
                    final DocFilter.Not secondNot = (DocFilter.Not)not.filter;
                    return secondNot.filter;
                } else if (not.filter instanceof DocFilter.Regex) {
                    final DocFilter.Regex regex = (DocFilter.Regex)not.filter;
                    return new DocFilter.NotRegex(regex.field, regex.regex);
                } else if (not.filter instanceof DocFilter.NotRegex) {
                    final DocFilter.NotRegex notRegex = (DocFilter.NotRegex) not.filter;
                    return new DocFilter.Regex(notRegex.field, notRegex.regex);
                } else if (not.filter instanceof DocFilter.FieldIs) {
                    final DocFilter.FieldIs fieldIs = (DocFilter.FieldIs)not.filter;
                    return DocFilter.FieldIsnt.create(fieldIs.field, fieldIs.term);
                } else if (not.filter instanceof DocFilter.FieldIsnt) {
                    final DocFilter.FieldIsnt fieldIsnt = (DocFilter.FieldIsnt) not.filter;
                    return DocFilter.FieldIs.create(fieldIsnt.field, fieldIsnt.term);
                }
            } else if (input instanceof DocFilter.Between) {
                final DocFilter.Between between = (DocFilter.Between)input;
                // Checking if it's possible to delete one (or both) condition(s).
                final boolean canDeleteLower = between.lowerBound == Long.MIN_VALUE;
                final boolean canDeleteUpper = between.isUpperInclusive && (between.upperBound == Long.MAX_VALUE);
                if (canDeleteLower && canDeleteUpper) {
                    return new DocFilter.Always();
                } else if (canDeleteLower) {
                    final DocFilter upperCondition = between.isUpperInclusive ?
                            new DocFilter.MetricLte(between.metric, new DocMetric.Constant(between.upperBound)) :
                            new DocFilter.MetricLt(between.metric, new DocMetric.Constant(between.upperBound));
                    return FILTER_OPTIMIZER.apply(upperCondition);
                } else if (canDeleteUpper) {
                    final DocFilter lowerCondition = new DocFilter.MetricGte(
                            between.metric, new DocMetric.Constant(between.lowerBound));
                    return FILTER_OPTIMIZER.apply(lowerCondition);
                }

                // check if values range is just one value
                final long upperInclusive;
                if (between.isUpperInclusive) {
                    upperInclusive = between.upperBound;
                } else {
                    if (between.upperBound == Long.MIN_VALUE) {
                        // How does it happen? Maybe it's an error.
                        return new DocFilter.Never();
                    }
                    upperInclusive = between.upperBound - 1;
                }
                if (between.lowerBound == upperInclusive) {
                    return FILTER_OPTIMIZER.apply(new DocFilter.MetricEqual(between.metric, new DocMetric.Constant(between.lowerBound)));
                }
            }
            return input;
        }

        private DocFilter makeConstant(final boolean b) {
            if (b) {
                return new DocFilter.Always();
            } else {
                return new DocFilter.Never();
            }
        }
    };

    public static Query apply(final Query query) {
        return query.transform(Function.identity(), AGG_METRIC_OPTIMIZER, METRIC_OPTIMIZER, Function.identity(), FILTER_OPTIMIZER);
    }

    public static AggregateMetric apply(final AggregateMetric metric) {
        return metric.transform(AGG_METRIC_OPTIMIZER, METRIC_OPTIMIZER, Function.identity(), FILTER_OPTIMIZER, Function.identity());
    }

    public static DocMetric apply(final DocMetric metric) {
        return metric.transform(METRIC_OPTIMIZER, FILTER_OPTIMIZER);
    }

    public static DocFilter apply(final DocFilter filter) {
        return filter.transform(METRIC_OPTIMIZER, FILTER_OPTIMIZER);
    }

    private static Pair<DocMetric, DocMetric> normalize(DocMetric x, DocMetric y) {
        if (isConstant(x)) {
            return Pair.of(x, y);
        } else if (isConstant(y)) {
            return Pair.of(y, x);
        } else {
            return Pair.of(x, y);
        }
    }

    private static double getConstant(AggregateMetric constant) {
        return ((AggregateMetric.Constant) constant).value;
    }

    private static boolean isConstant(AggregateMetric value) {
        return value instanceof AggregateMetric.Constant;
    }

    private static long getConstant(DocMetric constant) {
        return ((DocMetric.Constant) constant).value;
    }

    private static boolean isConstant(DocMetric value) {
        return value instanceof DocMetric.Constant;
    }

    private static boolean getConstant(DocFilter value) {
        if (value instanceof DocFilter.Always) {
            return true;
        } else if (value instanceof DocFilter.Never) {
            return false;
        }
        throw new IllegalArgumentException("Called getConstant on a non-constant: [" + value + "]");
    }

    private static boolean isConstant(DocFilter value) {
        return value instanceof DocFilter.Always || value instanceof DocFilter.Never;
    }
}
