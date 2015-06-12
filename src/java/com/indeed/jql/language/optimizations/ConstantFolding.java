package com.indeed.jql.language.optimizations;

import com.google.common.base.Function;
import com.indeed.jql.language.DocFilter;
import com.indeed.jql.language.DocMetric;
import com.indeed.jql.language.DocMetrics;
import com.indeed.jql.language.JQLParser;
import com.indeed.jql.language.Main;
import com.indeed.util.core.Pair;

import java.util.Collections;
import java.util.Set;

public class ConstantFolding {
    public static void main(String[] args) {
        final JQLParser parser = Main.parserForString("if oji=10 then oji else 0");
        final JQLParser.DocMetricContext ctx = parser.docMetric(false);
        final DocMetric metric = DocMetrics.parseDocMetric(ctx, Collections.<String, Set<String>>emptyMap());
        System.out.println("metric = " + metric);
        System.out.println("new DocMetric.PushableDocMetric(metric).getPushes() = " + new DocMetric.PushableDocMetric(metric).getPushes("organic"));
    }

    public static DocMetric apply(final DocMetric metric) {
        return metric.transform(new Function<DocMetric, DocMetric>() {
            @Override
            public DocMetric apply(DocMetric input) {
                if (input instanceof DocMetric.Add) {
                    // Add (Constant x) (Constant y) = Constant (x + y)
                    // Add (Constant 0) x = x
                    // Add x (Constant 0) = x
                    final DocMetric.Add add = (DocMetric.Add) input;
                    final Pair<DocMetric, DocMetric> normalized = normalize(add.m1, add.m2);
                    if (isConstant(normalized.getFirst()) && isConstant(normalized.getSecond())) {
                        return new DocMetric.Constant(getConstant(normalized.getFirst()) + getConstant(normalized.getSecond()));
                    } else if (isConstant(normalized.getFirst())) {
                        final long c = getConstant(normalized.getFirst());
                        if (c == 0) {
                            return normalized.getSecond();
                        }
                    }
                } else if (input instanceof DocMetric.Multiply) {
                    // Multiply (Constant x) (Constant y) = Constant (x * y)
                    // Multiply (Constant 0) x = Constant 0
                    // Multiply x (Constant 0) = x
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
                        final DocMetric.Abs innerAbs = (DocMetric.Abs) abs.m1;
                        return innerAbs;
                    }
                } else if (input instanceof DocMetric.Field) {
                    final DocMetric.Field field = (DocMetric.Field) input;
                    if ("count()".equals(field.field)) {
                        return new DocMetric.Constant(1);
                    } else {
                        try {
                            final long v = Long.parseLong(field.field);
                            return new DocMetric.Constant(v);
                        } catch (NumberFormatException e) {
                        }
                    }
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
                    if (isConstant(log.m1)) {
                        return new DocMetric.Constant((long) Math.log(getConstant(log.m1)));
                    }
                } else if (input instanceof DocMetric.Max) {
                    final DocMetric.Max max = (DocMetric.Max) input;
                    if (isConstant(max.m1) && isConstant(max.m2)) {
                        return new DocMetric.Constant(Math.max(getConstant(max.m1), getConstant(max.m2)));
                    }
                } else if (input instanceof DocMetric.Min) {
                    final DocMetric.Min min = (DocMetric.Min) input;
                    if (isConstant(min.m1) && isConstant(min.m2)) {
                        return new DocMetric.Constant(Math.min(getConstant(min.m1), getConstant(min.m2)));
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
                } else if (input instanceof DocMetric.MetricNotEqual) {
                    final DocMetric.MetricNotEqual metricNotEqual = (DocMetric.MetricNotEqual) input;
                    if (isConstant(metricNotEqual.m1) && isConstant(metricNotEqual.m2)) {
                        if (getConstant(metricNotEqual.m1) != getConstant(metricNotEqual.m2)) {
                            return new DocMetric.Constant(1);
                        } else {
                            return new DocMetric.Constant(0);
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
        }, new Function<DocFilter, DocFilter>() {
            @Override
            public DocFilter apply(DocFilter input) {
                if (input instanceof DocFilter.And) {
                    final DocFilter.And and = (DocFilter.And) input;
                    if (isConstant(and.f1) && isConstant(and.f2)) {
                        return makeConstant(getConstant(and.f1) && getConstant(and.f2));
                    } else if (isConstant(and.f1) && !getConstant(and.f1)) {
                        return new DocFilter.Never();
                    } else if (isConstant(and.f2) && !getConstant(and.f2)) {
                        return new DocFilter.Never();
                    } else if (isConstant(and.f1) && getConstant(and.f1)) {
                        return and.f2;
                    } else if (isConstant(and.f2) && getConstant(and.f2)) {
                        return and.f1;
                    }
                } else if (input instanceof DocFilter.Or) {
                    final DocFilter.Or or = (DocFilter.Or) input;
                    if (isConstant(or.f1) && isConstant(or.f2)) {
                        return makeConstant(getConstant(or.f1) || getConstant(or.f2));
                    } else if (isConstant(or.f1) && getConstant(or.f1)) {
                        return new DocFilter.Always();
                    } else if (isConstant(or.f2) && getConstant(or.f2)) {
                        return new DocFilter.Always();
                    } else if (isConstant(or.f1) && !getConstant(or.f1)) {
                        return or.f2;
                    } else if (isConstant(or.f2) && !getConstant(or.f2)) {
                        return or.f1;
                    }
                } else if (input instanceof DocFilter.MetricEqual) {
                    final DocFilter.MetricEqual metricEqual = (DocFilter.MetricEqual) input;
                    if (isConstant(metricEqual.m1) && isConstant(metricEqual.m2)) {
                        return makeConstant(getConstant(metricEqual.m1) == getConstant(metricEqual.m2));
                    }
                } else if (input instanceof DocFilter.MetricNotEqual) {
                    final DocFilter.MetricNotEqual metricNotEqual = (DocFilter.MetricNotEqual) input;
                    if (isConstant(metricNotEqual.m1) && isConstant(metricNotEqual.m2)) {
                        return makeConstant(getConstant(metricNotEqual.m1) != getConstant(metricNotEqual.m2));
                    }
                } else if (input instanceof DocFilter.MetricGt) {
                    final DocFilter.MetricGt metricGt = (DocFilter.MetricGt) input;
                    if (isConstant(metricGt.m1) && isConstant(metricGt.m2)) {
                        return makeConstant(getConstant(metricGt.m1) > getConstant(metricGt.m2));
                    }
                } else if (input instanceof DocFilter.MetricGte) {
                    final DocFilter.MetricGte metricGte = (DocFilter.MetricGte) input;
                    if (isConstant(metricGte.m1) && isConstant(metricGte.m2)) {
                        return makeConstant(getConstant(metricGte.m1) >= getConstant(metricGte.m2));
                    }
                } else if (input instanceof DocFilter.MetricLt) {
                    final DocFilter.MetricLt metricLt = (DocFilter.MetricLt) input;
                    if (isConstant(metricLt.m1) && isConstant(metricLt.m2)) {
                        return makeConstant(getConstant(metricLt.m1) < getConstant(metricLt.m2));
                    }
                } else if (input instanceof DocFilter.MetricLte) {
                    final DocFilter.MetricLte metricLte = (DocFilter.MetricLte) input;
                    if (isConstant(metricLte.m1) && isConstant(metricLte.m2)) {
                        return makeConstant(getConstant(metricLte.m1) <= getConstant(metricLte.m2));
                    }
                } else if (input instanceof DocFilter.Not) {
                    final DocFilter.Not not = (DocFilter.Not) input;
                    if (isConstant(not.filter)) {
                        return makeConstant(!getConstant(not.filter));
                    }
                }
                return input;
            }

            private DocFilter makeConstant(boolean b) {
                if (b) {
                    return new DocFilter.Always();
                } else {
                    return new DocFilter.Never();
                }
            }
        });
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
