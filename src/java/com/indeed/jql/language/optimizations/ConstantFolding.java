package com.indeed.jql.language.optimizations;

import com.google.common.base.Function;
import com.indeed.jql.language.DocFilter;
import com.indeed.jql.language.DocMetric;
import com.indeed.jql.language.DocMetrics;
import com.indeed.jql.language.JQLParser;
import com.indeed.jql.language.Main;
import com.indeed.util.core.Pair;

public class ConstantFolding {
    public static void main(String[] args) {
        final JQLParser parser = Main.parserForString("1 + 1 + 1 * 5 / 1 + 8 / 4");
        final JQLParser.DocMetricContext ctx = parser.docMetric(false);
        final DocMetric metric = DocMetrics.parseDocMetric(ctx);
        System.out.println("metric = " + metric);
        final DocMetric metric2 = ConstantFolding.apply(metric);
        System.out.println("metric2 = " + metric2);
    }

    public static DocMetric apply(DocMetric metric) {
        return metric.transform(new Function<DocMetric, DocMetric>() {
            @Override
            public DocMetric apply(DocMetric input) {
                if (input instanceof DocMetric.Add) {
                    final DocMetric.Add add = (DocMetric.Add) input;
                    final Pair<DocMetric, DocMetric> normalized = normalize(add.m1, add.m2);
                    if (isConstant(normalized.getFirst()) && isConstant(normalized.getSecond())) {
                        return new DocMetric.Constant(getConstant(normalized.getFirst()) + getConstant(normalized.getSecond()));
                    } else if (isConstant(normalized.getFirst())) {
                        final long c = getConstant(normalized.getFirst());
                        if (c == 0) {
                            return normalized.getSecond();
                        } else {
                            return input;
                        }
                    }
                } else if (input instanceof DocMetric.Multiply) {
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
                        } else {
                            return input;
                        }
                    }
                } else if (input instanceof DocMetric.Subtract) {
                    final DocMetric.Subtract subtract = (DocMetric.Subtract) input;
                    if (isConstant(subtract.m1) && isConstant(subtract.m2)) {
                        return new DocMetric.Constant(getConstant(subtract.m1) - getConstant(subtract.m2));
                    } else if (isConstant(subtract.m2)) {
                        final long c = getConstant(subtract.m2);
                        if (c == 0) {
                            return subtract.m1;
                        } else {
                            return input;
                        }
                    }
                } else if (input instanceof DocMetric.Divide) {
                    final DocMetric.Divide divide = (DocMetric.Divide) input;
                    if (isConstant(divide.m1) && isConstant(divide.m2)) {
                        return new DocMetric.Constant(getConstant(divide.m1) / getConstant(divide.m2));
                    } else if (isConstant(divide.m2)) {
                        final long c = getConstant(divide.m2);
                        if (c == 0) {
                            throw new IllegalArgumentException("Unconditional divide by zero!: " + input);
                        } else if (c == 1) {
                            return divide.m1;
                        } else {
                            return input;
                        }
                    }
                }
                return input;
            }
        }, new Function<DocFilter, DocFilter>() {
            @Override
            public DocFilter apply(DocFilter input) {
                return input;
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
}
