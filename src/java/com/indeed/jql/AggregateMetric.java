package com.indeed.jql;

import org.antlr.v4.runtime.misc.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

public interface AggregateMetric {
    static AggregateMetric parseAggregateMetric(JQLParser.AggregateMetricContext metricContext) {
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
                accept(new Parent(parseAggregateMetric(ctx.aggregateMetric())));
            }

            public void enterAggregateRawField(@NotNull JQLParser.AggregateRawFieldContext ctx) {
                accept(new DocStats(new DocMetric.Field(ctx.identifier().getText())));
            }

            public void enterAggregateLag(@NotNull JQLParser.AggregateLagContext ctx) {
                accept(new Lag(Integer.parseInt(ctx.INT().getText()), parseAggregateMetric(ctx.aggregateMetric())));
            }

            public void enterAggregateAvg(@NotNull JQLParser.AggregateAvgContext ctx) {
                accept(new Divide(parseAggregateMetric(ctx.aggregateMetric()), new DocStats(new DocMetric.Field("count()"))));
            }

            public void enterAggregateQualified(@NotNull JQLParser.AggregateQualifiedContext ctx) {
                final List<String> scope = new ArrayList<>();
                for (final JQLParser.IdentifierContext dataset : ctx.scope().datasets) {
                    scope.add(dataset.getText());
                }
                accept(new Qualified(scope, parseAggregateMetric(ctx.aggregateMetric())));
            }

            public void enterAggregateDiv(@NotNull JQLParser.AggregateDivContext ctx) {
                accept(new Divide(parseAggregateMetric(ctx.aggregateMetric(0)), parseAggregateMetric(ctx.aggregateMetric(1))));
            }

            public void enterAggregateLog(@NotNull JQLParser.AggregateLogContext ctx) {
                accept(new Log(parseAggregateMetric(ctx.aggregateMetric())));
            }

            public void enterAggregateStandardDeviation(@NotNull JQLParser.AggregateStandardDeviationContext ctx) {
                accept(new Power(variance(DocMetric.parseDocMetric(ctx.docMetric())), new Constant(0.5)));
            }

            public void enterAggregatePower(@NotNull JQLParser.AggregatePowerContext ctx) {
                accept(new Power(parseAggregateMetric(ctx.aggregateMetric(0)), parseAggregateMetric(ctx.aggregateMetric(1))));
            }

            public void enterAggregateMod(@NotNull JQLParser.AggregateModContext ctx) {
                accept(new Modulus(parseAggregateMetric(ctx.aggregateMetric(0)), parseAggregateMetric(ctx.aggregateMetric(1))));
            }

            public void enterAggregatePDiff(@NotNull JQLParser.AggregatePDiffContext ctx) {
                final AggregateMetric actual = parseAggregateMetric(ctx.actual);
                final AggregateMetric expected = parseAggregateMetric(ctx.expected);
                // 100 * (actual - expected) / expected
                accept(new Multiply(new Constant(100), new Divide(new Subtract(actual, expected), expected)));
            }

            public void enterAggregateSum(@NotNull JQLParser.AggregateSumContext ctx) {
                accept(new DocStats(DocMetric.parseDocMetric(ctx.docMetric())));
            }

            public void enterAggregateMinus(@NotNull JQLParser.AggregateMinusContext ctx) {
                accept(new Subtract(parseAggregateMetric(ctx.aggregateMetric(0)), parseAggregateMetric(ctx.aggregateMetric(1))));
            }

            public void enterAggregateMult(@NotNull JQLParser.AggregateMultContext ctx) {
                accept(new Multiply(parseAggregateMetric(ctx.aggregateMetric(0)), parseAggregateMetric(ctx.aggregateMetric(1))));
            }

            public void enterAggregateConstant(@NotNull JQLParser.AggregateConstantContext ctx) {
                accept(new Constant(Double.parseDouble(ctx.number().getText())));
            }

            public void enterAggregateVariance(@NotNull JQLParser.AggregateVarianceContext ctx) {
                accept(variance(DocMetric.parseDocMetric(ctx.docMetric())));
            }

            public void enterAggregateAbs(@NotNull JQLParser.AggregateAbsContext ctx) {
                accept(new Abs(parseAggregateMetric(ctx.aggregateMetric())));
            }

            public void enterAggregateWindow(@NotNull JQLParser.AggregateWindowContext ctx) {
                accept(new Window(Integer.parseInt(ctx.INT().getText()), parseAggregateMetric(ctx.aggregateMetric())));
            }

            public void enterAggregatePlus(@NotNull JQLParser.AggregatePlusContext ctx) {
                accept(new Add(parseAggregateMetric(ctx.aggregateMetric(0)), parseAggregateMetric(ctx.aggregateMetric(1))));
            }

            public void enterAggregateDistinctWindow(@NotNull JQLParser.AggregateDistinctWindowContext ctx) {
                final Optional<AggregateFilter> filter;
                if (ctx.aggregateFilter() == null) {
                    filter = Optional.empty();
                } else {
                    filter = Optional.of(AggregateFilter.parseAggregateFilter(ctx.aggregateFilter()));
                }
                accept(new Distinct(ctx.identifier().getText(), filter, OptionalInt.of(Integer.parseInt(ctx.INT().getText()))));
            }

            public void enterAggregateNegate(@NotNull JQLParser.AggregateNegateContext ctx) {
                accept(new Negate(parseAggregateMetric(ctx.aggregateMetric())));
            }

            public void enterAggregatePercentile(@NotNull JQLParser.AggregatePercentileContext ctx) {
                accept(new Percentile(ctx.identifier().getText(), Double.parseDouble(ctx.DOUBLE().getText())));
            }

            public void enterAggregateRunning(@NotNull JQLParser.AggregateRunningContext ctx) {
                accept(new Running(parseAggregateMetric(ctx.aggregateMetric())));
            }

            public void enterAggregateCounts(@NotNull JQLParser.AggregateCountsContext ctx) {
                accept(new DocStats(new DocMetric.Field("count()")));
            }

            public void enterAggregateDistinct(@NotNull JQLParser.AggregateDistinctContext ctx) {
                final Optional<AggregateFilter> filter;
                if (ctx.aggregateFilter() == null) {
                    filter = Optional.empty();
                } else {
                    filter = Optional.of(AggregateFilter.parseAggregateFilter(ctx.aggregateFilter()));
                }
                accept(new Distinct(ctx.identifier().getText(), filter, OptionalInt.empty()));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled aggregate metric: [" + metricContext.getText() + "]");
        }

        return ref[0];
    }

    static AggregateMetric variance(DocMetric docMetric) {
        // [m * m] / count()
        final AggregateMetric firstHalf = new Divide(new DocStats(new DocMetric.Multiply(docMetric, docMetric)), new DocStats(new DocMetric.Field("count()")));
        // [m] / count()
        final AggregateMetric halfOfSecondHalf = new Divide(new DocStats(docMetric), new DocStats(new DocMetric.Field("count()")));
        // ([m] / count()) ^ 2
        final AggregateMetric secondHalf = new Multiply(halfOfSecondHalf, halfOfSecondHalf);
        // E(m^2) - E(m)^2
        return new Subtract(firstHalf, secondHalf);
    }

    class Unop implements AggregateMetric {
        private final AggregateMetric m1;

        public Unop(AggregateMetric m1) {
            this.m1 = m1;
        }
    }

    class Log extends Unop {
        public Log(AggregateMetric m1) {
            super(m1);
        }
    }

    class Negate extends Unop {
        public Negate(AggregateMetric m1) {
            super(m1);
        }
    }

    class Abs extends Unop {
        public Abs(AggregateMetric m1) {
            super(m1);
        }
    }

    class Binop implements AggregateMetric {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public Binop(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class Add extends Binop {
        public Add(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2);
        }
    }

    class Subtract extends Binop {
        public Subtract(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2);
        }
    }

    class Multiply extends Binop {
        public Multiply(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2);
        }
    }

    class Divide extends Binop {
        public Divide(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2);
        }
    }

    class Modulus extends Binop {
        public Modulus(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2);
        }
    }

    class Power extends Binop {
        public Power(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2);
        }
    }

    class Parent implements AggregateMetric {
        private final AggregateMetric metric;

        public Parent(AggregateMetric metric) {
            this.metric = metric;
        }
    }

    class Lag implements AggregateMetric {
        private final int lag;
        private final AggregateMetric metric;

        public Lag(int lag, AggregateMetric metric) {
            this.lag = lag;
            this.metric = metric;
        }
    }

    class Window implements AggregateMetric {
        private final int window;
        private final AggregateMetric metric;

        public Window(int window, AggregateMetric metric) {
            this.window = window;
            this.metric = metric;
        }
    }

    class Qualified implements AggregateMetric {
        private final List<String> scope;
        private final AggregateMetric metric;

        public Qualified(List<String> scope, AggregateMetric metric) {
            this.scope = scope;
            this.metric = metric;
        }
    }

    class DocStats implements AggregateMetric {
        private final DocMetric metric;

        public DocStats(DocMetric metric) {
            this.metric = metric;
        }
    }

    class Constant implements AggregateMetric {
        private final double value;

        public Constant(double value) {
            this.value = value;
        }
    }

    class Percentile implements AggregateMetric {
        private final String field;
        private final double percentile;

        public Percentile(String field, double percentile) {
            this.field = field;
            this.percentile = percentile;
        }
    }

    class Running implements AggregateMetric {
        private final AggregateMetric metric;

        public Running(AggregateMetric metric) {
            this.metric = metric;
        }
    }

    class Distinct implements AggregateMetric {
        private final String field;
        private final Optional<AggregateFilter> filter;
        private final OptionalInt windowSize;

        public Distinct(String field, Optional<AggregateFilter> filter, OptionalInt windowSize) {
            this.field = field;
            this.filter = filter;
            this.windowSize = windowSize;
        }
    }
}
