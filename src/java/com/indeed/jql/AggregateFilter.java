package com.indeed.jql;

import org.antlr.v4.runtime.misc.NotNull;

import java.util.List;
import java.util.function.BiFunction;

public interface AggregateFilter {
    static AggregateFilter aggregateInHelper(List<JQLParser.TermValContext> terms, boolean negate) {
        AggregateFilter filter = null;
        for (final JQLParser.TermValContext term : terms) {
            if (filter == null) {
                filter = new TermIs(Main.parseTerm(term));
            } else {
                filter = new Or(new TermIs(Main.parseTerm(term)), filter);
            }
        }
        if (filter == null) {
            // TODO (optional): Make this new Always() and don't negate if (ctx.not != null).
            filter = new Never();
        }
        if (negate) {
            filter = new Not(filter);
        }
        return filter;
    }

    static AggregateFilter parseAggregateFilter(JQLParser.AggregateFilterContext aggregateFilterContext) {
        final AggregateFilter[] ref = new AggregateFilter[1];

        aggregateFilterContext.enterRule(new JQLBaseListener() {
            private void accept(AggregateFilter value) {
                if (ref[0] != null) {
                    throw new IllegalArgumentException("Can't accept multiple times!");
                }
                ref[0] = value;
            }

            public void enterAggregateRegex(@NotNull JQLParser.AggregateRegexContext ctx) {
                accept(new Regex(ctx.field.getText(), Main.unquote(ctx.STRING_LITERAL().getText())));
            }

            public void enterAggregateFalse(@NotNull JQLParser.AggregateFalseContext ctx) {
                accept(new Never());
            }

            public void enterAggregateTermIs(@NotNull JQLParser.AggregateTermIsContext ctx) {
                accept(new TermIs(Main.parseTerm(ctx.termVal())));
            }

            public void enterAggregateNotRegex(@NotNull JQLParser.AggregateNotRegexContext ctx) {
                accept(new Not(new Regex(ctx.field.getText(), Main.unquote(ctx.STRING_LITERAL().getText()))));
            }

            public void enterAggregateTrue(@NotNull JQLParser.AggregateTrueContext ctx) {
                accept(new Always());
            }

            public void enterAggregateFilterParens(@NotNull JQLParser.AggregateFilterParensContext ctx) {
                accept(parseAggregateFilter(ctx.aggregateFilter()));
            }

            public void enterAggregateAnd(@NotNull JQLParser.AggregateAndContext ctx) {
                accept(new And(parseAggregateFilter(ctx.aggregateFilter(0)), parseAggregateFilter(ctx.aggregateFilter(1))));
            }

            public void enterAggregateMetricInequality(@NotNull JQLParser.AggregateMetricInequalityContext ctx) {
                BiFunction<AggregateMetric, AggregateMetric, AggregateFilter> f;
                final String operation = ctx.op.getText();
                switch (operation) {
                    case "=": {
                        f = AggregateFilter.MetricIs::new;
                        break;
                    }
                    case "!=": {
                        f = AggregateFilter.MetricIsnt::new;
                        break;
                    }
                    case "<": {
                        f = AggregateFilter.Lt::new;
                        break;
                    }
                    case "<=": {
                        f = AggregateFilter.Lte::new;
                        break;
                    }
                    case ">": {
                        f = AggregateFilter.Gt::new;
                        break;
                    }
                    case ">=": {
                        f = AggregateFilter.Gte::new;
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Unhandled inequality operation: [" + operation + "]");
                }
                accept(f.apply(AggregateMetric.parseAggregateMetric(ctx.aggregateMetric(0)), AggregateMetric.parseAggregateMetric(ctx.aggregateMetric(1))));
            }

            public void enterAggregateNot(@NotNull JQLParser.AggregateNotContext ctx) {
                accept(new Not(parseAggregateFilter(ctx.aggregateFilter())));
            }

            public void enterAggregateOr(@NotNull JQLParser.AggregateOrContext ctx) {
                accept(new Or(parseAggregateFilter(ctx.aggregateFilter(0)), parseAggregateFilter(ctx.aggregateFilter(1))));
            }
        });

        if (ref[0] == null) {
            throw new UnsupportedOperationException("Unhandled aggregate filter: [" + aggregateFilterContext.getText() + "]");
        }

        return ref[0];
    }

    class TermIs implements AggregateFilter {
        private final Term term;

        public TermIs(Term term) {
            this.term = term;
        }
    }

    class MetricIs implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public MetricIs(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class MetricIsnt implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public MetricIsnt(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class Gt implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public Gt(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class Gte implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public Gte(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class Lt implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public Lt(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class Lte implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public Lte(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class And implements AggregateFilter {
        private final AggregateFilter f1;
        private final AggregateFilter f2;

        public And(AggregateFilter f1, AggregateFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }
    }

    class Or implements AggregateFilter {
        private final AggregateFilter f1;
        private final AggregateFilter f2;

        public Or(AggregateFilter f1, AggregateFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }
    }

    class Not implements AggregateFilter {
        private final AggregateFilter filter;

        public Not(AggregateFilter filter) {
            this.filter = filter;
        }
    }

    class Regex implements AggregateFilter {
        private final String field;
        private final String regex;

        public Regex(String field, String regex) {
            this.field = field;
            this.regex = regex;
        }
    }

    class Always implements AggregateFilter {}
    class Never implements AggregateFilter {}
}
