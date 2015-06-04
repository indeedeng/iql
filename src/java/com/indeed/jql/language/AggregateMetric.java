package com.indeed.jql.language;

import com.google.common.base.Optional;

import java.util.List;

public interface AggregateMetric {

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
        private final Optional<Integer> windowSize;

        public Distinct(String field, Optional<AggregateFilter> filter, Optional<Integer> windowSize) {
            this.field = field;
            this.filter = filter;
            this.windowSize = windowSize;
        }
    }
}
