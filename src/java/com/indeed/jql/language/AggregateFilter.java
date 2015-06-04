package com.indeed.jql.language;

public interface AggregateFilter {

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
