package com.indeed.jql.language;

import com.google.common.base.Function;

public interface AggregateFilter {

    AggregateFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i);

    class TermIs implements AggregateFilter {
        private final Term term;

        public TermIs(Term term) {
            this.term = term;
        }

        @Override
        public AggregateFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(this);
        }
    }

    class MetricIs implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public MetricIs(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public AggregateFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(new MetricIs(m1.traverse(f,g,h,i), m2.traverse(f,g,h,i)));
        }
    }

    class MetricIsnt implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public MetricIsnt(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public AggregateFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(new MetricIsnt(m1.traverse(f,g,h,i), m2.traverse(f,g,h,i)));
        }
    }

    class Gt implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public Gt(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public AggregateFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(new Gt(m1.traverse(f,g,h,i), m2.traverse(f,g,h,i)));
        }
    }

    class Gte implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public Gte(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public AggregateFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(new Gte(m1.traverse(f,g,h,i), m2.traverse(f,g,h,i)));
        }
    }

    class Lt implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public Lt(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public AggregateFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(new Lt(m1.traverse(f,g,h,i), m2.traverse(f,g,h,i)));
        }
    }

    class Lte implements AggregateFilter {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public Lte(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public AggregateFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(new Lte(m1.traverse(f,g,h,i), m2.traverse(f,g,h,i)));
        }
    }

    class And implements AggregateFilter {
        private final AggregateFilter f1;
        private final AggregateFilter f2;

        public And(AggregateFilter f1, AggregateFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public AggregateFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(new And(f1.traverse(f,g,h,i), f2.traverse(f,g,h,i)));
        }
    }

    class Or implements AggregateFilter {
        private final AggregateFilter f1;
        private final AggregateFilter f2;

        public Or(AggregateFilter f1, AggregateFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public AggregateFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(new Or(f1.traverse(f, g, h, i), f2.traverse(f, g, h, i)));
        }
    }

    class Not implements AggregateFilter {
        private final AggregateFilter filter;

        public Not(AggregateFilter filter) {
            this.filter = filter;
        }

        @Override
        public AggregateFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(new Not(filter.traverse(f, g, h, i)));
        }
    }

    class Regex implements AggregateFilter {
        private final String field;
        private final String regex;

        public Regex(String field, String regex) {
            this.field = field;
            this.regex = regex;
        }

        @Override
        public AggregateFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(this);
        }
    }

    class Always implements AggregateFilter {
        @Override
        public AggregateFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(this);
        }
    }

    class Never implements AggregateFilter {
        @Override
        public AggregateFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(this);
        }
    }
}
