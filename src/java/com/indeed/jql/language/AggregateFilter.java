package com.indeed.jql.language;

import com.google.common.base.Function;

public interface AggregateFilter {

    AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i);

    AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f);

    class TermIs implements AggregateFilter {
        private final Term term;

        public TermIs(Term term) {
            this.term = term;
        }

        @Override
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(this);
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
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
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(new MetricIs(m1.transform(f,g,h,i), m2.transform(f,g,h,i)));
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new MetricIs(f.apply(m1), f.apply(m2));
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
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(new MetricIsnt(m1.transform(f,g,h,i), m2.transform(f,g,h,i)));
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new MetricIs(f.apply(m1), f.apply(m2));
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
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(new Gt(m1.transform(f,g,h,i), m2.transform(f,g,h,i)));
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Gt(f.apply(m1), f.apply(m2));
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
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(new Gte(m1.transform(f,g,h,i), m2.transform(f,g,h,i)));
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Gte(f.apply(m1), f.apply(m2));
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
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(new Lt(m1.transform(f,g,h,i), m2.transform(f,g,h,i)));
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Lt(f.apply(m1), f.apply(m2));
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
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(new Lte(m1.transform(f,g,h,i), m2.transform(f,g,h,i)));
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Lte(f.apply(m1), f.apply(m2));
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
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(new And(f1.transform(f,g,h,i), f2.transform(f,g,h,i)));
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new And(f1.traverse1(f), f2.traverse1(f));
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
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(new Or(f1.transform(f, g, h, i), f2.transform(f, g, h, i)));
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Or(f1.traverse1(f), f2.traverse1(f));
        }
    }

    class Not implements AggregateFilter {
        private final AggregateFilter filter;

        public Not(AggregateFilter filter) {
            this.filter = filter;
        }

        @Override
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(new Not(filter.transform(f, g, h, i)));
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Not(filter.traverse1(f));
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
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(this);
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }
    }

    class Always implements AggregateFilter {
        @Override
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(this);
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }
    }

    class Never implements AggregateFilter {
        @Override
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return h.apply(this);
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }
    }
}
