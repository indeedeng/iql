package com.indeed.jql.language;

import com.google.common.base.Function;

import java.util.List;

public interface DocFilter {

    DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i);

    class FieldIs implements DocFilter {
        private final String field;
        private final Term term;

        public FieldIs(String field, Term term) {
            this.field = field;
            this.term = term;
        }

        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }
    }

    class FieldIsnt implements DocFilter {
        private final String field;
        private final Term term;

        public FieldIsnt(String field, Term term) {
            this.field = field;
            this.term = term;
        }

        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }
    }

    class Between implements DocFilter {
        private final String field;
        private final long lowerBound;
        private final long upperBound;

        public Between(String field, long lowerBound, long upperBound) {
            this.field = field;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }
    }

    class MetricEqual implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricEqual(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricEqual(m1.traverse(f, g, h, i), m2.traverse(f, g, h, i)));
        }
    }

    class MetricNotEqual implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricNotEqual(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricNotEqual(m1.traverse(f, g, h, i), m2.traverse(f, g, h, i)));
        }
    }

    class MetricGt implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricGt(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricGt(m1.traverse(f, g, h, i), m2.traverse(f, g, h, i)));
        }
    }

    class MetricGte implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricGte(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricGte(m1.traverse(f, g, h, i), m2.traverse(f, g, h, i)));
        }
    }

    class MetricLt implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricLt(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricLt(m1.traverse(f, g, h, i), m2.traverse(f, g, h, i)));
        }
    }

    class MetricLte implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricLte(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricLte(m1.traverse(f, g, h, i), m2.traverse(f, g, h, i)));
        }
    }

    class And implements DocFilter {
        private final DocFilter f1;
        private final DocFilter f2;

        public And(DocFilter f1, DocFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(new And(f1.traverse(f, g, h, i), f2.traverse(f, g, h, i)));
        }
    }

    class Or implements DocFilter {
        private final DocFilter f1;
        private final DocFilter f2;

        public Or(DocFilter f1, DocFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(new Or(f1.traverse(f, g, h, i), f2.traverse(f, g, h, i)));
        }
    }

    class Not implements DocFilter {
        private final DocFilter filter;

        public Not(DocFilter filter) {
            this.filter = filter;
        }

        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(new Not(filter.traverse(f, g, h, i)));
        }
    }

    class Regex implements DocFilter {
        private final String field;
        private final String regex;

        public Regex(String field, String regex) {
            this.field = field;
            this.regex = regex;
        }

        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }
    }

    class NotRegex implements DocFilter {
        private final String field;
        private final String regex;

        public NotRegex(String field, String regex) {
            this.field = field;
            this.regex = regex;
        }

        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }
    }

    class Qualified implements DocFilter {
        private final List<String> scope;
        private final DocFilter filter;

        public Qualified(List<String> scope, DocFilter filter) {
            this.scope = scope;
            this.filter = filter;
        }

        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(new Qualified(scope, filter.traverse(f, g, h, i)));
        }
    }

    class Lucene implements DocFilter {
        private final String query;

        public Lucene(String query) {
            this.query = query;
        }

        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }
    }

    class Sample implements DocFilter {
        private final String field;
        private final long numerator;
        private final long denominator;
        private final String seed;

        public Sample(String field, long numerator, long denominator, String seed) {
            this.field = field;
            this.numerator = numerator;
            this.denominator = denominator;
            this.seed = seed;
        }

        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }
    }

    class Always implements DocFilter {
        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }
    }

    class Never implements DocFilter {
        @Override
        public DocFilter traverse(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }
    }
}
