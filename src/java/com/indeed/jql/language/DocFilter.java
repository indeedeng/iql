package com.indeed.jql.language;

import com.google.common.base.Function;

import java.util.List;

public interface DocFilter {

    DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i);

    DocMetric asZeroOneMetric(String dataset);

    class FieldIs implements DocFilter {
        private final String field;
        private final Term term;

        public FieldIs(String field, Term term) {
            this.field = field;
            this.term = term;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            if (term.isIntTerm) {
                return new DocMetric.HasInt(field, term.intTerm);
            } else {
                return new DocMetric.HasString(field, term.stringTerm);
            }
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
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new Not(new FieldIs(field, term)).asZeroOneMetric(dataset);
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
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new And(
                new MetricGte(new DocMetric.Field(field), new DocMetric.Constant(lowerBound)),
                new MetricLt(new DocMetric.Field(field), new DocMetric.Constant(upperBound))
            ).asZeroOneMetric(dataset);
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
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricEqual(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricEqual(m1, m2);
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
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricNotEqual(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricNotEqual(m1, m2);
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
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricGt(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricGt(m1, m2);
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
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricGte(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricGte(m1, m2);
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
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricLt(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricLt(m1, m2);
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
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricLte(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricLte(m1, m2);
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
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new And(f1.transform(g, i), f2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricEqual(new DocMetric.Add(f1.asZeroOneMetric(dataset), f2.asZeroOneMetric(dataset)), new DocMetric.Constant(2));
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
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new Or(f1.transform(g, i), f2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricGt(new DocMetric.Add(f1.asZeroOneMetric(dataset), f2.asZeroOneMetric(dataset)), new DocMetric.Constant(0));
        }
    }

    class Not implements DocFilter {
        private final DocFilter filter;

        public Not(DocFilter filter) {
            this.filter = filter;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new Not(filter.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.Subtract(new DocMetric.Constant(1), filter.asZeroOneMetric(dataset));
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
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.RegexMetric(field, regex);
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
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new Not(new Regex(field, regex)).asZeroOneMetric(dataset);
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
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new Qualified(scope, filter.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            if (scope.contains(dataset)) {
                return filter.asZeroOneMetric(dataset);
            } else {
                // TODO: Is this what we want to be doing?
                return new DocMetric.Constant(1);
            }
        }
    }

    class Lucene implements DocFilter {
        private final String query;

        public Lucene(String query) {
            this.query = query;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            throw new UnsupportedOperationException("Haven't implemented Lucene queries yet");
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
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            throw new UnsupportedOperationException("Haven't implemented Sample yet");
        }
    }

    class Always implements DocFilter {
        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.Constant(1);
        }
    }

    class Never implements DocFilter {
        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.Constant(0);
        }
    }
}
