package com.indeed.jql;

import java.util.List;

public interface DocFilter {

    class FieldIs implements DocFilter {
        private final String field;
        private final Term term;

        public FieldIs(String field, Term term) {
            this.field = field;
            this.term = term;
        }
    }

    class FieldIsnt implements DocFilter {
        private final String field;
        private final Term term;

        public FieldIsnt(String field, Term term) {
            this.field = field;
            this.term = term;
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
    }

    class MetricEqual implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricEqual(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class MetricNotEqual implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricNotEqual(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class MetricGt implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricGt(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class MetricGte implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricGte(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class MetricLt implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricLt(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class MetricLte implements DocFilter {
        private final DocMetric m1;
        private final DocMetric m2;

        public MetricLte(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class And implements DocFilter {
        private final DocFilter f1;
        private final DocFilter f2;

        public And(DocFilter f1, DocFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }
    }

    class Or implements DocFilter {
        private final DocFilter f1;
        private final DocFilter f2;

        public Or(DocFilter f1, DocFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }
    }

    class Not implements DocFilter {
        private final DocFilter f;

        public Not(DocFilter f) {
            this.f = f;
        }
    }

    class Regex implements DocFilter {
        private final String field;
        private final String regex;

        public Regex(String field, String regex) {
            this.field = field;
            this.regex = regex;
        }
    }

    class NotRegex implements DocFilter {
        private final String field;
        private final String regex;

        public NotRegex(String field, String regex) {
            this.field = field;
            this.regex = regex;
        }
    }

    class Qualified implements DocFilter {
        private final List<String> scope;
        private final DocFilter filter;

        public Qualified(List<String> scope, DocFilter filter) {
            this.scope = scope;
            this.filter = filter;
        }
    }

    class Lucene implements DocFilter {
        private final String query;

        public Lucene(String query) {
            this.query = query;
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
    }

    class Always implements DocFilter {}
    class Never implements DocFilter {}
}
