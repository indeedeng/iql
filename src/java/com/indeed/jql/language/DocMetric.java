package com.indeed.jql.language;

public interface DocMetric {

    class Field implements DocMetric {
        private final String field;

        public Field(String field) {
            this.field = field;
        }
    }

    class Unop implements DocMetric {
        private final DocMetric m1;

        public Unop(DocMetric m1) {
            this.m1 = m1;
        }
    }

    class Log extends Unop {
        public Log(DocMetric m1) {
            super(m1);
        }
    }

    class Negate extends Unop {
        public Negate(DocMetric m1) {
            super(m1);
        }
    }

    class Abs extends Unop {
        public Abs(DocMetric m1) {
            super(m1);
        }
    }

    class Signum extends Unop {
        public Signum(DocMetric m1) {
            super(m1);
        }
    }

    class Binop implements DocMetric {
        private final DocMetric m1;
        private final DocMetric m2;

        public Binop(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }
    }

    class Add extends Binop {
        public Add(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class Subtract extends Binop {
        public Subtract(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class Multiply extends Binop {
        public Multiply(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class Divide extends Binop {
        public Divide(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class Modulus extends Binop {
        public Modulus(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class Min extends Binop {
        public Min(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class Max extends Binop {
        public Max(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class MetricEqual extends Binop {
        public MetricEqual(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class MetricNotEqual extends Binop {
        public MetricNotEqual(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class MetricLt extends Binop {
        public MetricLt(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class MetricLte extends Binop {
        public MetricLte(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class MetricGt extends Binop {
        public MetricGt(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class MetricGte extends Binop {
        public MetricGte(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }
    }

    class RegexMetric implements DocMetric {
        private final String field;
        private final String regex;

        public RegexMetric(String field, String regex) {
            this.field = field;
            this.regex = regex;
        }
    }

    class FloatScale implements DocMetric {
        private final String field;
        private final double mult;
        private final double add;

        public FloatScale(String field, double mult, double add) {
            this.field = field;
            this.mult = mult;
            this.add = add;
        }
    }

    class Constant implements DocMetric {
        private final long value;

        public Constant(long value) {
            this.value = value;
        }
    }

    class HasInt implements DocMetric {
        private final String field;
        private final long term;

        public HasInt(String field, long term) {
            this.field = field;
            this.term = term;
        }
    }

    class HasString implements DocMetric {
        private final String field;
        private final String term;

        public HasString(String field, String term) {
            this.field = field;
            this.term = term;
        }
    }

    class IfThenElse implements DocMetric {
        private final DocFilter condition;
        private final DocMetric trueCase;
        private final DocMetric falseCase;

        public IfThenElse(DocFilter condition, DocMetric trueCase, DocMetric falseCase) {
            this.condition = condition;
            this.trueCase = trueCase;
            this.falseCase = falseCase;
        }
    }

}
