package com.indeed.jql.language;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public interface DocMetric {

    DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i);

    List<String> getPushes(String dataset);

    class Field implements DocMetric {
        private final String field;

        public Field(String field) {
            this.field = field;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return Collections.singletonList(field);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Field field1 = (Field) o;
            return Objects.equals(field, field1.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field);
        }

        @Override
        public String toString() {
            return "Field{" +
                    "field='" + field + '\'' +
                    '}';
        }
    }

    abstract class Unop implements DocMetric {
        protected final DocMetric m1;

        public Unop(DocMetric m1) {
            this.m1 = m1;
        }

        protected List<String> unop(String dataset, String operator) {
            final List<String> pushes = m1.getPushes(dataset);
            final ArrayList<String> result = Lists.newArrayList(pushes);
            result.add(operator);
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Unop unop = (Unop) o;
            return Objects.equals(m1, unop.m1);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1);
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "{" +
                    "m1=" + m1 +
                    '}';
        }
    }

    class Log extends Unop {
        public Log(DocMetric m1) {
            super(m1);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Log(m1.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            return unop(dataset, "log");
        }
    }

    class Negate extends Unop {
        public Negate(DocMetric m1) {
            super(m1);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Negate(m1.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            return new Subtract(new Constant(0), m1).getPushes(dataset);
        }
    }

    class Abs extends Unop {
        public Abs(DocMetric m1) {
            super(m1);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Abs(m1.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            return unop(dataset, "abs()");
        }
    }

    class Signum extends Unop {
        public Signum(DocMetric m1) {
            super(m1);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Signum(m1.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            return new IfThenElse(new DocFilter.MetricGt(m1, new Constant(0)),
                                  new Constant(1),
                                  new IfThenElse(new DocFilter.MetricLt(m1, new Constant(0)),
                                                 new Constant(-1),
                                                 new Constant(0))
                                 ).getPushes(dataset);
        }
    }

    abstract class Binop implements DocMetric {
        protected final DocMetric m1;
        protected final DocMetric m2;

        public Binop(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        protected List<String> binop(String dataset, String operator) {
            final List<String> result = new ArrayList<>();
            result.addAll(m1.getPushes(dataset));
            result.addAll(m2.getPushes(dataset));
            result.add(operator);
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Binop binop = (Binop) o;
            return Objects.equals(m1, binop.m1) &&
                    Objects.equals(m2, binop.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return this.getClass().toString() + "{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    class Add extends Binop {
        public Add(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Add(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "+");
        }
    }

    class Subtract extends Binop {
        public Subtract(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Subtract(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "-");
        }
    }

    class Multiply extends Binop {
        public Multiply(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Multiply(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "*");
        }
    }

    class Divide extends Binop {
        public Divide(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Divide(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "/");
        }
    }

    class Modulus extends Binop {
        public Modulus(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Modulus(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "%");
        }
    }

    class Min extends Binop {
        public Min(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Min(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "min()");
        }
    }

    class Max extends Binop {
        public Max(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Max(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "max()");
        }
    }

    class MetricEqual extends Binop {
        public MetricEqual(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new MetricEqual(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "=");
        }
    }

    class MetricNotEqual extends Binop {
        public MetricNotEqual(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new MetricNotEqual(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "!=");
        }
    }

    class MetricLt extends Binop {
        public MetricLt(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new MetricLt(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "<");
        }
    }

    class MetricLte extends Binop {
        public MetricLte(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new MetricLte(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "<=");
        }
    }

    class MetricGt extends Binop {
        public MetricGt(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new MetricGt(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, ">");
        }
    }

    class MetricGte extends Binop {
        public MetricGte(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new MetricGte(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, ">=");
        }
    }

    class RegexMetric implements DocMetric {
        private final String field;
        private final String regex;

        public RegexMetric(String field, String regex) {
            this.field = field;
            this.regex = regex;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return Collections.singletonList("regex " + field + ":" + regex);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RegexMetric that = (RegexMetric) o;
            return Objects.equals(field, that.field) &&
                    Objects.equals(regex, that.regex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, regex);
        }

        @Override
        public String toString() {
            return "RegexMetric{" +
                    "field='" + field + '\'' +
                    ", regex='" + regex + '\'' +
                    '}';
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

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return Collections.singletonList("floatscale " + field + "*" + mult + "+" + add);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FloatScale that = (FloatScale) o;
            return Objects.equals(mult, that.mult) &&
                    Objects.equals(add, that.add) &&
                    Objects.equals(field, that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, mult, add);
        }

        @Override
        public String toString() {
            return "FloatScale{" +
                    "field='" + field + '\'' +
                    ", mult=" + mult +
                    ", add=" + add +
                    '}';
        }
    }

    class Constant implements DocMetric {
        private final long value;

        public Constant(long value) {
            this.value = value;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            if (value < 0) {
                // TODO: Is this still necessary?
                return new Negate(new Constant(value)).getPushes(dataset);
            } else {
                return Collections.singletonList(String.valueOf(value));
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Constant constant = (Constant) o;
            return Objects.equals(value, constant.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        @Override
        public String toString() {
            return "Constant{" +
                    "value=" + value +
                    '}';
        }
    }

    class HasInt implements DocMetric {
        private final String field;
        private final long term;

        public HasInt(String field, long term) {
            this.field = field;
            this.term = term;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return Collections.singletonList("hasint " + field + ":" + term);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HasInt hasInt = (HasInt) o;
            return Objects.equals(term, hasInt.term) &&
                    Objects.equals(field, hasInt.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, term);
        }

        @Override
        public String toString() {
            return "HasInt{" +
                    "field='" + field + '\'' +
                    ", term=" + term +
                    '}';
        }
    }

    class HasString implements DocMetric {
        private final String field;
        private final String term;

        public HasString(String field, String term) {
            this.field = field;
            this.term = term;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return Collections.singletonList("hasstr " + field + ":" + term);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HasString hasString = (HasString) o;
            return Objects.equals(field, hasString.field) &&
                    Objects.equals(term, hasString.term);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, term);
        }

        @Override
        public String toString() {
            return "HasString{" +
                    "field='" + field + '\'' +
                    ", term='" + term + '\'' +
                    '}';
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

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new IfThenElse(condition.transform(g, i), trueCase.transform(g, i), falseCase.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            final DocMetric truth = condition.asZeroOneMetric(dataset);
            return new Add(new Multiply(truth, trueCase), new Multiply(new Subtract(new Constant(1), truth), falseCase)).getPushes(dataset);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IfThenElse that = (IfThenElse) o;
            return Objects.equals(condition, that.condition) &&
                    Objects.equals(trueCase, that.trueCase) &&
                    Objects.equals(falseCase, that.falseCase);
        }

        @Override
        public int hashCode() {
            return Objects.hash(condition, trueCase, falseCase);
        }

        @Override
        public String toString() {
            return "IfThenElse{" +
                    "condition=" + condition +
                    ", trueCase=" + trueCase +
                    ", falseCase=" + falseCase +
                    '}';
        }
    }
}
