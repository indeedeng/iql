package com.indeed.squall.iql2.language;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.marshal.ImhotepClientMarshaller;
import com.indeed.imhotep.protobuf.QueryMessage;
import com.indeed.squall.iql2.language.metadata.DatasetsMetadata;
import com.indeed.squall.iql2.language.optimizations.ConstantFolding;
import com.indeed.squall.iql2.language.util.ErrorMessages;
import com.indeed.squall.iql2.language.util.ParserUtil;
import com.indeed.squall.iql2.language.util.ValidationHelper;
import com.indeed.squall.iql2.language.util.ValidationUtil;
import org.apache.commons.codec.binary.Base64;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public abstract class DocMetric extends AbstractPositional {
    public interface Visitor<T, E extends Throwable> {
        T visit(Log log) throws E;
        T visit(PushableDocMetric pushableDocMetric) throws E;
        T visit(PerDatasetDocMetric perDatasetDocMetric) throws E;
        T visit(Count count) throws E;
        T visit(DocId count) throws E;
        T visit(Field field) throws E;
        T visit(Exponentiate exponentiate) throws E;
        T visit(Negate negate) throws E;
        T visit(Abs abs) throws E;
        T visit(Signum signum) throws E;
        T visit(Add add) throws E;
        T visit(Subtract subtract) throws E;
        T visit(Multiply multiply) throws E;
        T visit(Divide divide) throws E;
        T visit(Modulus modulus) throws E;
        T visit(Min min) throws E;
        T visit(Max max) throws E;
        T visit(MetricEqual metricEqual) throws E;
        T visit(MetricNotEqual metricNotEqual) throws E;
        T visit(MetricLt metricLt) throws E;
        T visit(MetricLte metricLte) throws E;
        T visit(MetricGt metricGt) throws E;
        T visit(MetricGte metricGte) throws E;
        T visit(RegexMetric regexMetric) throws E;
        T visit(FloatScale floatScale) throws E;
        T visit(Constant constant) throws E;
        T visit(HasIntField hasIntField) throws E;
        T visit(HasStringField hasStringField) throws E;
        T visit(IntTermCount intTermCount) throws E;
        T visit(StrTermCount stringTermCount) throws E;
        T visit(HasInt hasInt) throws E;
        T visit(HasString hasString) throws E;
        T visit(IfThenElse ifThenElse) throws E;
        T visit(Qualified qualified) throws E;
        T visit(Extract extract) throws E;
        T visit(Lucene lucene) throws E;
        T visit(FieldEqualMetric equalMetric) throws E;
        T visit(StringLen hasStringField) throws E;
    }

    public abstract DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i);

    protected abstract List<String> getPushes(String dataset);

    public abstract <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E;

    public abstract void validate(String dataset, ValidationHelper validationHelper, Validator validator);

    public static class PushableDocMetric extends DocMetric {
        public final DocMetric metric;

        public PushableDocMetric(DocMetric metric) {
            this.metric = metric;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new PushableDocMetric(metric.transform(g, i)));
        }

        @Override
        public List<String> getPushes(String dataset) {
            return ConstantFolding.apply(metric).getPushes(dataset);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            metric.validate(dataset, validationHelper, validator);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PushableDocMetric that = (PushableDocMetric) o;
            return Objects.equals(metric, that.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(metric);
        }

        @Override
        public String toString() {
            return "PushableDocMetric{" +
                    "metric=" + metric +
                    '}';
        }
    }

    public static class PerDatasetDocMetric extends DocMetric {
        public final ImmutableMap<String, DocMetric> datasetToMetric;

        public PerDatasetDocMetric(final Map<String, DocMetric> datasetToMetric) {
            this.datasetToMetric = ImmutableMap.copyOf(datasetToMetric);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new PerDatasetDocMetric(Maps.transformValues(datasetToMetric, d -> d.transform(g, i))));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            if (!datasetToMetric.containsKey(dataset)) {
                throw new IllegalArgumentException("Unknown dataset: " + dataset + " in [" + this + "]");
            } else {
                return datasetToMetric.get(dataset).getPushes(dataset);
            }
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            if (!datasetToMetric.containsKey(dataset)) {
                throw new IllegalArgumentException("Unknown dataset: " + dataset + " in [" + this + "]");
            } else {
                datasetToMetric.get(dataset).validate(dataset, validationHelper, validator);
            }
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final PerDatasetDocMetric that = (PerDatasetDocMetric) o;
            return com.google.common.base.Objects.equal(datasetToMetric, that.datasetToMetric);
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects.hashCode(datasetToMetric);
        }

        @Override
        public String toString() {
            return "PerDatasetDocMetric{" +
                    "datasetToMetric=" + datasetToMetric +
                    '}';
        }
    }

    public static class Count extends DocMetric {
        public Count() {
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return Collections.singletonList("count()");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
        }

        @Override
        public boolean equals(Object o) {
            return getClass() == o.getClass();
        }

        @Override
        public int hashCode() {
            return 611953;
        }

        @Override
        public String toString() {
            return "Count{}";
        }
    }

    public static class DocId extends DocMetric {
        public DocId() {
        }

        @Override
        public DocMetric transform(final Function<DocMetric, DocMetric> g,
                                   final Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        protected List<String> getPushes(final String dataset) {
            return Collections.singletonList("docId()");
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(final String dataset,
                             final ValidationHelper validationHelper,
                             final Validator validator) {
        }

        @Override
        public boolean equals(final Object o) {
            return getClass() == o.getClass();
        }

        @Override
        public int hashCode() {
            return 71543399;
        }

        @Override
        public String toString() {
            return "DocId{}";
        }
    }

    public static class Field extends DocMetric {
        // Positioned<String> unnecessary, since this Object (DocMetric.Field)
        // represents exactly just the string and is Positioned itself.
        public final String field;

        public Field(String field) {
            this.field = field;
        }

        public Field(Positioned<String> field) {
            this.field = field.unwrap();
            this.copyPosition(field);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return Collections.singletonList(field);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            if (!validationHelper.containsField(dataset, field)) {
                validator.error(ErrorMessages.missingField(dataset, field, this));
            }
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

    public abstract static class Unop extends DocMetric {
        public final DocMetric m1;

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

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            m1.validate(dataset, validationHelper, validator);
        }
    }

    public static class Log extends DocMetric {
        public final DocMetric metric;
        public final int scaleFactor;

        public Log(DocMetric metric, int scaleFactor) {
            this.metric = metric;
            this.scaleFactor = scaleFactor;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Log(metric.transform(g, i), scaleFactor));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            final List<String> result = new ArrayList<>(metric.getPushes(dataset));
            result.add("log " + scaleFactor);
            return result;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            metric.validate(dataset, validationHelper, validator);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Log log = (Log) o;

            if (scaleFactor != log.scaleFactor) return false;
            return !(metric != null ? !metric.equals(log.metric) : log.metric != null);

        }

        @Override
        public int hashCode() {
            int result = metric != null ? metric.hashCode() : 0;
            result = 31 * result + scaleFactor;
            return result;
        }

        @Override
        public String toString() {
            return "Log{" +
                    "metric=" + metric +
                    ", scaleFactor=" + scaleFactor +
                    '}';
        }
    }

    public static class Exponentiate extends DocMetric {
        public final DocMetric metric;
        public final int scaleFactor;

        public Exponentiate(DocMetric metric, int scaleFactor) {
            this.metric = metric;
            this.scaleFactor = scaleFactor;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Exponentiate(metric.transform(g, i), scaleFactor));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            final List<String> result = new ArrayList<>(metric.getPushes(dataset));
            result.add("exp " + scaleFactor);
            return result;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            metric.validate(dataset, validationHelper, validator);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Exponentiate that = (Exponentiate) o;

            if (scaleFactor != that.scaleFactor) return false;
            return !(metric != null ? !metric.equals(that.metric) : that.metric != null);

        }

        @Override
        public int hashCode() {
            int result = metric != null ? metric.hashCode() : 0;
            result = 31 * result + scaleFactor;
            return result;
        }

        @Override
        public String toString() {
            return "Exponentiate{" +
                    "metric=" + metric +
                    ", scaleFactor=" + scaleFactor +
                    '}';
        }
    }

    public static class Negate extends Unop {
        public Negate(DocMetric m1) {
            super(m1);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Negate(m1.transform(g, i)));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return new PushableDocMetric(new Subtract(new Constant(0), m1)).getPushes(dataset);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    public static class Abs extends Unop {
        public Abs(DocMetric m1) {
            super(m1);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Abs(m1.transform(g, i)));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return unop(dataset, "abs()");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    public static class Signum extends Unop {
        public Signum(DocMetric m1) {
            super(m1);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Signum(m1.transform(g, i)));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            final IfThenElse m = new IfThenElse(new DocFilter.MetricGt(m1, new Constant(0)),
                    new Constant(1),
                    new IfThenElse(new DocFilter.MetricLt(m1, new Constant(0)),
                            new Constant(-1),
                            new Constant(0))
            );
            return new PushableDocMetric(m).getPushes(dataset);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    public abstract static class Binop extends DocMetric {
        public final DocMetric m1;
        public final DocMetric m2;

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

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            m1.validate(dataset, validationHelper, validator);
            m2.validate(dataset, validationHelper, validator);
        }
    }

    public static class Add extends Binop {
        public Add(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Add(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return binop(dataset, "+");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    public static class Subtract extends Binop {
        public Subtract(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Subtract(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return binop(dataset, "-");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    public static class Multiply extends Binop {
        public Multiply(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Multiply(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return binop(dataset, "*");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    public static class Divide extends Binop {
        public Divide(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Divide(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return binop(dataset, "/");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    public static class Modulus extends Binop {
        public Modulus(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Modulus(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return binop(dataset, "%");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    public static class Min extends Binop {
        public Min(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Min(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return binop(dataset, "min()");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    public static class Max extends Binop {
        public Max(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Max(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return binop(dataset, "max()");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    public static class MetricEqual extends Binop {
        public MetricEqual(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new MetricEqual(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return binop(dataset, "=");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    public static class MetricNotEqual extends Binop {
        public MetricNotEqual(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new MetricNotEqual(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return binop(dataset, "!=");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    public static class MetricLt extends Binop {
        public MetricLt(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new MetricLt(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return binop(dataset, "<");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    public static class MetricLte extends Binop {
        public MetricLte(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new MetricLte(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return binop(dataset, "<=");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    public static class MetricGt extends Binop {
        public MetricGt(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new MetricGt(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return binop(dataset, ">");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    public static class MetricGte extends Binop {
        public MetricGte(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new MetricGte(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return binop(dataset, ">=");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    public static class RegexMetric extends DocMetric {
        public final Positioned<String> field;
        public final String regex;

        public RegexMetric(Positioned<String> field, String regex) {
            this.field = field;
            ValidationUtil.compileRegex(regex);
            this.regex = regex;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return Collections.singletonList("regex " + field.unwrap() + ":" + regex);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            if (!validationHelper.containsStringField(dataset, field.unwrap())) {
                validator.error(ErrorMessages.missingStringField(dataset, field.unwrap(), this));
            }
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

    public static class FieldEqualMetric extends DocMetric {
        public final Positioned<String> field1;
        public final Positioned<String> field2;

        public FieldEqualMetric(Positioned<String> field1, Positioned<String> field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return Collections.singletonList("fieldequal " + field1.unwrap() + "=" + field2.unwrap());
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            ValidationUtil.validateExistenceAndSameFieldType(dataset, field1.unwrap(), field2.unwrap(), validationHelper, validator);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
        @Override
        public int hashCode() {
            return com.google.common.base.Objects.hashCode(field1, field2);
        }


        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final FieldEqualMetric that = (FieldEqualMetric) o;
            return com.google.common.base.Objects.equal(field1, that.field1) &&
                    com.google.common.base.Objects.equal(field2, that.field2);
        }

        @Override
        public String toString() {
            return "FieldEqualMetric{" +
                    "field1=" + field1 +
                    ", field2=" + field2 +
                    '}';
        }

    }

    public static class FloatScale extends DocMetric {
        public final Positioned<String> field;
        public final double mult;
        public final double add;

        public FloatScale(Positioned<String> field, double mult, double add) {
            this.field = field;
            this.mult = mult;
            this.add = add;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return Collections.singletonList("floatscale " + field.unwrap() + "*" + mult + "+" + add);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            if (!validationHelper.containsStringField(dataset, field.unwrap())) {
                validator.error(ErrorMessages.missingStringField(dataset, field.unwrap(), this));
            }
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

    public static class Constant extends DocMetric {
        public final long value;

        public Constant(long value) {
            this.value = value;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return Collections.singletonList(String.valueOf(value));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {

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

    public static class HasIntField extends DocMetric {
        public final Positioned<String> field;

        public HasIntField(Positioned<String> field) {
            this.field = field;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return Collections.singletonList("hasintfield " + field.unwrap());
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            // Don't validate, since this is used for investigating field presence
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HasIntField that = (HasIntField) o;
            return Objects.equals(field, that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field);
        }

        @Override
        public String toString() {
            return "HasIntField{" +
                    "field='" + field + '\'' +
                    '}';
        }
    }

    public static class HasStringField extends DocMetric {
        public final Positioned<String> field;

        public HasStringField(Positioned<String> field) {
            this.field = field;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return Collections.singletonList("hasstrfield " + field.unwrap());
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            // Don't validate, since this is used for investigating field presence
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HasStringField that = (HasStringField) o;
            return Objects.equals(field, that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field);
        }

        @Override
        public String toString() {
            return "HasStringField{" +
                    "field='" + field + '\'' +
                    '}';
        }
    }

    public static class HasInt extends DocMetric {
        public final Positioned<String> field;
        public final long term;

        public HasInt(Positioned<String> field, long term) {
            this.field = field;
            this.term = term;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return Collections.singletonList("hasint " + field.unwrap() + ":" + term);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            final String datasetField = field.unwrap();
            validationHelper.validateIntField(dataset, datasetField, validator, this);
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

    public static class HasString extends DocMetric {
        public final Positioned<String> field;
        public final String term;

        public HasString(Positioned<String> field, String term) {
            this.field = field;
            this.term = term;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return Collections.singletonList("hasstr " + field.unwrap() + ":" + term);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            if (!validationHelper.containsStringField(dataset, field.unwrap())) {
                validator.error(ErrorMessages.missingStringField(dataset, field.unwrap(), this));
            }
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

    public static class IfThenElse extends DocMetric {
        public final DocFilter condition;
        public final DocMetric trueCase;
        public final DocMetric falseCase;

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
        protected List<String> getPushes(String dataset) {
            final DocMetric truth = condition.asZeroOneMetric(dataset);
            return new PushableDocMetric(new Add(new Multiply(truth, trueCase), new Multiply(new Subtract(new Constant(1), truth), falseCase))).getPushes(dataset);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            condition.validate(dataset, validationHelper, validator);
            trueCase.validate(dataset, validationHelper, validator);
            falseCase.validate(dataset, validationHelper, validator);
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

    public static class Qualified extends DocMetric {
        public final String dataset;
        public final DocMetric metric;

        public Qualified(String dataset, DocMetric metric) {
            this.dataset = dataset;
            this.metric = metric;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Qualified(dataset, metric.transform(g, i)));
        }

        @Override
        protected List<String> getPushes(String dataset) {
            if (!dataset.equals(this.dataset)) {
                throw new IllegalStateException("Qualified DocMetric getting pushes for a different dataset! [" + this.dataset + "] != [" + dataset + "]");
            }
            return metric.getPushes(dataset);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            if (!dataset.equals(this.dataset)) {
                validator.error("Qualified DocMetric getting validated against different dataset! [" + this.dataset + "] != [" + dataset + "]");
            }
            metric.validate(this.dataset, validationHelper, validator);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Qualified qualified = (Qualified) o;
            return Objects.equals(dataset, qualified.dataset) &&
                    Objects.equals(metric, qualified.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataset, metric);
        }

        @Override
        public String toString() {
            return "Qualified{" +
                    "dataset='" + dataset + '\'' +
                    ", metric=" + metric +
                    '}';
        }
    }

    public static class Extract extends DocMetric {
        public final Positioned<String> field;
        public final String regex;
        public final int groupNumber;

        public Extract(Positioned<String> field, String regex, int groupNumber) {
            this.field = field;
            this.regex = regex;
            this.groupNumber = groupNumber;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        protected List<String> getPushes(String dataset) {
            return Collections.singletonList("regexmatch " + field.unwrap() + " " + groupNumber + " " + regex);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            try {
                Pattern.compile(regex);
            } catch (PatternSyntaxException e) {
                validator.error("Invalid pattern: " + regex);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Extract extract = (Extract) o;
            return groupNumber == extract.groupNumber &&
                    Objects.equals(field, extract.field) &&
                    Objects.equals(regex, extract.regex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, regex, groupNumber);
        }

        @Override
        public String toString() {
            return "Extract{" +
                    "field='" + field + '\'' +
                    ", regex='" + regex + '\'' +
                    ", groupNumber=" + groupNumber +
                    '}';
        }
    }

    public static class Lucene extends DocMetric {
        public final String query;
        public final DatasetsMetadata datasetsMetadata;

        public Lucene(String query, DatasetsMetadata datasetsMetadata) {
            this.query = query;
            this.datasetsMetadata = datasetsMetadata;
        }

        @Override
        public DocMetric transform(final Function<DocMetric, DocMetric> g, final Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        protected List<String> getPushes(final String dataset) {
            final Query flamdexQuery = ParserUtil.getFlamdexQuery(
                    query, dataset, datasetsMetadata);
            final QueryMessage luceneQueryMessage = ImhotepClientMarshaller.marshal(flamdexQuery);
            final String base64EncodedQuery = Base64.encodeBase64String(luceneQueryMessage.toByteArray());
            return Lists.newArrayList("lucene " + base64EncodedQuery);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(final String dataset, final ValidationHelper validationHelper, final Validator validator) {
            final Query flamdexQuery = ParserUtil.getFlamdexQuery(
                    query, dataset, datasetsMetadata);
            ValidationUtil.validateQuery(validationHelper, ImmutableMap.of(dataset, flamdexQuery), validator, this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Lucene lucene = (Lucene) o;
            return query == lucene.query;
        }

        @Override
        public int hashCode() {
            return Objects.hash(query);
        }

        @Override
        public String toString() {
            return "Lucene{" +
                    "query='" + query + '}';
        }
    }

    public static class StringLen extends DocMetric {
        public final Positioned<String> field;

        public StringLen(final Positioned<String> field) {
            this.field = field;
        }

        @Override
        public DocMetric transform(final Function<DocMetric, DocMetric> g, final Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        protected List<String> getPushes(final String dataset) {
            return Collections.singletonList("len " + field.unwrap());
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(final String dataset, final ValidationHelper validationHelper, final Validator validator) {
            if(!validationHelper.containsStringField(dataset, field.unwrap())) {
                validator.error(ErrorMessages.missingStringField(dataset, field.unwrap(), this));
            }
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final StringLen that = (StringLen) o;
            return Objects.equals(field, that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field);
        }

        @Override
        public String toString() {
            return "StringLen{" +
                    "field='" + field + '\'' +
                    '}';
        }
    }

    public static class IntTermCount extends DocMetric {
        public final Positioned<String> field;

        public IntTermCount(final Positioned<String> field) {
            this.field = field;
        }

        @Override
        public DocMetric transform(final Function<DocMetric, DocMetric> g, final Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        protected List<String> getPushes(final String dataset) {
            return Collections.singletonList("inttermcount " + field.unwrap());
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(final String dataset, final ValidationHelper validationHelper, final Validator validator) {
            if(!validationHelper.containsIntOrAliasField(dataset, field.unwrap())) {
                validator.error(ErrorMessages.missingIntField(dataset, field.unwrap(), this));
            }
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            IntTermCount that = (IntTermCount) o;
            return Objects.equals(field, that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field);
        }

        @Override
        public String toString() {
            return "IntTermCount{" +
                    "field='" + field + '\'' +
                    '}';
        }
    }

    public static class StrTermCount extends DocMetric {
        public final Positioned<String> field;

        public StrTermCount(final Positioned<String> field) {
            this.field = field;
        }

        @Override
        public DocMetric transform(final Function<DocMetric, DocMetric> g, final Function<DocFilter, DocFilter> i) {
            return g.apply(this);
        }

        @Override
        protected List<String> getPushes(final String dataset) {
            return Collections.singletonList("strtermcount " + field.unwrap());
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(final String dataset, final ValidationHelper validationHelper, final Validator validator) {
            if(!validationHelper.containsStringField(dataset, field.unwrap())) {
                validator.error(ErrorMessages.missingStringField(dataset, field.unwrap(), this));
            }
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StrTermCount that = (StrTermCount) o;
            return Objects.equals(field, that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field);
        }

        @Override
        public String toString() {
            return "StrTermCount{" +
                    "field='" + field + '\'' +
                    '}';
        }
    }
}
