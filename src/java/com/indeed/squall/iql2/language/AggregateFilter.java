package com.indeed.squall.iql2.language;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.query.GroupBy;
import com.indeed.squall.iql2.language.util.DatasetsFields;
import com.indeed.squall.iql2.language.util.ErrorMessages;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

public interface AggregateFilter {

    AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction);

    AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f);

    void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer);

    boolean isOrdered();

    class TermIs implements AggregateFilter, JsonSerializable {
        private final Term term;

        public TermIs(Term term) {
            this.term = term;
        }

        @Override
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return h.apply(this);
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "termEquals", "value", term));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TermIs termIs = (TermIs) o;
            return Objects.equals(term, termIs.term);
        }

        @Override
        public int hashCode() {
            return Objects.hash(term);
        }

        @Override
        public String toString() {
            return "TermIs{" +
                    "term=" + term +
                    '}';
        }
    }

    class MetricIs implements AggregateFilter, JsonSerializable {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public MetricIs(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return h.apply(new MetricIs(m1.transform(f,g,h,i, groupByFunction), m2.transform(f,g,h,i, groupByFunction)));
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new MetricIs(f.apply(m1), f.apply(m2));
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            m1.validate(scope, datasetsFields, errorConsumer);
            m2.validate(scope, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return m1.isOrdered() || m2.isOrdered();
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "metricEquals", "arg1", m1, "arg2", m2));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricIs metricIs = (MetricIs) o;
            return Objects.equals(m1, metricIs.m1) &&
                    Objects.equals(m2, metricIs.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return "MetricIs{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    class MetricIsnt implements AggregateFilter, JsonSerializable {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public MetricIsnt(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return h.apply(new MetricIsnt(m1.transform(f,g,h,i, groupByFunction), m2.transform(f,g,h,i, groupByFunction)));
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new MetricIsnt(f.apply(m1), f.apply(m2));
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            m1.validate(scope, datasetsFields, errorConsumer);
            m2.validate(scope, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return m1.isOrdered() || m2.isOrdered();
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "metricNotEquals", "arg1", m1, "arg2", m2));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricIsnt that = (MetricIsnt) o;
            return Objects.equals(m1, that.m1) &&
                    Objects.equals(m2, that.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return "MetricIsnt{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    class Gt implements AggregateFilter, JsonSerializable {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public Gt(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return h.apply(new Gt(m1.transform(f,g,h,i, groupByFunction), m2.transform(f,g,h,i, groupByFunction)));
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Gt(f.apply(m1), f.apply(m2));
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            m1.validate(scope, datasetsFields, errorConsumer);
            m2.validate(scope, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return m1.isOrdered() || m2.isOrdered();
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "greaterThan", "arg1", m1, "arg2", m2));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Gt gt = (Gt) o;
            return Objects.equals(m1, gt.m1) &&
                    Objects.equals(m2, gt.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return "Gt{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    class Gte implements AggregateFilter, JsonSerializable {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public Gte(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return h.apply(new Gte(m1.transform(f,g,h,i, groupByFunction), m2.transform(f,g,h,i, groupByFunction)));
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Gte(f.apply(m1), f.apply(m2));
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            m1.validate(scope, datasetsFields, errorConsumer);
            m2.validate(scope, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return m1.isOrdered() || m2.isOrdered();
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "greaterThanOrEquals", "arg1", m1, "arg2", m2));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Gte gte = (Gte) o;
            return Objects.equals(m1, gte.m1) &&
                    Objects.equals(m2, gte.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return "Gte{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    class Lt implements AggregateFilter, JsonSerializable {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public Lt(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return h.apply(new Lt(m1.transform(f,g,h,i, groupByFunction), m2.transform(f,g,h,i, groupByFunction)));
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Lt(f.apply(m1), f.apply(m2));
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            m1.validate(scope, datasetsFields, errorConsumer);
            m2.validate(scope, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return m1.isOrdered() || m2.isOrdered();
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "lessThan", "arg1", m1, "arg2", m2));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Lt lt = (Lt) o;
            return Objects.equals(m1, lt.m1) &&
                    Objects.equals(m2, lt.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return "Lt{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    class Lte implements AggregateFilter, JsonSerializable {
        private final AggregateMetric m1;
        private final AggregateMetric m2;

        public Lte(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return h.apply(new Lte(m1.transform(f,g,h,i, groupByFunction), m2.transform(f,g,h,i, groupByFunction)));
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Lte(f.apply(m1), f.apply(m2));
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            m1.validate(scope, datasetsFields, errorConsumer);
            m2.validate(scope, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return m1.isOrdered() || m2.isOrdered();
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "lessThanOrEquals", "arg1", m1, "arg2", m2));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Lte lte = (Lte) o;
            return Objects.equals(m1, lte.m1) &&
                    Objects.equals(m2, lte.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return "Lte{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    class And implements AggregateFilter, JsonSerializable {
        private final AggregateFilter f1;
        private final AggregateFilter f2;

        public And(AggregateFilter f1, AggregateFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return h.apply(new And(f1.transform(f,g,h,i, groupByFunction), f2.transform(f,g,h,i, groupByFunction)));
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new And(f1.traverse1(f), f2.traverse1(f));
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            f1.validate(scope, datasetsFields, errorConsumer);
            f2.validate(scope, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return f1.isOrdered() || f2.isOrdered();
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "and", "arg1", f1, "arg2", f2));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            And and = (And) o;
            return Objects.equals(f1, and.f1) &&
                    Objects.equals(f2, and.f2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(f1, f2);
        }

        @Override
        public String toString() {
            return "And{" +
                    "f1=" + f1 +
                    ", f2=" + f2 +
                    '}';
        }
    }

    class Or implements AggregateFilter, JsonSerializable {
        private final AggregateFilter f1;
        private final AggregateFilter f2;

        public Or(AggregateFilter f1, AggregateFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return h.apply(new Or(f1.transform(f, g, h, i, groupByFunction), f2.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Or(f1.traverse1(f), f2.traverse1(f));
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            f1.validate(scope, datasetsFields, errorConsumer);
            f2.validate(scope, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return f1.isOrdered() || f2.isOrdered();
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "or", "arg1", f1, "arg2", f2));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Or or = (Or) o;
            return Objects.equals(f1, or.f1) &&
                    Objects.equals(f2, or.f2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(f1, f2);
        }

        @Override
        public String toString() {
            return "Or{" +
                    "f1=" + f1 +
                    ", f2=" + f2 +
                    '}';
        }
    }

    class Not implements AggregateFilter, JsonSerializable {
        private final AggregateFilter filter;

        public Not(AggregateFilter filter) {
            this.filter = filter;
        }

        @Override
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return h.apply(new Not(filter.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Not(filter.traverse1(f));
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            filter.validate(scope, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return filter.isOrdered();
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "not", "value", filter));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Not not = (Not) o;
            return Objects.equals(filter, not.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filter);
        }

        @Override
        public String toString() {
            return "Not{" +
                    "filter=" + filter +
                    '}';
        }
    }

    class Regex implements AggregateFilter, JsonSerializable {
        private final String field;
        private final String regex;

        public Regex(String field, String regex) {
            this.field = field;
            this.regex = regex;
        }

        @Override
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return h.apply(this);
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            for (final String dataset : scope) {
                if (!datasetsFields.getAllFields(dataset).contains(field)) {
                    errorConsumer.accept(ErrorMessages.missingField(dataset, field, this));
                }
            }
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "regex", "field", field, "value", regex));
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Regex regex1 = (Regex) o;
            return Objects.equals(field, regex1.field) &&
                    Objects.equals(regex, regex1.regex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, regex);
        }

        @Override
        public String toString() {
            return "Regex{" +
                    "field='" + field + '\'' +
                    ", regex='" + regex + '\'' +
                    '}';
        }
    }

    class Always implements AggregateFilter, JsonSerializable {
        @Override
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return h.apply(this);
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {

        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "always"));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public int hashCode() {
            return 1;
        }

        @Override
        public boolean equals(Object obj) {
            return getClass().equals(obj.getClass());
        }

        @Override
        public String toString() {
            return "Always{}";
        }
    }

    class Never implements AggregateFilter, JsonSerializable {
        @Override
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return h.apply(this);
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {

        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "never"));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public int hashCode() {
            return 2;
        }

        @Override
        public boolean equals(Object obj) {
            return getClass().equals(obj.getClass());
        }

        public String toString() {
            return "Never{}";
        }
    }

    class IsDefaultGroup implements AggregateFilter, JsonSerializable {
        @Override
        public AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return h.apply(this);
        }

        @Override
        public AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {

        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "isDefaultGroup"));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public int hashCode() {
            return 101;
        }

        @Override
        public boolean equals(Object obj) {
            return getClass().equals(obj.getClass());
        }


        @Override
        public String toString() {
            return "IsDefaultGroup{}";
        }
    }
}
