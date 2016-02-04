package com.indeed.squall.iql2.language;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.query.GroupBy;
import com.indeed.squall.iql2.language.util.DatasetsFields;
import com.indeed.squall.iql2.language.util.ErrorMessages;
import com.indeed.squall.iql2.language.util.ValidationUtil;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

// TODO: PerGroupConstants, SumChildren, IfThenElse ????
public interface AggregateMetric {

    AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction);
    AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f);
    void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer);
    boolean isOrdered();

    abstract class Unop implements AggregateMetric, JsonSerializable {
        public final AggregateMetric m1;
        private final String jsonType;

        public Unop(AggregateMetric m1, String jsonType) {
            this.m1 = m1;
            this.jsonType = jsonType;
        }

        @Override
        public boolean isOrdered() {
            return m1.isOrdered();
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", jsonType, "value", m1));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
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
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            m1.validate(scope, datasetsFields, errorConsumer);
        }
    }

    class Log extends Unop {
        public Log(AggregateMetric m1) {
            super(m1, "log");
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Log(m1.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Log(f.apply(m1));
        }
    }

    class Negate extends Unop {
        public Negate(AggregateMetric m1) {
            super(m1, "negate");
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Negate(m1.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Negate(f.apply(m1));
        }
    }

    class Abs extends Unop {
        public Abs(AggregateMetric m1) {
            super(m1, "abs");
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Abs(m1.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Abs(f.apply(m1));
        }
    }

    abstract class Binop implements AggregateMetric, JsonSerializable {
        public final AggregateMetric m1;
        public final AggregateMetric m2;
        private final String jsonType;

        public Binop(AggregateMetric m1, AggregateMetric m2, String jsonType) {
            this.m1 = m1;
            this.m2 = m2;
            this.jsonType = jsonType;
        }

        @Override
        public boolean isOrdered() {
            return m1.isOrdered() || m2.isOrdered();
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", jsonType, "m1", m1, "m2", m2));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
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
            return this.getClass().getSimpleName() + "{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            m1.validate(scope, datasetsFields, errorConsumer);
            m2.validate(scope, datasetsFields, errorConsumer);
        }
    }

    class Add extends Binop {
        public Add(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2, "addition");
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Add(m1.transform(f, g, h, i, groupByFunction), m2.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Add(f.apply(m1), f.apply(m2));
        }
   }

    class Subtract extends Binop {
        public Subtract(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2, "subtraction");
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Subtract(m1.transform(f, g, h, i, groupByFunction), m2.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Subtract(f.apply(m1), f.apply(m2));
        }
    }

    class Multiply extends Binop {
        public Multiply(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2, "multiplication");
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Multiply(m1.transform(f, g, h, i, groupByFunction), m2.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Multiply(f.apply(m1), f.apply(m2));
        }
    }

    class Divide extends Binop {
        public Divide(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2, "division");
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Divide(m1.transform(f, g, h, i, groupByFunction), m2.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Divide(f.apply(m1), f.apply(m2));
        }
    }

    class Modulus extends Binop {
        public Modulus(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2, "modulus");
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Modulus(m1.transform(f, g, h, i, groupByFunction), m2.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Modulus(f.apply(m1), f.apply(m2));
        }
    }

    class Power extends Binop {
        public Power(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2, "power");
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Power(m1.transform(f, g, h, i, groupByFunction), m2.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Power(f.apply(m1), f.apply(m2));
        }
    }

    class Parent implements AggregateMetric, JsonSerializable {
        public final AggregateMetric metric;

        public Parent(AggregateMetric metric) {
            this.metric = metric;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Parent(metric.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Parent(f.apply(metric));
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            metric.validate(scope, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return metric.isOrdered();
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            throw new IllegalStateException("Cannot serialize Parent metric");
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Parent parent = (Parent) o;
            return Objects.equals(metric, parent.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(metric);
        }

        @Override
        public String toString() {
            return "Parent{" +
                    "metric=" + metric +
                    '}';
        }
    }

    class Lag implements AggregateMetric, JsonSerializable {
        public final int lag;
        public final AggregateMetric metric;

        public Lag(int lag, AggregateMetric metric) {
            this.lag = lag;
            this.metric = metric;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Lag(lag, metric.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Lag(lag, f.apply(metric));
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            metric.validate(scope, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return true;
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "lag", "delay", lag, "m", metric));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Lag lag1 = (Lag) o;
            return Objects.equals(lag, lag1.lag) &&
                    Objects.equals(metric, lag1.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lag, metric);
        }

        @Override
        public String toString() {
            return "Lag{" +
                    "lag=" + lag +
                    ", metric=" + metric +
                    '}';
        }
    }

    class IterateLag implements AggregateMetric, JsonSerializable {
        public final int lag;
        public final AggregateMetric metric;

        public IterateLag(int lag, AggregateMetric metric) {
            this.lag = lag;
            this.metric = metric;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new IterateLag(lag, metric.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new IterateLag(lag, f.apply(metric));
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            metric.validate(scope, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return true;
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "iterateLag", "delay", lag, "m", metric));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IterateLag that = (IterateLag) o;
            return Objects.equals(lag, that.lag) &&
                    Objects.equals(metric, that.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lag, metric);
        }

        @Override
        public String toString() {
            return "IterateLag{" +
                    "lag=" + lag +
                    ", metric=" + metric +
                    '}';
        }
    }

    class Window implements AggregateMetric, JsonSerializable {
        public final int window;
        public final AggregateMetric metric;

        public Window(int window, AggregateMetric metric) {
            this.window = window;
            this.metric = metric;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Window(window, metric.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Window(window, f.apply(metric));
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            metric.validate(scope, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return true;
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "window", "size", window, "value", metric));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Window window1 = (Window) o;
            return Objects.equals(window, window1.window) &&
                    Objects.equals(metric, window1.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(window, metric);
        }

        @Override
        public String toString() {
            return "Window{" +
                    "window=" + window +
                    ", metric=" + metric +
                    '}';
        }
    }

    class Qualified implements AggregateMetric, JsonSerializable {
        public final List<String> scope;
        public final AggregateMetric metric;

        public Qualified(List<String> scope, AggregateMetric metric) {
            this.scope = scope;
            this.metric = metric;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Qualified(scope, metric.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Qualified(scope, f.apply(metric));
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            metric.validate(Sets.newHashSet(this.scope), datasetsFields, errorConsumer);
            ValidationUtil.validateScope(this.scope, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return metric.isOrdered();
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            throw new UnsupportedOperationException("Cannot / should not serialize Qualified metrics -- ExtractPrecomputed should remove them!");
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Qualified qualified = (Qualified) o;
            return Objects.equals(scope, qualified.scope) &&
                    Objects.equals(metric, qualified.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(scope, metric);
        }

        @Override
        public String toString() {
            return "Qualified{" +
                    "scope=" + scope +
                    ", metric=" + metric +
                    '}';
        }
    }

    class DocStatsPushes implements AggregateMetric, JsonSerializable {
        public final String dataset;
        public final DocMetric.PushableDocMetric pushes;

        public DocStatsPushes(String dataset, DocMetric.PushableDocMetric pushes) {
            this.dataset = dataset;
            this.pushes = pushes;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            pushes.validate(dataset, datasetsFields, errorConsumer);
            ValidationUtil.validateDataset(dataset, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "docStats", "pushes", pushes.getPushes(dataset), "sessionName", dataset));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DocStatsPushes that = (DocStatsPushes) o;
            return Objects.equals(dataset, that.dataset) &&
                    Objects.equals(pushes, that.pushes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataset, pushes);
        }

        @Override
        public String toString() {
            return "DocStatsPushes{" +
                    "dataset='" + dataset + '\'' +
                    ", pushes=" + pushes +
                    '}';
        }
    }

    class DocStats implements AggregateMetric, JsonSerializable {
        public final DocMetric metric;

        public DocStats(DocMetric metric) {
            this.metric = metric;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new DocStats(metric.transform(g, i)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            for (final String dataset : scope) {
                metric.validate(dataset, datasetsFields, errorConsumer);
            }
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            throw new UnsupportedOperationException("Cannot / should not serialize raw DocStats metrics -- ExtractPrecomputed should transform them into DocStatsPushes!");
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DocStats docStats = (DocStats) o;
            return Objects.equals(metric, docStats.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(metric);
        }

        @Override
        public String toString() {
            return "DocStats{" +
                    "metric=" + metric +
                    '}';
        }
    }

    /**
     * DocStats in which there is no explicit sum, but a single atomic, unambiguous atom.
     */
    class ImplicitDocStats implements AggregateMetric, JsonSerializable {
        public final DocMetric docMetric;

        public ImplicitDocStats(DocMetric docMetric) {
            this.docMetric = docMetric;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new ImplicitDocStats(g.apply(docMetric)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            for (final String dataset : scope) {
                docMetric.validate(dataset, datasetsFields, errorConsumer);
            }
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            throw new UnsupportedOperationException("Cannot / should not serialize raw ImplicitDocStats metrics -- ExtractPrecomputed should transform them into DocStatsPushes!");
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ImplicitDocStats that = (ImplicitDocStats) o;
            return Objects.equals(docMetric, that.docMetric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(docMetric);
        }

        @Override
        public String toString() {
            return "ImplicitDocStats{" +
                    "docMetric=" + docMetric +
                    '}';
        }
    }

    class Constant implements AggregateMetric, JsonSerializable {
        public final double value;

        public Constant(double value) {
            this.value = value;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
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
            gen.writeObject(ImmutableMap.of("type", "constant", "value", value));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
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

    class Percentile implements AggregateMetric, JsonSerializable {
        public final String field;
        public final double percentile;

        public Percentile(String field, double percentile) {
            this.field = field;
            this.percentile = percentile;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            for (final String dataset : scope) {
                if (!datasetsFields.getIntFields(dataset).contains(field)) {
                    errorConsumer.accept(ErrorMessages.missingIntField(dataset, field, this));
                }
            }
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            throw new UnsupportedOperationException("Cannot / should not serialize raw Percentile metrics -- ExtractPrecomputed should remove them!");
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Percentile that = (Percentile) o;
            return Objects.equals(percentile, that.percentile) &&
                    Objects.equals(field, that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, percentile);
        }

        @Override
        public String toString() {
            return "Percentile{" +
                    "field='" + field + '\'' +
                    ", percentile=" + percentile +
                    '}';
        }
    }

    class Running implements AggregateMetric, JsonSerializable {
        public final int offset;
        public final AggregateMetric metric;

        public Running(int offset, AggregateMetric metric) {
            this.offset = offset;
            this.metric = metric;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Running(offset, metric.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Running(offset, f.apply(metric));
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            metric.validate(scope, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return true;
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "running", "offset", offset, "value", metric));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Running running = (Running) o;
            return Objects.equals(metric, running.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(metric);
        }

        @Override
        public String toString() {
            return "Running{" +
                    "metric=" + metric +
                    '}';
        }
    }

    class Distinct implements AggregateMetric, JsonSerializable {
        public final String field;
        public final Optional<AggregateFilter> filter;
        public final Optional<Integer> windowSize;

        public Distinct(String field, Optional<AggregateFilter> filter, Optional<Integer> windowSize) {
            this.field = field;
            this.filter = filter;
            this.windowSize = windowSize;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            if (filter.isPresent()) {
                return f.apply(new Distinct(field, Optional.of(filter.get().transform(f, g, h, i, groupByFunction)), windowSize));
            } else {
                return f.apply(this);
            }
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            if (filter.isPresent()) {
                return new Distinct(field, Optional.of(filter.get().traverse1(f)), windowSize);
            } else {
                return this;
            }
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            for (final String dataset : scope) {
                if (!datasetsFields.getAllFields(dataset).contains(field)) {
                    errorConsumer.accept(ErrorMessages.missingField(dataset, field, this));
                }
            }

            if (filter.isPresent()) {
                filter.get().validate(scope, datasetsFields, errorConsumer);
            }
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            throw new UnsupportedOperationException("Cannot / should not serialize raw Distinct metrics -- ExtractPrecomputed should remove them!");
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Distinct distinct = (Distinct) o;
            return Objects.equals(field, distinct.field) &&
                    Objects.equals(filter, distinct.filter) &&
                    Objects.equals(windowSize, distinct.windowSize);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, filter, windowSize);
        }

        @Override
        public String toString() {
            return "Distinct{" +
                    "field='" + field + '\'' +
                    ", filter=" + filter +
                    ", windowSize=" + windowSize +
                    '}';
        }
    }

    class Named implements AggregateMetric, JsonSerializable {
        public final AggregateMetric metric;
        public final String name;

        public Named(AggregateMetric metric, String name) {
            this.metric = metric;
            this.name = name;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Named(metric.transform(f, g, h, i, groupByFunction), name));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Named(f.apply(metric), name);
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            metric.validate(scope, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return metric.isOrdered();
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            throw new UnsupportedOperationException("Cannot / should not serialize Named metrics -- RemoveNames should have removed them!");
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Named named = (Named) o;
            return Objects.equals(metric, named.metric) &&
                    Objects.equals(name, named.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(metric, name);
        }

        @Override
        public String toString() {
            return "Named{" +
                    "metric=" + metric +
                    ", name='" + name + '\'' +
                    '}';
        }
    }

    class GroupStatsLookup implements AggregateMetric, JsonSerializable {
        public final String name;

        public GroupStatsLookup(String name) {
            this.name = name;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
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
            gen.writeObject(ImmutableMap.of("type", "groupStatsLookup", "name", name));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GroupStatsLookup that = (GroupStatsLookup) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public String toString() {
            return "GroupStatsLookup{" +
                    "name='" + name + '\'' +
                    '}';
        }
    }

    class SumAcross implements AggregateMetric, JsonSerializable {
        public final GroupBy groupBy;
        public final AggregateMetric metric;

        public SumAcross(GroupBy groupBy, AggregateMetric metric) {
            this.groupBy = groupBy;
            this.metric = metric;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new SumAcross(groupBy.transform(groupByFunction, f, g, h, i), metric.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new SumAcross(groupBy.traverse1(f), f.apply(metric));
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            // TODO: Validate groupBy somehow?
            metric.validate(scope, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            throw new UnsupportedOperationException("Cannot serialize SumAcross -- should be removed by ExtractPrecomputed!");
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SumAcross sumAcross = (SumAcross) o;
            return Objects.equals(groupBy, sumAcross.groupBy) &&
                    Objects.equals(metric, sumAcross.metric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(groupBy, metric);
        }


        @Override
        public String toString() {
            return "SumAcross{" +
                    "groupBy=" + groupBy +
                    ", metric=" + metric +
                    '}';
        }
    }

    class IfThenElse implements AggregateMetric, JsonSerializable {
        public final AggregateFilter condition;
        public final AggregateMetric trueCase;
        public final AggregateMetric falseCase;

        public IfThenElse(AggregateFilter condition, AggregateMetric trueCase, AggregateMetric falseCase) {
            this.condition = condition;
            this.trueCase = trueCase;
            this.falseCase = falseCase;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(
                    new IfThenElse(
                            condition.transform(f, g, h, i, groupByFunction),
                            trueCase.transform(f, g, h, i, groupByFunction),
                            falseCase.transform(f, g, h, i, groupByFunction)
                    )
            );
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new IfThenElse(condition.traverse1(f), f.apply(trueCase), f.apply(falseCase));
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            condition.validate(scope, datasetsFields, errorConsumer);
            trueCase.validate(scope, datasetsFields, errorConsumer);
            falseCase.validate(scope, datasetsFields, errorConsumer);
        }

        @Override
        public boolean isOrdered() {
            return condition.isOrdered() || trueCase.isOrdered() || falseCase.isOrdered();
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "ifThenElse", "condition", condition, "trueCase", trueCase, "falseCase", falseCase));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
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
    
    class FieldMin implements AggregateMetric, JsonSerializable {
        public final String field;

        public FieldMin(String field) {
            this.field = field;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
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
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            throw new UnsupportedOperationException("Cannot serialize FieldMin -- should be removed by ExtractPrecomputed!");
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FieldMin fieldMin = (FieldMin) o;
            return Objects.equals(field, fieldMin.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field);
        }

        @Override
        public String toString() {
            return "FieldMin{" +
                    "field='" + field + '\'' +
                    '}';
        }
    }
    
    class FieldMax implements AggregateMetric, JsonSerializable {
        public final String field;

        public FieldMax(String field) {
            this.field = field;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
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
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            throw new UnsupportedOperationException("Cannot serialize FieldMax -- should be removed by ExtractPrecomputed!");
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FieldMax fieldMax = (FieldMax) o;
            return Objects.equals(field, fieldMax.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field);
        }

        @Override
        public String toString() {
            return "FieldMax{" +
                    "field='" + field + '\'' +
                    '}';
        }
    }

    class Min implements AggregateMetric, JsonSerializable {
        private final List<AggregateMetric> metrics;

        public Min(List<AggregateMetric> metrics) {
            this.metrics = metrics;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            final List<AggregateMetric> newMetrics = Lists.newArrayListWithCapacity(metrics.size());
            for (final AggregateMetric metric : metrics) {
                newMetrics.add(metric.transform(f, g, h, i, groupByFunction));
            }
            return f.apply(new Min(newMetrics));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            final List<AggregateMetric> newMetrics = Lists.newArrayListWithCapacity(metrics.size());
            for (final AggregateMetric metric : metrics) {
                newMetrics.add(f.apply(metric));
            }
            return new Min(newMetrics);
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            for (final AggregateMetric metric : metrics) {
                metric.validate(scope, datasetsFields, errorConsumer);
            }
        }

        @Override
        public boolean isOrdered() {
            boolean isOrdered = false;
            for (final AggregateMetric metric : metrics) {
                isOrdered |= metric.isOrdered();
            }
            return isOrdered;
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "min", "metrics", metrics));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Min min = (Min) o;
            return Objects.equals(metrics, min.metrics);
        }

        @Override
        public int hashCode() {
            return Objects.hash(metrics);
        }

        @Override
        public String toString() {
            return "Min{" +
                    "metrics=" + metrics +
                    '}';
        }
    }

    class Max implements AggregateMetric, JsonSerializable {
        private final List<AggregateMetric> metrics;

        public Max(List<AggregateMetric> metrics) {
            this.metrics = metrics;
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            final List<AggregateMetric> newMetrics = Lists.newArrayListWithCapacity(metrics.size());
            for (final AggregateMetric metric : metrics) {
                newMetrics.add(metric.transform(f, g, h, i, groupByFunction));
            }
            return f.apply(new Max(newMetrics));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            final List<AggregateMetric> newMetrics = Lists.newArrayListWithCapacity(metrics.size());
            for (final AggregateMetric metric : metrics) {
                newMetrics.add(f.apply(metric));
            }
            return new Max(newMetrics);
        }

        @Override
        public void validate(Set<String> scope, DatasetsFields datasetsFields, Consumer<String> errorConsumer) {
            for (final AggregateMetric metric : metrics) {
                metric.validate(scope, datasetsFields, errorConsumer);
            }
        }

        @Override
        public boolean isOrdered() {
            boolean isOrdered = false;
            for (final AggregateMetric metric : metrics) {
                isOrdered |= metric.isOrdered();
            }
            return isOrdered;
        }

        @Override
        public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeObject(ImmutableMap.of("type", "max", "metrics", metrics));
        }

        @Override
        public void serializeWithType(JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            this.serialize(gen, serializers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Max max = (Max) o;
            return Objects.equals(metrics, max.metrics);
        }

        @Override
        public int hashCode() {
            return Objects.hash(metrics);
        }

        @Override
        public String toString() {
            return "Max{" +
                    "metrics=" + metrics +
                    '}';
        }
    }
}
