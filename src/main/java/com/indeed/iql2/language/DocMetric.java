/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.iql2.language;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.marshal.ImhotepClientMarshaller;
import com.indeed.imhotep.protobuf.QueryMessage;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.query.fieldresolution.ScopedFieldResolver;
import com.indeed.iql2.language.util.ErrorMessages;
import com.indeed.iql2.language.util.ParserUtil;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;
import com.indeed.iql2.server.web.servlets.query.CommandValidator;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.codec.binary.Base64;

import javax.annotation.Nullable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.indeed.iql2.language.DocMetrics.hasTermMetricOrThrow;
import static com.indeed.iql2.language.DocMetrics.negateMetric;

public abstract class DocMetric extends AbstractPositional {
    public interface Visitor<T, E extends Throwable> {
        T visit(Log log) throws E;
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
        T visit(Sample random) throws E;
        T visit(SampleMetric random) throws E;
        T visit(Random random) throws E;
        T visit(RandomMetric random) throws E;
    }

    /**
     * @see com.indeed.iql2.language.query.Query#transform(Function, Function, Function, Function, Function)
     */
    public abstract DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i);

    public abstract List<String> getPushes(String dataset);

    public abstract <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E;

    public abstract void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector);

    @Override
    public abstract boolean equals(final Object other);
    @Override
    public abstract int hashCode();
    @Override
    public abstract String toString();

    @Override
    public DocMetric copyPosition(Positional positional) {
        super.copyPosition(positional);
        return this;
    }

    // Metric that result only 0 or 1, so it could be converted to filter
    public interface ZeroOneMetric {
        // convert to a corresponding filter
        DocFilter convertToFilter();

        // invert if possible
        @Nullable
        DocMetric invert();
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class PerDatasetDocMetric extends DocMetric {
        public final ImmutableMap<String, DocMetric> datasetToMetric;

        public PerDatasetDocMetric(final ImmutableMap<String, DocMetric> datasetToMetric) {
            this.datasetToMetric = datasetToMetric;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new PerDatasetDocMetric(ImmutableMap.copyOf(Maps.transformValues(datasetToMetric, d -> d.transform(g, i)))))
                    .copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            if (!datasetToMetric.containsKey(dataset)) {
                throw new IqlKnownException.UnknownDatasetException("Unknown dataset: " + dataset + " in [" + this + "]");
            } else {
                return datasetToMetric.get(dataset).getPushes(dataset);
            }
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            if (!datasetToMetric.containsKey(dataset)) {
                throw new IqlKnownException.UnknownDatasetException("Unknown dataset: " + dataset + " in [" + this + "]");
            } else {
                datasetToMetric.get(dataset).validate(dataset, validationHelper, errorCollector);
            }
        }
    }

    public static class Count extends DocMetric {
        public Count() {
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Count()).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return Collections.singletonList("count()");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
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
            return g.apply(new DocId()).copyPosition(this);
        }

        @Override
        public List<String> getPushes(final String dataset) {
            return Collections.singletonList("docId()");
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(final String dataset,
                             final ValidationHelper validationHelper,
                             final ErrorCollector errorCollector) {
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

    @ToString
    @EqualsAndHashCode(callSuper = false)
    public static class Field extends DocMetric {
        public final FieldSet field;

        public Field(final FieldSet field) {
            this.field = field;
            copyPosition(field);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Field(field)).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return Collections.singletonList(field.datasetFieldName(dataset));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            final String fieldName = field.datasetFieldName(dataset);
            if (!validationHelper.containsField(dataset, fieldName)) {
                errorCollector.error(ErrorMessages.missingField(dataset, fieldName, this));
            }
        }
    }

    @EqualsAndHashCode(callSuper = false)
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
        public String toString() {
            return this.getClass().getSimpleName() + "{" +
                    "m1=" + m1 +
                    '}';
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            m1.validate(dataset, validationHelper, errorCollector);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Log extends DocMetric {
        public final DocMetric metric;
        public final int scaleFactor;

        public Log(final DocMetric metric, final int scaleFactor) {
            this.metric = metric;
            this.scaleFactor = scaleFactor;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Log(metric.transform(g, i), scaleFactor))
                    .copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            final List<String> result = new ArrayList<>(metric.getPushes(dataset));
            result.add("log " + scaleFactor);
            return result;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            metric.validate(dataset, validationHelper, errorCollector);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Exponentiate extends DocMetric {
        public final DocMetric metric;
        public final int scaleFactor;

        public Exponentiate(final DocMetric metric, final int scaleFactor) {
            this.metric = metric;
            this.scaleFactor = scaleFactor;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Exponentiate(metric.transform(g, i), scaleFactor)).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            final List<String> result = new ArrayList<>(metric.getPushes(dataset));
            result.add("exp " + scaleFactor);
            return result;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            metric.validate(dataset, validationHelper, errorCollector);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class Negate extends Unop {
        public Negate(DocMetric m1) {
            super(m1);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Negate(m1.transform(g, i))).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return new Subtract(new Constant(0), m1).getPushes(dataset);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class Abs extends Unop {
        public Abs(DocMetric m1) {
            super(m1);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Abs(m1.transform(g, i))).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return unop(dataset, "abs()");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class Signum extends Unop {
        public Signum(DocMetric m1) {
            super(m1);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Signum(m1.transform(g, i))).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            final IfThenElse m = new IfThenElse(new DocFilter.MetricGt(m1, new Constant(0)),
                    new Constant(1),
                    new IfThenElse(new DocFilter.MetricLt(m1, new Constant(0)),
                            new Constant(-1),
                            new Constant(0))
            );
            return m.getPushes(dataset);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    @EqualsAndHashCode(callSuper = false)
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
        public String toString() {
            return getClass().getSimpleName() + "{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            m1.validate(dataset, validationHelper, errorCollector);
            m2.validate(dataset, validationHelper, errorCollector);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    public abstract static class Multiary extends DocMetric {
        public final List<DocMetric> metrics;
        public final String combinePush;

        private Multiary(final List<DocMetric> metrics, final String combinePush) {
            this.metrics = metrics;
            this.combinePush = combinePush;
        }

        public abstract DocMetric createMetric(final List<DocMetric> metrics);

        @Override
        public DocMetric transform(
                final Function<DocMetric, DocMetric> g,
                final Function<DocFilter, DocFilter> i) {
            final List<DocMetric> transformed = new ArrayList<>(metrics.size());
            for (final DocMetric metric : metrics) {
                transformed.add(metric.transform(g, i));
            }
            return g.apply(createMetric(transformed)).copyPosition(this);
        }

        @Override
        public List<String> getPushes(final String dataset) {
            final List<String> result = new ArrayList<>();
            for (int i = 0; i < metrics.size(); i++) {
                result.addAll(metrics.get(i).getPushes(dataset));
                if (i > 0) {
                    result.add(combinePush);
                }
            }
            return result;
        }

        @Override
        public void validate(
                final String dataset,
                final ValidationHelper validationHelper,
                final ErrorCollector errorCollector) {
            metrics.forEach(m -> m.validate(dataset, validationHelper, errorCollector));
        }

        @Override
        public final String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append(getClass().getSimpleName()).append('{');
            for (int i = 0; i < metrics.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append('m').append(i+1).append('=').append(metrics.get(i));
            }
            sb.append('}');
            return sb.toString();
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class Add extends Multiary {

        private Add(final List<DocMetric> metrics) {
            super(metrics, "+");
        }

        @Override
        public DocMetric createMetric(final List<DocMetric> metrics) {
            return create(metrics);
        }

        // create filter that is equivalent to '+' of all metrics and simplify it.
        public static DocMetric create(final List<DocMetric> original) {
            // unwrapping another Add if present.
            final List<DocMetric> unwraped = new ArrayList<>(original.size());
            for (final DocMetric metric : original) {
                if (metric instanceof Add) {
                    unwraped.addAll(((Multiary) metric).metrics);
                } else {
                    unwraped.add(metric);
                }
            }
            final List<DocMetric> metrics = new ArrayList<>(unwraped.size());
            // iterating throw metrics and gathering all constants into one.
            long constant = 0;
            for (final DocMetric metric : unwraped) {
                if (metric instanceof Constant) {
                    constant += ((Constant)metric).value;
                } else if (metric instanceof Count) {
                    constant++;
                } else {
                    metrics.add(metric);
                }
            }
            if (constant != 0) {
                metrics.add(new Constant(constant));
            }
            if (metrics.isEmpty()) {
                return new Constant(0);
            }
            if (metrics.size() == 1) {
                return metrics.get(0);
            }
            return new Add(metrics);
        }

        public static DocMetric create(final DocMetric m1, final DocMetric m2) {
            return create(ImmutableList.of(m1, m2));
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class Subtract extends Binop {
        public Subtract(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Subtract(m1.transform(g, i), m2.transform(g, i)))
                    .copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "-");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class Multiply extends Binop {
        public Multiply(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Multiply(m1.transform(g, i), m2.transform(g, i))).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "*");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class Divide extends Binop {
        public Divide(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Divide(m1.transform(g, i), m2.transform(g, i))).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "/");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class Modulus extends Binop {
        public Modulus(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Modulus(m1.transform(g, i), m2.transform(g, i))).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "%");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class Min extends Multiary {
        private Min(final List<DocMetric> metrics) {
            super(metrics, "min()");
        }

        @Override
        public DocMetric createMetric(final List<DocMetric> metrics) {
            return create(metrics);
        }

        // create filter that is equivalent to 'min' of all metrics and simplify it.
        public static DocMetric create(final List<DocMetric> original) {
            // unwrapping another Min if present.
            final List<DocMetric> unwraped = new ArrayList<>(original.size());
            for (final DocMetric metric : original) {
                if (metric instanceof Min) {
                    unwraped.addAll(((Multiary) metric).metrics);
                } else {
                    unwraped.add(metric);
                }
            }
            final List<DocMetric> metrics = new ArrayList<>(unwraped.size());
            // iterating throw metrics and replace all constants with min value.
            Long min = null;
            for (final DocMetric metric : unwraped) {
                if (metric instanceof Constant) {
                    final long value =((Constant)metric).value;
                    min = (min == null) ? value : Math.min(min, value);
                } else if (metric instanceof Count) {
                    min = (min == null) ? 1 : Math.min(min, 1);
                } else {
                    metrics.add(metric);
                }
            }
            if (min != null) {
                metrics.add(new Constant(min));
            }
            if (metrics.isEmpty()) {
                throw new IllegalStateException("Should not be here!");
            }
            if (metrics.size() == 1) {
                return metrics.get(0);
            }
            return new Min(metrics);
        }

        public static DocMetric create(final DocMetric m1, final DocMetric m2) {
            return create(ImmutableList.of(m1, m2));
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class Max extends Multiary {
        private Max(final List<DocMetric> metrics) {
            super(metrics, "max()");
        }

        @Override
        public DocMetric createMetric(final List<DocMetric> metrics) {
            return create(metrics);
        }

        // create filter that is equivalent to 'max' of all metrics and simplify it.
        public static DocMetric create(final List<DocMetric> original) {
            // unwrapping another Max if present.
            final List<DocMetric> unwraped = new ArrayList<>(original.size());
            for (final DocMetric metric : original) {
                if (metric instanceof Max) {
                    unwraped.addAll(((Multiary) metric).metrics);
                } else {
                    unwraped.add(metric);
                }
            }
            final List<DocMetric> metrics = new ArrayList<>(unwraped.size());
            // iterating throw metrics and replace all constants with max value.
            Long max = null;
            for (final DocMetric metric : unwraped) {
                if (metric instanceof Constant) {
                    final long value =((Constant)metric).value;
                    max = (max == null) ? value : Math.max(max, value);
                } else if (metric instanceof Count) {
                    max = (max == null) ? 1 : Math.max(max, 1);
                } else {
                    metrics.add(metric);
                }
            }
            if (max != null) {
                metrics.add(new Constant(max));
            }
            if (metrics.isEmpty()) {
                throw new IllegalStateException("Should not be here!");
            }
            if (metrics.size() == 1) {
                return metrics.get(0);
            }
            return new Max(metrics);
        }

        public static DocMetric create(final DocMetric m1, final DocMetric m2) {
            return create(ImmutableList.of(m1, m2));
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class MetricEqual extends Binop implements ZeroOneMetric {
        private MetricEqual(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        public static DocMetric create(final DocMetric left, final DocMetric right) {
            final FieldSet field = DocFilters.extractPlainField(left);
            if ((field != null) && (right instanceof DocMetric.Constant)) {
                final String rawInput = right.getRawInput();
                final Term term = (rawInput != null) ? Term.term(rawInput) : Term.term(((Constant) right).value);
                return hasTermMetricOrThrow(field, term);
            } else {
                return new DocMetric.MetricEqual(left, right);
            }
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new MetricEqual(m1.transform(g, i), m2.transform(g, i))).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "=");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public DocFilter convertToFilter() {
            return new DocFilter.MetricEqual(m1, m2);
        }

        @Override
        public DocMetric invert() {
            return MetricNotEqual.create(m1, m2);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class MetricNotEqual extends Binop implements ZeroOneMetric {
        private MetricNotEqual(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        public static DocMetric create(final DocMetric left, final DocMetric right) {
            final FieldSet field = DocFilters.extractPlainField(left);
            if ((field != null) && (right instanceof DocMetric.Constant)) {
                final String rawInput = right.getRawInput();
                final Term term = (rawInput != null) ? Term.term(rawInput) : Term.term(((Constant) right).value);
                return negateMetric(hasTermMetricOrThrow(field, term));
            } else {
                return new DocMetric.MetricNotEqual(left, right);
            }
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new MetricNotEqual(m1.transform(g, i), m2.transform(g, i))).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "!=");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public DocFilter convertToFilter() {
            return new DocFilter.MetricNotEqual(m1, m2);
        }

        @Override
        public DocMetric invert() {
            return MetricEqual.create(m1, m2);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class MetricLt extends Binop implements ZeroOneMetric {
        public MetricLt(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new MetricLt(m1.transform(g, i), m2.transform(g, i))).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "<");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public DocFilter convertToFilter() {
            return new DocFilter.MetricLt(m1, m2);
        }

        @Override
        public DocMetric invert() {
            return new MetricGte(m1, m2);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class MetricLte extends Binop implements ZeroOneMetric {
        public MetricLte(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new MetricLte(m1.transform(g, i), m2.transform(g, i))).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, "<=");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public DocFilter convertToFilter() {
            return new DocFilter.MetricLte(m1, m2);
        }

        @Override
        public DocMetric invert() {
            return new MetricGt(m1, m2);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class MetricGt extends Binop implements ZeroOneMetric {
        public MetricGt(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new MetricGt(m1.transform(g, i), m2.transform(g, i))).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, ">");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public DocFilter convertToFilter() {
            return new DocFilter.MetricGt(m1, m2);
        }

        @Override
        public DocMetric invert() {
            return new MetricLte(m1, m2);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class MetricGte extends Binop implements ZeroOneMetric {
        public MetricGte(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new MetricGte(m1.transform(g, i), m2.transform(g, i))).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return binop(dataset, ">=");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public DocFilter convertToFilter() {
            return new DocFilter.MetricGte(m1, m2);
        }

        @Override
        public DocMetric invert() {
            return new MetricLt(m1, m2);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class RegexMetric extends DocMetric {
        public final FieldSet field;
        public final String regex;

        public RegexMetric(final FieldSet field, final String regex) {
            this.field = field;
            this.regex = regex;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new RegexMetric(field, regex)).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return Collections.singletonList("regex " + field.datasetFieldName(dataset) + ":" + regex);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            ValidationUtil.compileRegex(regex);
            final String fieldName = field.datasetFieldName(dataset);
            if (!validationHelper.containsStringField(dataset, fieldName)) {
                errorCollector.error(ErrorMessages.missingStringField(dataset, fieldName, this));
            }
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class FieldEqualMetric extends DocMetric implements ZeroOneMetric {
        public final FieldSet field1;
        public final FieldSet field2;

        public FieldEqualMetric(final FieldSet field1, final FieldSet field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new FieldEqualMetric(field1, field2)).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return Collections.singletonList("fieldequal " + field1.datasetFieldName(dataset) + "=" + field2.datasetFieldName(dataset));
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            ValidationUtil.validateExistenceAndSameFieldType(dataset, field1.datasetFieldName(dataset), field2.datasetFieldName(dataset), validationHelper, errorCollector);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public DocFilter convertToFilter() {
            return new DocFilter.FieldEqual(field1, field2);
        }

        @Override
        public DocMetric invert() {
            return null;
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class FloatScale extends DocMetric {
        public final FieldSet field;
        public final double mult;
        public final double add;

        public FloatScale(final FieldSet field, final double mult, final double add) {
            this.field = field;
            this.mult = mult;
            this.add = add;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new FloatScale(field, mult, add)).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return Collections.singletonList("floatscale " + field.datasetFieldName(dataset) + "*" + mult + "+" + add);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            final String fieldName = field.datasetFieldName(dataset);
            if (!validationHelper.containsStringField(dataset, fieldName)) {
                errorCollector.error(ErrorMessages.missingStringField(dataset, fieldName, this));
            }
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Constant extends DocMetric {
        public final long value;

        public Constant(final long value) {
            this.value = value;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Constant(value)).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return Collections.singletonList(String.valueOf(value));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {

        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class HasIntField extends DocMetric {
        public final FieldSet field;

        public HasIntField(final FieldSet field) {
            this.field = field;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new HasIntField(field)).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return Collections.singletonList("hasintfield " + field.datasetFieldName(dataset));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            // Don't validate, since this is used for investigating field presence
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class HasStringField extends DocMetric {
        public final FieldSet field;

        public HasStringField(final FieldSet field) {
            this.field = field;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new HasStringField(field)).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return Collections.singletonList("hasstrfield " + field.datasetFieldName(dataset));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            // Don't validate, since this is used for investigating field presence
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class HasInt extends DocMetric implements ZeroOneMetric {
        public final FieldSet field;
        public final long term;

        public HasInt(final FieldSet field, final long term) {
            this.field = field;
            this.term = term;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new HasInt(field, term)).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return Collections.singletonList("hasint " + field.datasetFieldName(dataset) + ":" + term);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            final String fieldName = field.datasetFieldName(dataset);
            validationHelper.validateIntField(dataset, fieldName, errorCollector, this);
        }

        @Override
        public DocFilter convertToFilter() {
            return DocFilter.FieldIs.create(field, Term.term(term));
        }

        @Nullable
        @Override
        public DocMetric invert() {
            return null;
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class HasString extends DocMetric implements ZeroOneMetric {
        public final FieldSet field;
        public final String term;

        public HasString(final FieldSet field, final String term) {
            Preconditions.checkState(!field.isIntField());
            this.field = field;
            this.term = term;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new HasString(field, term)).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return Collections.singletonList("hasstr " + field.datasetFieldName(dataset) + ":" + term);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            final String fieldName = field.datasetFieldName(dataset);
            if (!validationHelper.containsStringField(dataset, fieldName)) {
                errorCollector.error(ErrorMessages.missingStringField(dataset, fieldName, this));
            }
        }

        @Override
        public DocFilter convertToFilter() {
            return DocFilter.FieldIs.create(field, Term.term(term));
        }

        @Nullable
        @Override
        public DocMetric invert() {
            return null;
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class IfThenElse extends DocMetric {
        public final DocFilter condition;
        public final DocMetric trueCase;
        public final DocMetric falseCase;

        public IfThenElse(final DocFilter condition, final DocMetric trueCase, final DocMetric falseCase) {
            this.condition = condition;
            this.trueCase = trueCase;
            this.falseCase = falseCase;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new IfThenElse(condition.transform(g, i), trueCase.transform(g, i), falseCase.transform(g, i)))
                    .copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            final DocMetric truth = condition.asZeroOneMetric(dataset);
            final DocMetric trueOrZero = new Multiply(truth, trueCase);
            final DocMetric falseOrZero = new Multiply(new Subtract(new Constant(1), truth), falseCase);
            return Add.create(trueOrZero, falseOrZero).getPushes(dataset);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            condition.validate(dataset, validationHelper, errorCollector);
            trueCase.validate(dataset, validationHelper, errorCollector);
            falseCase.validate(dataset, validationHelper, errorCollector);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Qualified extends DocMetric {
        public final String dataset;
        public final DocMetric metric;

        public Qualified(final String dataset, final DocMetric metric) {
            this.dataset = dataset;
            this.metric = metric;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Qualified(dataset, metric.transform(g, i)))
                    .copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            if (!dataset.equals(this.dataset)) {
                throw new IqlKnownException.ParseErrorException("Qualified DocMetric getting pushes for a different dataset! [" + this.dataset + "] != [" + dataset + "]");
            }
            return metric.getPushes(dataset);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            if (!dataset.equals(this.dataset)) {
                errorCollector.error("Qualified DocMetric getting validated against different dataset! [" + this.dataset + "] != [" + dataset + "]");
            }
            metric.validate(this.dataset, validationHelper, errorCollector);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Extract extends DocMetric {
        public final FieldSet field;
        public final String regex;
        public final int groupNumber;

        public Extract(final FieldSet field, final String regex, final int groupNumber) {
            this.field = field;
            this.regex = regex;
            this.groupNumber = groupNumber;
        }

        @Override
        public DocMetric transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return g.apply(new Extract(field, regex, groupNumber)).copyPosition(this);
        }

        @Override
        public List<String> getPushes(String dataset) {
            return Collections.singletonList("regexmatch " + field.datasetFieldName(dataset) + " " + groupNumber + " " + regex);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            try {
                Pattern.compile(regex);
            } catch (PatternSyntaxException e) {
                errorCollector.error("Invalid pattern: " + regex);
            }
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Lucene extends DocMetric {
        public final String query;

        @EqualsAndHashCode.Exclude
        @ToString.Exclude
        public final DatasetsMetadata datasetsMetadata;

        @EqualsAndHashCode.Exclude
        @ToString.Exclude
        public final ScopedFieldResolver fieldResolver;

        public Lucene(final String query, final DatasetsMetadata datasetsMetadata, final ScopedFieldResolver fieldResolver) {
            this.query = query;
            this.datasetsMetadata = datasetsMetadata;
            this.fieldResolver = fieldResolver;
        }

        @Override
        public DocMetric transform(final Function<DocMetric, DocMetric> g, final Function<DocFilter, DocFilter> i) {
            return g.apply(new Lucene(query, datasetsMetadata, fieldResolver)).copyPosition(this);
        }

        @Override
        public List<String> getPushes(final String dataset) {
            final Query flamdexQuery = ParserUtil.getFlamdexQuery(
                    query, dataset, datasetsMetadata, fieldResolver);
            final QueryMessage luceneQueryMessage = ImhotepClientMarshaller.marshal(flamdexQuery);
            final String base64EncodedQuery = Base64.encodeBase64String(luceneQueryMessage.toByteArray());
            return Lists.newArrayList("lucene " + base64EncodedQuery);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(final String dataset, final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
            final Query flamdexQuery = ParserUtil.getFlamdexQuery(
                    query, dataset, datasetsMetadata, fieldResolver);
            ValidationUtil.validateQuery(validationHelper, ImmutableMap.of(dataset, flamdexQuery), errorCollector, this);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class StringLen extends DocMetric {
        public final FieldSet field;

        public StringLen(final FieldSet field) {
            this.field = field;
        }

        @Override
        public DocMetric transform(final Function<DocMetric, DocMetric> g, final Function<DocFilter, DocFilter> i) {
            return g.apply(new StringLen(field)).copyPosition(this);
        }

        @Override
        public List<String> getPushes(final String dataset) {
            return Collections.singletonList("len " + field.datasetFieldName(dataset));
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(final String dataset, final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
            final String fieldName = field.datasetFieldName(dataset);
            if(!validationHelper.containsStringField(dataset, fieldName)) {
                errorCollector.error(ErrorMessages.missingStringField(dataset, fieldName, this));
            }
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class IntTermCount extends DocMetric {
        public final FieldSet field;

        public IntTermCount(final FieldSet field) {
            this.field = field;
        }

        @Override
        public DocMetric transform(final Function<DocMetric, DocMetric> g, final Function<DocFilter, DocFilter> i) {
            return g.apply(new IntTermCount(field)).copyPosition(this);
        }

        @Override
        public List<String> getPushes(final String dataset) {
            return Collections.singletonList("inttermcount " + field.datasetFieldName(dataset));
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(final String dataset, final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
            final String fieldName = field.datasetFieldName(dataset);
            if(!validationHelper.containsIntOrAliasField(dataset, fieldName)) {
                if (validationHelper.containsStringField(dataset, fieldName)) {
                    // IntTermCount(stringField), maybe not what user wants.
                    final String warning =
                        "Suspicious use of INTTERMCOUNT. Did you mean STRTERMCOUNT?" +
                        " Using operator INTTERMCOUNT over string field \"" + field + "\" in dataset \"" + dataset + "\"." +
                        " Only string terms that can be converted to integer value will be counted." +
                        " If you want to get all terms count in a string field use STRTERMCOUNT operator instead";
                    errorCollector.warn(warning);
                } else {
                    // field not found, error.
                    errorCollector.error(ErrorMessages.missingIntField(dataset, fieldName, this));
                }
            }
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class StrTermCount extends DocMetric {
        public final FieldSet field;

        public StrTermCount(final FieldSet field) {
            this.field = field;
        }

        @Override
        public DocMetric transform(final Function<DocMetric, DocMetric> g, final Function<DocFilter, DocFilter> i) {
            return g.apply(new StrTermCount(field)).copyPosition(this);
        }

        @Override
        public List<String> getPushes(final String dataset) {
            return Collections.singletonList("strtermcount " + field.datasetFieldName(dataset));
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(final String dataset, final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
            final String fieldName = field.datasetFieldName(dataset);
            if(!validationHelper.containsStringField(dataset, fieldName)) {
                if (validationHelper.containsIntOrAliasField(dataset, fieldName)) {
                    // StrTermCount(intField) is 0.
                    final String warning =
                            "Using operator STRTERMCOUNT over int field \"" + field + "\" in dataset \"" + dataset + "\"." +
                            " Result is always zero.";
                    errorCollector.warn(warning);
                } else {
                    // field not found, error.
                    errorCollector.error(ErrorMessages.missingStringField(dataset, fieldName, this));
                }
            }
        }
    }

    // 0 for documents missing the field
    // 1 for documents with hash(term, salt) < probability
    // 2 for documents with hash(term, salt) >= probability
    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Sample extends DocMetric {
        public final FieldSet field;
        public final boolean isIntField;
        public final long numerator;
        public final long denominator;
        public final String salt;

        public Sample(final FieldSet field, final boolean isIntField, final long numerator, final long denominator, final String salt) {
            this.field = field;
            this.isIntField = isIntField;
            this.numerator = numerator;
            this.denominator = denominator;
            this.salt = salt;
        }

        @Override
        public DocMetric transform(final Function<DocMetric, DocMetric> g, final Function<DocFilter, DocFilter> i) {
            return g.apply(this).copyPosition(this);
        }

        @Override
        public List<String> getPushes(final String dataset) {
            final String datasetField = field.datasetFieldName(dataset);
            return Collections.singletonList("random " + (isIntField ? "int" : "str") + " [" + ((double) numerator) / denominator + "] " + datasetField + " \"" + salt + "\"");
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(final String dataset, final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
            validationHelper.validateSampleParams(numerator, denominator, errorCollector);
            final String fieldName = field.datasetFieldName(dataset);
            if (isIntField) {
                if (!validationHelper.containsIntField(dataset, fieldName)) {
                    errorCollector.error(ErrorMessages.missingStringField(dataset, fieldName, this));
                }
            } else {
                if (!validationHelper.containsStringField(dataset, fieldName)) {
                    errorCollector.error(ErrorMessages.missingIntField(dataset, fieldName, this));
                }
            }
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Random extends DocMetric {
        public final FieldSet field;
        public final boolean isIntField;
        public final int max;
        public final String salt;

        public Random(final FieldSet field, final boolean isIntField, final int max, final String salt) {
            this.field = field;
            this.isIntField = isIntField;
            this.max = max;
            this.salt = salt;
        }

        @Override
        public DocMetric transform(final Function<DocMetric, DocMetric> g, final Function<DocFilter, DocFilter> i) {
            return g.apply(new Random(field, isIntField, max, salt)).copyPosition(this);
        }

        @Override
        public List<String> getPushes(final String dataset) {
            final String datasetField = field.datasetFieldName(dataset);
            final String percentages = makePercentages(max);
            return Collections.singletonList("random " + (isIntField ? "int" : "str") + " [" + percentages + "] " + datasetField + " \"" + salt + "\"");
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(final String dataset, final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
            final String fieldName = field.datasetFieldName(dataset);
            if (isIntField) {
                if (!validationHelper.containsIntField(dataset, fieldName)) {
                    errorCollector.error(ErrorMessages.missingStringField(dataset, fieldName, this));
                }
            } else {
                if (!validationHelper.containsStringField(dataset, fieldName)) {
                    errorCollector.error(ErrorMessages.missingIntField(dataset, fieldName, this));
                }
            }
        }
    }

    // 0 for documents missing the field
    // 1 for documents with hash(term, salt) < probability
    // 2 for documents with hash(term, salt) >= probability
    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class SampleMetric extends DocMetric {
        public final DocMetric metric;
        public final long numerator;
        public final long denominator;
        public final String salt;

        public SampleMetric(final DocMetric metric, final long numerator, final long denominator, final String salt) {
            this.metric = metric;
            this.numerator = numerator;
            this.denominator = denominator;
            this.salt = salt;
        }

        @Override
        public DocMetric transform(final Function<DocMetric, DocMetric> g, final Function<DocFilter, DocFilter> i) {
            return g.apply(new SampleMetric(metric.transform(g, i), numerator, denominator, salt))
                    .copyPosition(this);
        }

        @Override
        public List<String> getPushes(final String dataset) {
            final List<String> result = new ArrayList<>(metric.getPushes(dataset));
            result.add("random_metric  [" + ((double) numerator) / denominator + "] \"" + salt + "\"");
            return result;
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(final String dataset, final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
            validationHelper.validateSampleParams(numerator, denominator, errorCollector);
            metric.validate(dataset, validationHelper, errorCollector);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class RandomMetric extends DocMetric {
        public final DocMetric metric;
        public final int max;
        public final String salt;

        public RandomMetric(final DocMetric metric, final int max, final String salt) {
            this.metric = metric;
            this.max = max;
            this.salt = salt;
        }

        @Override
        public DocMetric transform(final Function<DocMetric, DocMetric> g, final Function<DocFilter, DocFilter> i) {
            return g.apply(new RandomMetric(metric.transform(g, i), max, salt)).copyPosition(this);
        }

        @Override
        public List<String> getPushes(final String dataset) {
            final List<String> result = new ArrayList<>(metric.getPushes(dataset));
            final String percentages = makePercentages(max);
            result.add("random_metric [" + percentages + "] \"" + salt + "\"");
            return result;
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(final String dataset, final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
            metric.validate(dataset, validationHelper, errorCollector);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class FieldInQueryPlaceholderMetric extends DocMetric implements ZeroOneMetric {
        public final com.indeed.iql2.language.query.Query query;
        public final FieldSet field;
        public final boolean isNegated; // true if <field> NOT IN <query>
        @ToString.Exclude
        @EqualsAndHashCode.Exclude
        private final DatasetsMetadata datasetsMetadata;

        FieldInQueryPlaceholderMetric(final com.indeed.iql2.language.query.Query query, final FieldSet field, final boolean isNegated, final DatasetsMetadata datasetsMetadata) {
            this.query = query;
            this.field = field;
            this.isNegated = isNegated;
            this.datasetsMetadata = datasetsMetadata;
        }

        @Override
        public DocMetric transform(final Function<DocMetric, DocMetric> g, final Function<DocFilter, DocFilter> i) {
            return g.apply(new FieldInQueryPlaceholderMetric(query.transform(Function.identity(), Function.identity(), g, Function.identity(), i), field, isNegated, datasetsMetadata).copyPosition(this));
        }

        @Override
        public List<String> getPushes(final String dataset) {
            throw new UnsupportedOperationException("You shouldn't get pushes out of this place holder metric just exists for validating FieldInQuery before executing subquery");
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            throw new UnsupportedOperationException("You shouldn't visit this place holder metric just exists for validating FieldInQuery before executing subquery");
        }

        @Override
        public void validate(final String dataset, final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
            final String fieldName = field.datasetFieldName(dataset);
            if (!validationHelper.containsField(dataset, fieldName)) {
                errorCollector.error(ErrorMessages.missingField(dataset, fieldName, this));
            }
            CommandValidator.validate(query, datasetsMetadata, errorCollector);
        }

        @Override
        public DocFilter convertToFilter() {
            return new DocFilter.FieldInQuery(query, field, isNegated, datasetsMetadata);
        }

        @Nullable
        @Override
        public DocMetric invert() {
            return null;
        }
    }

    private static String makePercentages(final int max) {
        final StringBuilder percentages = new StringBuilder();
        final DecimalFormat decimalFormat = new DecimalFormat();
        // DecimalFormat.DOUBLE_FRACTION_DIGITS is 340
        decimalFormat.setMaximumFractionDigits(340);
        for (int i = 1; i < max; i++) {
            if (percentages.length() > 0) {
                percentages.append(',');
            }
            percentages.append(decimalFormat.format(((double) i) / max));
        }
        return percentages.toString();
    }
}
