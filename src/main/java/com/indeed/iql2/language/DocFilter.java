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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.indeed.flamdex.query.BooleanOp;
import com.indeed.flamdex.query.Query;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.language.actions.Action;
import com.indeed.iql2.language.actions.FieldInQueryPlaceholderAction;
import com.indeed.iql2.language.actions.IntOrAction;
import com.indeed.iql2.language.actions.MetricAction;
import com.indeed.iql2.language.actions.QueryAction;
import com.indeed.iql2.language.actions.RegexAction;
import com.indeed.iql2.language.actions.SampleAction;
import com.indeed.iql2.language.actions.SampleMetricAction;
import com.indeed.iql2.language.actions.StringOrAction;
import com.indeed.iql2.language.actions.UnconditionalAction;
import com.indeed.iql2.language.passes.ExtractQualifieds;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.query.fieldresolution.ScopedFieldResolver;
import com.indeed.iql2.language.util.ErrorMessages;
import com.indeed.iql2.language.util.MapUtil;
import com.indeed.iql2.language.util.ParserUtil;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;
import com.indeed.iql2.server.web.servlets.query.CommandValidator;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class DocFilter extends AbstractPositional {

    public interface Visitor<T, E extends Throwable> {
        T visit(FieldIs fieldIs) throws E;
        T visit(FieldIsnt fieldIsnt) throws E;
        T visit(MetricEqual metricEqual) throws E;
        T visit(FieldInQuery fieldInQuery) throws E;
        T visit(Between between) throws E;
        T visit(MetricNotEqual metricNotEqual) throws E;
        T visit(MetricGt metricGt) throws E;
        T visit(MetricGte metricGte) throws E;
        T visit(MetricLt metricLt) throws E;
        T visit(MetricLte metricLte) throws E;
        T visit(And and) throws E;
        T visit(Or or) throws E;
        T visit(Not not) throws E;
        T visit(Regex regex) throws E;
        T visit(NotRegex notRegex) throws E;
        T visit(Qualified qualified) throws E;
        T visit(Lucene lucene) throws E;
        T visit(Sample sample) throws E;
        T visit(SampleDocMetric sample) throws E;
        T visit(Always always) throws E;
        T visit(Never never) throws E;
        T visit(FieldInTermsSet fieldInTermsSet) throws E;
        T visit(ExplainFieldIn explainFieldIn) throws E;
        T visit(FieldEqual equal) throws E;
    }

    /**
     * @see com.indeed.iql2.language.query.Query#transform(Function, Function, Function, Function, Function)
     */
    public abstract DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i);

    public abstract DocMetric asZeroOneMetric(String dataset);

    public abstract List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier);

    public abstract <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E;

    public abstract void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector);

    @Override
    public abstract boolean equals(final Object other);
    @Override
    public abstract int hashCode();
    @Override
    public abstract String toString();

    @Override
    public DocFilter copyPosition(Positional positional) {
        super.copyPosition(positional);
        return this;
    }

    @EqualsAndHashCode(callSuper = false)
    public abstract static class FieldTermEqual extends DocFilter {
        public final FieldSet field;
        public final Term term;

        public FieldTermEqual(FieldSet field, Term term) {
            this.field = field;
            this.term = term;
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            final String fieldName = field.datasetFieldName(dataset);
            if (term.isIntTerm()) {
                ValidationUtil.validateField(ImmutableSet.of(dataset), fieldName, validationHelper, errorCollector, this);
            } else {
                ValidationUtil.validateStringField(ImmutableSet.of(dataset), fieldName, validationHelper, errorCollector, this);
            }
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class FieldIs extends FieldTermEqual {
        private FieldIs(FieldSet field, Term term) {
            super(field, term);
        }

        public static DocFilter create(final FieldSet field, final Term term) {
            if (field.isIntField() && !term.isIntTerm()) {
                return new DocFilter.Never();
            }
            return new FieldIs(field, term);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new FieldIs(field, term)).copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(final String dataset) {
            // Will not throw if validate succeeded
            return DocMetrics.hasTermMetricOrThrow(field, term);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            Preconditions.checkState(scope.keySet().equals(field.datasets()));
            final Map<String, Query> datasetToQuery = new HashMap<>();
            for (final String dataset : field.datasets()) {
                final Query query = Query.newTermQuery(term.toFlamdex(field.datasetFieldName(dataset), field.isIntField()));
                datasetToQuery.put(dataset, query);
            }
            return Collections.singletonList(new QueryAction(datasetToQuery, target, positive, negative));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public String toString() {
            return "FieldIs{" +
                    "field='" + field + '\'' +
                    ", term=" + term +
                    '}';
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class FieldIsnt extends FieldTermEqual {
        private FieldIsnt(FieldSet field, Term term) {
            super(field, term);
        }

        public static DocFilter create(final FieldSet field, final Term term) {
            if (field.isIntField() && !term.isIntTerm()) {
                return new Always();
            }
            return new FieldIsnt(field, term);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new FieldIsnt(field, term)).copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new Not(FieldIs.create(field, term)).asZeroOneMetric(dataset);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return new Not(FieldIs.create(field, term)).getExecutionActions(scope, target, positive, negative, groupSupplier);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public String toString() {
            return "FieldIsnt{" +
                    "field='" + field + '\'' +
                    ", term=" + term +
                    '}';
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class FieldInQuery extends DocFilter {
        public final com.indeed.iql2.language.query.Query query;
        public final FieldSet field;
        public final boolean isNegated; // true if <field> NOT IN <query>
        @ToString.Exclude
        @EqualsAndHashCode.Exclude
        private final DatasetsMetadata datasetsMetadata;

        public FieldInQuery(final com.indeed.iql2.language.query.Query query, final FieldSet field, final boolean isNegated, final DatasetsMetadata datasetsMetadata) {
            this.query = query;
            this.field = field;
            this.isNegated = isNegated;
            this.datasetsMetadata = datasetsMetadata;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new FieldInQuery(query, field, isNegated, datasetsMetadata)).copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.FieldInQueryPlaceholderMetric(query, field, isNegated, datasetsMetadata);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return Collections.singletonList(new FieldInQueryPlaceholderAction(field, query, isNegated, datasetsMetadata, target, positive, negative));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            CommandValidator.validate(query, datasetsMetadata, errorCollector);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Between extends DocFilter {
        public final DocMetric metric;
        public final long lowerBound;
        public final long upperBound;
        public final boolean isUpperInclusive;

        public Between(final DocMetric metric, final long lowerBound, final long upperBound, final boolean isUpperInclusive) {
            this.metric = metric;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.isUpperInclusive = isUpperInclusive;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new Between(metric.transform(g, i), lowerBound, upperBound, isUpperInclusive)).copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(final String dataset) {
            final DocFilter lowerCondition = new MetricGte(metric, new DocMetric.Constant(lowerBound));
            final DocFilter upperCondition = isUpperInclusive ?
                    new MetricLte(metric, new DocMetric.Constant(upperBound)) :
                    new MetricLt(metric, new DocMetric.Constant(upperBound));
            final DocFilter between = And.create(lowerCondition, upperCondition);

            return between.asZeroOneMetric(dataset);
        }

        @Override
        public List<Action> getExecutionActions(
                final Map<String, String> scope,
                final int target,
                final int positive,
                final int negative,
                final GroupSupplier groupSupplier) {
            if (metric instanceof DocMetric.Field) {
                // Quite frequent case. And it's possible to process it more optimal than metricFilter
                final FieldSet field = ((DocMetric.Field) metric).field;
                Preconditions.checkState(scope.keySet().equals(field.datasets()));
                final Map<String, Query> datasetToQuery = new HashMap<>();
                for (final String dataset : field.datasets()) {
                    datasetToQuery.put(dataset, Query.newRangeQuery(field.datasetFieldName(dataset), lowerBound, upperBound, isUpperInclusive));
                }
                return Collections.singletonList(new QueryAction(datasetToQuery, target, positive, negative));
            } else {
                return Collections.singletonList(new MetricAction(ImmutableSet.copyOf(scope.keySet()), this, target, positive, negative));
            }
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
    public abstract static class MetricBinop extends DocFilter {
        public final DocMetric m1;
        public final DocMetric m2;

        public MetricBinop(DocMetric m1, DocMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            final Set<String> qualifications = Sets.union(ExtractQualifieds.extractDocMetricDatasets(m1), ExtractQualifieds.extractDocMetricDatasets(m2));
            if (qualifications.size() > 1) {
                throw new IqlKnownException.ParseErrorException("DocFilter cannot have multiple different qualifications! docFilter = [" + this + "], qualifications = [" + qualifications + "]");
            } else if (!scope.keySet().containsAll(qualifications)) {
                throw new IqlKnownException.ParseErrorException("Scope does not contain qualifications! scope = [" + scope.keySet() + "], qualifications = [" + qualifications + "]");
            } else if (qualifications.size() == 1) {
                return Collections.singletonList(new MetricAction(ImmutableSet.copyOf(qualifications), this, target, positive, negative));
            } else {
                return Collections.singletonList(new MetricAction(ImmutableSet.copyOf(scope.keySet()), this, target, positive, negative));
            }
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            m1.validate(dataset, validationHelper, errorCollector);
            m2.validate(dataset, validationHelper, errorCollector);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class MetricEqual extends MetricBinop {
        public MetricEqual(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricEqual(m1.transform(g, i), m2.transform(g, i)))
                    .copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricEqual(m1, m2);
        }

//        @Override
//        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
//            return super.getExecutionActions(scope, target, positive, negative, groupSupplier);
//            if (m1 instanceof DocMetric.Field && m2 instanceof DocMetric.Constant) {
//                final String field = ((DocMetric.Field) m1).field;
//                final long value = ((DocMetric.Constant) m2).value;
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newTermQuery(com.indeed.flamdex.query.Term.intTerm(field, value));
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else if (m1 instanceof DocMetric.Constant && m2 instanceof DocMetric.Field) {
//                final String field = ((DocMetric.Field) m2).field;
//                final long value = ((DocMetric.Constant) m1).value;
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newTermQuery(com.indeed.flamdex.query.Term.intTerm(field, value));
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else {
//                return Collections.<Action>singletonList(new MetricAction(scope.keySet(), this, target, positive, negative));
//            }
//        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public String toString() {
            return "MetricEqual{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class MetricNotEqual extends MetricBinop {
        public MetricNotEqual(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricNotEqual(m1.transform(g, i), m2.transform(g, i)))
                    .copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricNotEqual(m1, m2);
        }

//        @Override
//        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
//            return super.getExecutionActions(scope, target, positive, negative, groupSupplier);
//            // TODO: Not duplicate logic across these two branches
//            if (m1 instanceof DocMetric.Field && m2 instanceof DocMetric.Constant) {
//                final String field = ((DocMetric.Field) m1).field;
//                final long value = ((DocMetric.Constant) m2).value;
//                return getFieldNotEqualValue(scope, field, value, target, positive, negative);
//            } else if (m1 instanceof DocMetric.Constant && m2 instanceof DocMetric.Field) {
//                final String field = ((DocMetric.Field) m2).field;
//                final long value = ((DocMetric.Constant) m1).value;
//                return getFieldNotEqualValue(scope, field, value, target, positive, negative);
//            } else {
//                return Collections.<Action>singletonList(new MetricAction(scope.keySet(), this, target, positive, negative));
//            }
//        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        private static List<Action> getFieldNotEqualValue(Map<String, String> scope, String field, long value, int target, int positive, int negative) {
            final Query query = Query.newTermQuery(com.indeed.flamdex.query.Term.intTerm(field, value));
            final Query negated = Query.newBooleanQuery(BooleanOp.NOT, Collections.singletonList(query));
            return Collections.singletonList(new QueryAction(MapUtil.replicate(scope, negated), target, positive, negative));
        }

        @Override
        public String toString() {
            return "MetricNotEqual{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class MetricGt extends MetricBinop {
        public MetricGt(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricGt(m1.transform(g, i), m2.transform(g, i)))
                    .copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricGt(m1, m2);
        }

//        @Override
//        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
//            return super.getExecutionActions(scope, target, positive, negative, groupSupplier);
//            if (m1 instanceof DocMetric.Field && m2 instanceof DocMetric.Constant) {
//                final String field = ((DocMetric.Field) m1).field;
//                final long value = ((DocMetric.Constant) m2).value;
//                // field > value => field in [value + 1, MAX_VALUE]
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newRangeQuery(field, value + 1, Long.MAX_VALUE, true);
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else if (m1 instanceof DocMetric.Constant && m2 instanceof DocMetric.Field) {
//                final String field = ((DocMetric.Field) m2).field;
//                final long value = ((DocMetric.Constant) m1).value;
//                // value > field => field in [MIN_VALUE, value)
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newRangeQuery(field, Long.MIN_VALUE, value, false);
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else {
//                return Collections.<Action>singletonList(new MetricAction(scope.keySet(), this, target, positive, negative));
//            }
//        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public String toString() {
            return "MetricGt{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class MetricGte extends MetricBinop {
        public MetricGte(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricGte(m1.transform(g, i), m2.transform(g, i)))
                    .copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricGte(m1, m2);
        }

//        @Override
//        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
//            return super.getExecutionActions(scope, target, positive, negative, groupSupplier);
//            if (m1 instanceof DocMetric.Field && m2 instanceof DocMetric.Constant) {
//                final String field = ((DocMetric.Field) m1).field;
//                final long value = ((DocMetric.Constant) m2).value;
//                // field >= value => field in [value, MAX_VALUE]
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newRangeQuery(field, value, Long.MAX_VALUE, true);
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else if (m1 instanceof DocMetric.Constant && m2 instanceof DocMetric.Field) {
//                final String field = ((DocMetric.Field) m2).field;
//                final long value = ((DocMetric.Constant) m1).value;
//                // value >= field => field in [MIN_VALUE, value]
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newRangeQuery(field, Long.MIN_VALUE, value, true);
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else {
//                return Collections.<Action>singletonList(new MetricAction(scope.keySet(), this, target, positive, negative));
//            }
//        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public String toString() {
            return "MetricGte{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class MetricLt extends MetricBinop {
        public MetricLt(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricLt(m1.transform(g, i), m2.transform(g, i)))
                    .copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricLt(m1, m2);
        }

//        @Override
//        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
//            return super.getExecutionActions(scope, target, positive, negative, groupSupplier);
//            if (m1 instanceof DocMetric.Field && m2 instanceof DocMetric.Constant) {
//                final String field = ((DocMetric.Field) m1).field;
//                final long value = ((DocMetric.Constant) m2).value;
//                // field < value => field in [MIN_VALUE, value)
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newRangeQuery(field, Long.MIN_VALUE, value, false);
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else if (m1 instanceof DocMetric.Constant && m2 instanceof DocMetric.Field) {
//                final String field = ((DocMetric.Field) m2).field;
//                final long value = ((DocMetric.Constant) m1).value;
//                // value < field => field in [value + 1, MAX_VALUE]
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newRangeQuery(field, value + 1, Long.MAX_VALUE, true);
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else {
//                return Collections.<Action>singletonList(new MetricAction(scope.keySet(), this, target, positive, negative));
//            }
//        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public String toString() {
            return "MetricLt{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class MetricLte extends MetricBinop {
        public MetricLte(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricLte(m1.transform(g, i), m2.transform(g, i)))
                    .copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricLte(m1, m2);
        }

//        @Override
//        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
//            return super.getExecutionActions(scope, target, positive, negative, groupSupplier);
//            if (m1 instanceof DocMetric.Field && m2 instanceof DocMetric.Constant) {
//                final String field = ((DocMetric.Field) m1).field;
//                final long value = ((DocMetric.Constant) m2).value;
//                // field <= value => field in [MIN_VALUE, value]
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newRangeQuery(field, Long.MIN_VALUE, value, true);
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else if (m1 instanceof DocMetric.Constant && m2 instanceof DocMetric.Field) {
//                final String field = ((DocMetric.Field) m2).field;
//                final long value = ((DocMetric.Constant) m1).value;
//                // value <= field => field in [value, MAX_VALUE]
//                final com.indeed.flamdex.query.Query query = com.indeed.flamdex.query.Query.newRangeQuery(field, value, Long.MAX_VALUE, true);
//                return Collections.<Action>singletonList(new QueryAction(scope.keySet(), MapUtil.replicate(scope, query), target, positive, negative));
//            } else {
//                return Collections.<Action>singletonList(new MetricAction(scope.keySet(), this, target, positive, negative));
//            }
//        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public String toString() {
            return "MetricLte{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    public abstract static class Multiary extends DocFilter {
        public final List<DocFilter> filters;

        protected Multiary(final List<DocFilter> filters) {
            this.filters = filters;
        }

        protected abstract DocFilter createFilter(final List<DocFilter> filters);

        @Override
        public final DocFilter transform(
                final Function<DocMetric, DocMetric> g,
                final Function<DocFilter, DocFilter> i) {
            final List<DocFilter> transformed =
                    filters.stream()
                    .map(f -> f.transform(g, i))
                    .collect(Collectors.toList());
            return i.apply(createFilter(transformed)).copyPosition(this);
        }

        @Override
        public final void validate(
                final String dataset,
                final ValidationHelper validationHelper,
                final ErrorCollector errorCollector) {
            filters.forEach(f -> f.validate(dataset, validationHelper, errorCollector));
        }

        @Override
        public final boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Multiary other = (Multiary) o;
            if (filters.size() != other.filters.size()) {
                return false;
            }
            for (int i = 0; i < filters.size(); i++) {
                if (!Objects.equals(filters.get(i), other.filters.get(i))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public final int hashCode() {
            int hash = 0;
            for (final DocFilter filter : filters) {
                hash = 31 * hash + filter.hashCode();
            }
            return hash;
        }

        @Override
        public final String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append(getClass().getSimpleName()).append('{');
            for (int i = 0; i < filters.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append('f').append(i+1).append('=').append(filters.get(i));
            }
            sb.append('}');
            return sb.toString();
        }
    }

    public static class And extends Multiary {

        private And(final List<DocFilter> filters) {
            super(filters);
        }

        // create filter that is equivalent to 'and' of all filters and simplify it.
        public static DocFilter create(final List<DocFilter> original) {
            final List<DocFilter> filters = new ArrayList<>(original.size());
            for (final DocFilter filter : original) {
                if (filter instanceof Never) {
                    // result is constant false
                    return new Never();
                }
                if (filter instanceof Always) {
                    // skipping as it does not affect result
                    continue;
                }
                if (filter instanceof And) {
                    // unwrapping filters from another And
                    filters.addAll(((And)filter).filters);
                } else {
                    filters.add(filter);
                }
            }
            if (filters.isEmpty()) {
                return new Always();
            }
            if (filters.size() == 1) {
                return filters.get(0);
            }
            return new And(filters);
        }

        public static DocFilter create(final DocFilter f1, final DocFilter f2) {
            return create(ImmutableList.of(f1, f2));
        }

        // unwrap And filters if present
        public static List<DocFilter> unwrap(final List<DocFilter> filters) {
            if (filters.isEmpty()) {
                return new ArrayList<>();
            }
            // will be unwrapped here.
            final DocFilter filter = create(filters);
            if (filter instanceof And) {
                return new ArrayList<>(((And)filter).filters);
            }
            final List<DocFilter> result = new ArrayList<>(1);
            result.add(filter);
            return result;
        }

        @Override
        public DocFilter createFilter(final List<DocFilter> filters) {
            return create(filters);
        }

        @Override
        public DocMetric asZeroOneMetric(final String dataset) {
            final List<DocMetric> metrics =
                    filters.stream()
                    .map(f -> f.asZeroOneMetric(dataset))
                    .collect(Collectors.toList());
            return new DocMetric.MetricEqual(DocMetric.Add.create(metrics), new DocMetric.Constant(metrics.size()));
        }

        @Override
        public List<Action> getExecutionActions(
                final Map<String, String> scope,
                final int target,
                final int positive,
                final int negative,
                final GroupSupplier groupSupplier) {
            final List<Action> result = new ArrayList<>();
            if (positive == negative) {
                return result;
            }
            if (target != negative) {
                for (int i = 0; i < (filters.size() - 1); i++) {
                    result.addAll(filters.get(i).getExecutionActions(scope, target, target, negative, groupSupplier));
                }
                result.addAll(filters.get(filters.size()-1).getExecutionActions(scope, target, positive, negative, groupSupplier));
            } else {
                // target == negative
                final int newGroup = groupSupplier.acquire();
                for (final DocFilter filter : filters) {
                    result.addAll(filter.getExecutionActions(scope, target, target, newGroup, groupSupplier));
                }
                result.add(new UnconditionalAction(ImmutableSet.copyOf(scope.keySet()), target, positive));
                result.add(new UnconditionalAction(ImmutableSet.copyOf(scope.keySet()), newGroup, target));
                groupSupplier.release(newGroup);
            }
            return result;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    public static class Or extends Multiary {

        private Or(final List<DocFilter> filters) {
            super(filters);
        }

        // create filter that is equivalent to 'or' of all filters and simplify it.
        public static DocFilter create(final List<DocFilter> original) {
            final List<DocFilter> filters = new ArrayList<>(original.size());
            for (final DocFilter filter : original) {
                if (filter instanceof Never) {
                    // skipping as it does not affect result
                    continue;
                }
                if (filter instanceof Always) {
                    // result is constant true
                    return new Always();
                }
                if (filter instanceof Or) {
                    // unwrapping filters from another Or
                    filters.addAll(((Or)filter).filters);
                } else {
                    filters.add(filter);
                }
            }
            if (filters.isEmpty()) {
                return new Never();
            }
            if (filters.size() == 1) {
                return filters.get(0);
            }
            return new Or(filters);
        }

        public static DocFilter create(final DocFilter f1, final DocFilter f2) {
            return create(ImmutableList.of(f1, f2));
        }

        @Override
        public DocFilter createFilter(final List<DocFilter> filters) {
            return create(filters);
        }

        @Override
        public DocMetric asZeroOneMetric(final String dataset) {
            final List<DocMetric> metrics = new ArrayList<>(filters.size());
            for (final DocFilter filter : filters) {
                metrics.add(filter.asZeroOneMetric(dataset));
            }
            return new DocMetric.MetricGt(DocMetric.Add.create(metrics), new DocMetric.Constant(0));
        }

        @Override
        public List<Action> getExecutionActions(
                final Map<String, String> scope,
                final int target,
                final int positive,
                final int negative,
                final GroupSupplier groupSupplier) {
            final List<Action> result = new ArrayList<>();
            if (positive == negative) {
                return result;
            }
            if ((target != positive)) {
                for (int i = 0; i < filters.size() - 1; i++) {
                    result.addAll(filters.get(i).getExecutionActions(scope, target, positive, target, groupSupplier));
                }
                result.addAll(filters.get(filters.size()-1).getExecutionActions(scope, target, positive, negative, groupSupplier));
            } else {
                // target == positive
                final int newGroup = groupSupplier.acquire();
                for (final DocFilter filter : filters) {
                    result.addAll(filter.getExecutionActions(scope, target, newGroup, target, groupSupplier));
                }
                result.add(new UnconditionalAction(ImmutableSet.copyOf(scope.keySet()), target, negative));
                result.add(new UnconditionalAction(ImmutableSet.copyOf(scope.keySet()), newGroup, positive));
                groupSupplier.release(newGroup);
            }
            return result;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Not extends DocFilter {
        public final DocFilter filter;

        public Not(final DocFilter filter) {
            this.filter = filter;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new Not(filter.transform(g, i))).copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.Subtract(new DocMetric.Constant(1), filter.asZeroOneMetric(dataset));
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return filter.getExecutionActions(scope, target, negative, positive, groupSupplier);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            filter.validate(dataset, validationHelper, errorCollector);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Regex extends DocFilter {
        public final FieldSet field;
        public final String regex;

        public Regex(final FieldSet field, final String regex) {
            this.field = field;
            this.regex = regex;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new Regex(field, regex)).copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.RegexMetric(field, regex);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return Collections.singletonList(new RegexAction(field, regex, target, positive, negative));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            ValidationUtil.compileRegex(regex);
            if (!validationHelper.containsStringField(dataset, field.datasetFieldName(dataset))) {
                errorCollector.error(ErrorMessages.missingStringField(dataset, field.datasetFieldName(dataset), this));
            }
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class NotRegex extends DocFilter {
        public final FieldSet field;
        public final String regex;

        public NotRegex(final FieldSet field, final String regex) {
            this.field = field;
            this.regex = regex;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new NotRegex(field, regex)).copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new Not(new Regex(field, regex)).asZeroOneMetric(dataset);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return new Not(new Regex(field, regex)).getExecutionActions(scope, target, positive, negative, groupSupplier);
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
    public static class FieldEqual extends DocFilter {
        public final FieldSet field1;
        public final FieldSet field2;

        public FieldEqual(final FieldSet field1, final FieldSet field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new FieldEqual(field1, field2)).copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.FieldEqualMetric(field1, field2);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return Collections.singletonList(new MetricAction(ImmutableSet.copyOf(scope.keySet()), this, target, positive, negative));
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            ValidationUtil.validateExistenceAndSameFieldType(dataset, field1.datasetFieldName(dataset), field2.datasetFieldName(dataset), validationHelper, errorCollector);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Qualified extends DocFilter {
        // TODO: Why is this a List<String> instead of a single string? A per-document thing can only be one dataset!
        public final List<String> scope;
        public final DocFilter filter;

        public Qualified(final List<String> scope, final DocFilter filter) {
            this.scope = scope;
            this.filter = filter;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new Qualified(scope, filter.transform(g, i))).copyPosition(this);
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

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            final Map<String, String> restrictedScope = new HashMap<>();
            for (final String dataset : this.scope) {
                restrictedScope.put(dataset, scope.get(dataset));
            }
            return filter.getExecutionActions(restrictedScope, target, positive, negative, groupSupplier);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            if (scope.contains(dataset)) {
                filter.validate(dataset, validationHelper, errorCollector);
            }
            ValidationUtil.validateScope(scope, validationHelper, errorCollector);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Lucene extends DocFilter {
        public final String query;
        @ToString.Exclude
        @EqualsAndHashCode.Exclude
        private final ScopedFieldResolver fieldResolver;
        @ToString.Exclude
        @EqualsAndHashCode.Exclude
        private final DatasetsMetadata datasetsMetadata;

        public Lucene(final String query, final ScopedFieldResolver fieldResolver, final DatasetsMetadata datasetsMetadata) {
            this.query = query;
            this.fieldResolver = fieldResolver;
            this.datasetsMetadata = datasetsMetadata;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new Lucene(query, fieldResolver, datasetsMetadata)).copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            final Query flamdexQuery = ParserUtil.getFlamdexQuery(query, dataset, datasetsMetadata, fieldResolver);
            final DocFilter filter = FlamdexQueryTranslator.translate(flamdexQuery, fieldResolver);
            return filter.asZeroOneMetric(dataset);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            final Map<String, Query> datasetToQuery = new HashMap<>();
            for (final String dataset : scope.keySet()) {
                final Query flamdexQuery = ParserUtil.getFlamdexQuery(query, dataset, datasetsMetadata, fieldResolver);
                datasetToQuery.put(dataset, flamdexQuery);
            }
            return Collections.singletonList(new QueryAction(datasetToQuery, target, positive, negative));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            final Query flamdexQuery = ParserUtil.getFlamdexQuery(query, dataset, datasetsMetadata, fieldResolver);
            ValidationUtil.validateQuery(validationHelper, ImmutableMap.of(dataset, flamdexQuery), errorCollector, this);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Sample extends DocFilter {
        public final FieldSet field;
        public final boolean isIntField;
        public final long numerator;
        public final long denominator;
        public final String seed;

        public Sample(final FieldSet field, final boolean isIntField, final long numerator, final long denominator, final String seed) {
            this.field = field;
            this.isIntField = isIntField;
            this.numerator = numerator;
            this.denominator = denominator;
            this.seed = seed;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new Sample(field, isIntField, numerator, denominator, seed)).copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            // Sample() returns 0 for no term, 1 for below p, and 2 for above p.
            // We do (1 - p) to keep the same half of the divide as SAMPLE does.
            return new DocMetric.MetricEqual(
                    new DocMetric.Sample(field, isIntField, (denominator - numerator), denominator, seed),
                    new DocMetric.Constant(2)
            );
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            Preconditions.checkState(scope.keySet().equals(field.datasets()));
            return Collections.singletonList(new SampleAction(field, (double) numerator / denominator, seed, target, positive, negative));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            if ((numerator < 0) || (numerator > denominator)) {
                errorCollector.error(ErrorMessages.incorrectSampleParams(numerator, denominator));
            }
            final String fieldName = field.datasetFieldName(dataset);
            if (!validationHelper.containsField(dataset, fieldName)) {
                errorCollector.error(ErrorMessages.missingField(dataset, fieldName, this));
            }
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class SampleDocMetric extends DocFilter {
        public final DocMetric metric;
        public final long numerator;
        public final long denominator;
        public final String seed;

        public SampleDocMetric(final DocMetric metric, final long numerator, final long denominator, final String seed) {
            this.metric = metric;
            this.numerator = numerator;
            this.denominator = denominator;
            this.seed = seed;
        }

        @Override
        public DocFilter transform(final Function<DocMetric, DocMetric> g,
                                   final Function<DocFilter, DocFilter> i) {
            return i.apply(new SampleDocMetric(metric.transform(g, i), numerator, denominator, seed))
                    .copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(final String dataset) {
            // SampleMetric() returns 0 for no term, 1 for below p, and 2 for above p.
            // We do (1 - p) to keep the same half of the divide as SAMPLE does.
            return new DocMetric.MetricEqual(
                    new DocMetric.SampleMetric(metric, denominator - numerator, denominator, seed),
                    new DocMetric.Constant(2)
            );
        }

        @Override
        public List<Action> getExecutionActions(final Map<String, String> scope,
                                                final int target,
                                                final int positive,
                                                final int negative,
                                                final GroupSupplier groupSupplier) {
            final ImmutableMap.Builder<String, DocMetric> perDatasetMetrics = ImmutableMap.builder();
            for (final String dataset : scope.keySet()) {
                perDatasetMetrics.put(dataset, metric);
            }
            return Collections.singletonList(new SampleMetricAction(perDatasetMetrics.build(), (double) numerator / denominator, seed, target, positive, negative));
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(final String dataset,
                             final ValidationHelper validationHelper,
                             final ErrorCollector errorCollector) {
            if ((numerator < 0) || (numerator > denominator)) {
                errorCollector.error(ErrorMessages.incorrectSampleParams(numerator, denominator));
            }
            metric.validate(dataset, validationHelper, errorCollector);
        }
    }

    public static class Always extends DocFilter {
        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new Always()).copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.Constant(1);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return Collections.singletonList(new UnconditionalAction(ImmutableSet.copyOf(scope.keySet()), target, positive));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {

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

    public static class Never extends DocFilter {
        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new Never()).copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.Constant(0);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return Collections.singletonList(new UnconditionalAction(ImmutableSet.copyOf(scope.keySet()), target, negative));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {

        }

        @Override
        public int hashCode() {
            return 2;
        }

        @Override
        public boolean equals(Object obj) {
            return getClass().equals(obj.getClass());
        }

        @Override
        public String toString() {
            return "Never{}";
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class FieldInTermsSet extends DocFilter {
        public final FieldSet field;
        public final ImmutableSet<Term> terms;

        private FieldInTermsSet(final FieldSet field, final ImmutableSet<Term> terms) {
            this.field = field;
            this.terms = terms;
        }

        public static DocFilter create(final FieldSet field, final ImmutableSet<Term> terms) {
            if (terms.isEmpty()) {
                // TODO: propagate this to upper level
                return new Never();
            }
            return new FieldInTermsSet(field, terms);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new FieldInTermsSet(field, terms)).copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(final String dataset) {
            final List<DocFilter> filters =
                    terms.stream()
                            .map(term -> FieldIs.create(field, term))
                            .collect(Collectors.toList());
            final DocFilter or = Or.create(filters);
            return or.asZeroOneMetric(dataset);
        }

        @Override
        public List<Action> getExecutionActions(
                final Map<String, String> scope,
                final int target,
                final int positive,
                final int negative,
                final GroupSupplier groupSupplier) {
            Preconditions.checkState(scope.keySet().equals(field.datasets()));
            if (field.isIntField()) {
                // Some of terms cannot be represented as int.
                // We did a warning about it in validate
                final ImmutableSet<Long> intTerms =
                        ImmutableSet.copyOf(terms.stream().filter(Term::isIntTerm).map(Term::getIntTerm).iterator());
                if (intTerms.isEmpty()) {
                    return Collections.singletonList(new UnconditionalAction(ImmutableSet.copyOf(scope.keySet()), target, negative));
                } else {
                    return Collections.singletonList(new IntOrAction(field, intTerms, target, positive, negative));
                }
            } else {
                final ImmutableSet<String> stringTerms = ImmutableSet.copyOf(terms.stream().map(Term::asString).iterator());
                // TODO: Should this care about the keyword analyzer fields?
                // TODO 2: What does comment above mean?
                return Collections.singletonList(new StringOrAction(field, stringTerms, target, positive, negative));
            }
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(final String dataset, final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
            if (terms.isEmpty()) {
                errorCollector.error("Cannot have empty set of terms in [" + getTextOrToString() + "]");
            }
            final String fieldName = field.datasetFieldName(dataset);
            if (field.isIntField()) {
                if (!validationHelper.containsIntField(dataset, fieldName)) {
                    errorCollector.error(ErrorMessages.missingIntField(dataset, fieldName, this));
                }
                final boolean allInts = terms.stream().allMatch(Term::isIntTerm);
                if (!allInts) {
                    errorCollector.warn(ErrorMessages.intFieldWithStringTerms(dataset, fieldName, this));
                }
            } else {
                if (!validationHelper.containsStringField(dataset, fieldName)) {
                    errorCollector.error(ErrorMessages.missingStringField(dataset, fieldName, this));
                }
            }
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class ExplainFieldIn extends DocFilter {
        public final com.indeed.iql2.language.query.Query query;
        private final FieldSet field;
        private final boolean isNegated;

        public ExplainFieldIn(final com.indeed.iql2.language.query.Query query, final FieldSet field, final boolean isNegated) {
            this.query = query;
            this.field = field;
            this.isNegated = isNegated;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this).copyPosition(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.Constant(1);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            final HashSet<String> fakeTerms = new HashSet<>();
            fakeTerms.add("fakeTerm");
            if (isNegated) {
                int tmp = positive;
                positive = negative;
                negative = tmp;
            }
            return Collections.singletonList(new StringOrAction(field, ImmutableSet.copyOf(fakeTerms), target, positive, negative));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, ErrorCollector errorCollector) {
        }
    }
}
