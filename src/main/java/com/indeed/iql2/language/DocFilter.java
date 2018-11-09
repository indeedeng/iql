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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.flamdex.query.BooleanOp;
import com.indeed.flamdex.query.Query;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.language.actions.Action;
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
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
        T visit(Ors ors) throws E;
        T visit(Not not) throws E;
        T visit(Regex regex) throws E;
        T visit(NotRegex notRegex) throws E;
        T visit(Qualified qualified) throws E;
        T visit(Lucene lucene) throws E;
        T visit(Sample sample) throws E;
        T visit(SampleDocMetric sample) throws E;
        T visit(Always always) throws E;
        T visit(Never never) throws E;
        T visit(StringFieldIn stringFieldIn) throws E;
        T visit(IntFieldIn intFieldIn) throws E;
        T visit(ExplainFieldIn explainFieldIn) throws E;
        T visit(FieldEqual equal) throws E;
    }

    public abstract DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i);

    public abstract DocMetric asZeroOneMetric(String dataset);

    public abstract List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier);

    public abstract <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E;

    public abstract void validate(String dataset, ValidationHelper validationHelper, Validator validator);

    public abstract static class FieldTermEqual extends DocFilter {
        public final FieldSet field;
        public final Term term;

        public FieldTermEqual(FieldSet field, Term term) {
            this.field = field;
            this.term = term;
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            final String fieldName = field.datasetFieldName(dataset);
            if (term.isIntTerm) {
                ValidationUtil.validateIntField(ImmutableSet.of(dataset), fieldName, validationHelper, validator, this);
            } else {
                ValidationUtil.validateStringField(ImmutableSet.of(dataset), fieldName, validationHelper, validator, this);
            }
        }
    }

    public static class FieldIs extends FieldTermEqual {
        public FieldIs(FieldSet field, Term term) {
            super(field, term);
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

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            Preconditions.checkState(scope.keySet().equals(field.datasets()));
            final Map<String, Query> datasetToQuery = new HashMap<>();
            for (final String dataset : field.datasets()) {
                final Query query = Query.newTermQuery(term.toFlamdex(field.datasetFieldName(dataset)));
                datasetToQuery.put(dataset, query);
            }
            return Collections.singletonList(new QueryAction(datasetToQuery, target, positive, negative));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FieldIs fieldIs = (FieldIs) o;
            return Objects.equals(field, fieldIs.field) &&
                    Objects.equals(term, fieldIs.term);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, term);
        }

        @Override
        public String toString() {
            return "FieldIs{" +
                    "field='" + field + '\'' +
                    ", term=" + term +
                    '}';
        }
    }

    public static class FieldIsnt extends FieldTermEqual {
        public FieldIsnt(FieldSet field, Term term) {
            super(field, term);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new Not(new FieldIs(field, term)).asZeroOneMetric(dataset);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return new Not(new FieldIs(field, term)).getExecutionActions(scope, target, positive, negative, groupSupplier);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FieldIsnt fieldIsnt = (FieldIsnt) o;
            return Objects.equals(field, fieldIsnt.field) &&
                    Objects.equals(term, fieldIsnt.term);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, term);
        }

        @Override
        public String toString() {
            return "FieldIsnt{" +
                    "field='" + field + '\'' +
                    ", term=" + term +
                    '}';
        }
    }

    public static class FieldInQuery extends DocFilter {
        public final com.indeed.iql2.language.query.Query query;
        public final FieldSet field;
        public final boolean isNegated; // true if <field> NOT IN <query>

        public FieldInQuery(com.indeed.iql2.language.query.Query query, FieldSet field, boolean isNegated) {
            this.query = query;
            this.field = field;
            this.isNegated = isNegated;
        }


        // TODO: Should this propagate the transformation or just allow `i` to recurse if desired?
        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            throw new UnsupportedOperationException("Must transform the FieldInQuery out before doing a .asZeroOneMetric()");
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            throw new UnsupportedOperationException("Must transform the FieldInQuery out before doing a .getExecutionActions()");
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            // TODO: Should we do anything here? I don't think so...
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FieldInQuery that = (FieldInQuery) o;

            if (isNegated != that.isNegated) return false;
            if (query != null ? !query.equals(that.query) : that.query != null) return false;
            return !(field != null ? !field.equals(that.field) : that.field != null);

        }

        @Override
        public int hashCode() {
            int result = query != null ? query.hashCode() : 0;
            result = 31 * result + (field != null ? field.hashCode() : 0);
            result = 31 * result + (isNegated ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "FieldInQuery{" +
                    "query=" + query +
                    ", field='" + field + '\'' +
                    ", isNegated=" + isNegated +
                    '}';
        }
    }

    public static class Between extends DocFilter {
        public final FieldSet field;
        public final long lowerBound;
        public final long upperBound;

        public Between(FieldSet field, long lowerBound, long upperBound) {
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

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            Preconditions.checkState(scope.keySet().equals(field.datasets()));
            final Map<String, Query> datasetToQuery = new HashMap<>();
            for (final String dataset : field.datasets()) {
                datasetToQuery.put(dataset, Query.newRangeQuery(field.datasetFieldName(dataset), lowerBound, upperBound, false));
            }
            return Collections.<Action>singletonList(new QueryAction(datasetToQuery, target, positive, negative));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            ValidationUtil.validateIntField(ImmutableSet.of(dataset), field.datasetFieldName(dataset), validationHelper, validator, this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Between between = (Between) o;
            return Objects.equals(lowerBound, between.lowerBound) &&
                    Objects.equals(upperBound, between.upperBound) &&
                    Objects.equals(field, between.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, lowerBound, upperBound);
        }

        @Override
        public String toString() {
            return "Between{" +
                    "field='" + field + '\'' +
                    ", lowerBound=" + lowerBound +
                    ", upperBound=" + upperBound +
                    '}';
        }
    }

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
                return Collections.<Action>singletonList(new MetricAction(qualifications, this, target, positive, negative));
            } else {
                return Collections.<Action>singletonList(new MetricAction(scope.keySet(), this, target, positive, negative));
            }
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            m1.validate(dataset, validationHelper, validator);
            m2.validate(dataset, validationHelper, validator);
        }
    }

    public static class MetricEqual extends MetricBinop {
        public MetricEqual(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricEqual(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricEqual(m1, m2);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return super.getExecutionActions(scope, target, positive, negative, groupSupplier);
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
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricEqual that = (MetricEqual) o;
            return Objects.equals(m1, that.m1) &&
                    Objects.equals(m2, that.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return "MetricEqual{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    public static class MetricNotEqual extends MetricBinop {
        public MetricNotEqual(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricNotEqual(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricNotEqual(m1, m2);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return super.getExecutionActions(scope, target, positive, negative, groupSupplier);
            // TODO: Not duplicate logic across these two branches
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
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        private static List<Action> getFieldNotEqualValue(Map<String, String> scope, String field, long value, int target, int positive, int negative) {
            final Query query = Query.newTermQuery(com.indeed.flamdex.query.Term.intTerm(field, value));
            final Query negated = Query.newBooleanQuery(BooleanOp.NOT, Collections.singletonList(query));
            return Collections.<Action>singletonList(new QueryAction(MapUtil.replicate(scope, negated), target, positive, negative));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricNotEqual that = (MetricNotEqual) o;
            return Objects.equals(m1, that.m1) &&
                    Objects.equals(m2, that.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return "MetricNotEqual{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    public static class MetricGt extends MetricBinop {
        public MetricGt(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricGt(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricGt(m1, m2);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return super.getExecutionActions(scope, target, positive, negative, groupSupplier);
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
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricGt metricGt = (MetricGt) o;
            return Objects.equals(m1, metricGt.m1) &&
                    Objects.equals(m2, metricGt.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return "MetricGt{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    public static class MetricGte extends MetricBinop {
        public MetricGte(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricGte(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricGte(m1, m2);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return super.getExecutionActions(scope, target, positive, negative, groupSupplier);
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
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricGte metricGte = (MetricGte) o;
            return Objects.equals(m1, metricGte.m1) &&
                    Objects.equals(m2, metricGte.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return "MetricGte{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    public static class MetricLt extends MetricBinop {
        public MetricLt(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricLt(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricLt(m1, m2);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return super.getExecutionActions(scope, target, positive, negative, groupSupplier);
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
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricLt metricLt = (MetricLt) o;
            return Objects.equals(m1, metricLt.m1) &&
                    Objects.equals(m2, metricLt.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return "MetricLt{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    public static class MetricLte extends MetricBinop {
        public MetricLte(DocMetric m1, DocMetric m2) {
            super(m1, m2);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(new MetricLte(m1.transform(g, i), m2.transform(g, i)));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.MetricLte(m1, m2);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return super.getExecutionActions(scope, target, positive, negative, groupSupplier);
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
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetricLte metricLte = (MetricLte) o;
            return Objects.equals(m1, metricLte.m1) &&
                    Objects.equals(m2, metricLte.m2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m1, m2);
        }

        @Override
        public String toString() {
            return "MetricLte{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    public static class And extends DocFilter {
        public final DocFilter f1;
        public final DocFilter f2;

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

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            final List<Action> result = new ArrayList<>();
            if (target != negative && positive != negative) {
                result.addAll(f1.getExecutionActions(scope, target, target, negative, groupSupplier));
                result.addAll(f2.getExecutionActions(scope, target, positive, negative, groupSupplier));
            } else {
                final int newGroup = groupSupplier.acquire();
                result.addAll(f1.getExecutionActions(scope, target, target, newGroup, groupSupplier));
                result.addAll(f2.getExecutionActions(scope, target, target, newGroup, groupSupplier));
                result.add(new UnconditionalAction(scope.keySet(), target, positive));
                result.add(new UnconditionalAction(scope.keySet(), newGroup, target));
                groupSupplier.release(newGroup);
            }
            return result;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            f1.validate(dataset, validationHelper, validator);
            f2.validate(dataset, validationHelper, validator);
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

    public static class Or extends DocFilter {
        public final DocFilter f1;
        public final DocFilter f2;

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

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            final List<Action> result = new ArrayList<>();
            if (target != positive && positive != negative) {
                result.addAll(f1.getExecutionActions(scope, target, positive, target, groupSupplier));
                result.addAll(f2.getExecutionActions(scope, target, positive, negative, groupSupplier));
            } else {
                final int newGroup = groupSupplier.acquire();
                result.addAll(f1.getExecutionActions(scope, target, newGroup, target, groupSupplier));
                result.addAll(f2.getExecutionActions(scope, target, newGroup, target, groupSupplier));
                result.add(new UnconditionalAction(scope.keySet(), target, negative));
                result.add(new UnconditionalAction(scope.keySet(), newGroup, positive));
                groupSupplier.release(newGroup);
            }
            return result;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            f1.validate(dataset, validationHelper, validator);
            f2.validate(dataset, validationHelper, validator);
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

    public static class Ors extends DocFilter {
        public final List<DocFilter> filters;

        public Ors(List<DocFilter> filters) {
            this.filters = filters;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            final List<DocFilter> transformed = new ArrayList<>();
            for (final DocFilter filter : filters) {
                transformed.add(filter.transform(g, i));
            }
            return i.apply(new Ors(transformed));
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            if (filters.isEmpty()) {
                return new DocMetric.Constant(0);
            }
            DocMetric metric = filters.get(0).asZeroOneMetric(dataset);
            for (int i = 1; i < filters.size(); i++) {
                metric = new DocMetric.Add(metric, filters.get(i).asZeroOneMetric(dataset));
            }
            return new DocMetric.MetricGt(metric, new DocMetric.Constant(0));
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            if (filters.isEmpty()) {
                return Collections.<Action>singletonList(new UnconditionalAction(scope.keySet(), target, negative));
            }
            DocFilter reOrred = filters.get(0);
            for (int i = 1; i < filters.size(); i++) {
                reOrred = new Or(filters.get(i), reOrred);
            }
            return reOrred.getExecutionActions(scope, target, positive, negative, groupSupplier);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            for (final DocFilter filter : filters) {
                filter.validate(dataset, validationHelper, validator);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Ors ors = (Ors) o;
            return Objects.equals(filters, ors.filters);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filters);
        }


        @Override
        public String toString() {
            return "Ors{" +
                    "filters=" + filters +
                    '}';
        }
    }

    public static class Not extends DocFilter {
        public final DocFilter filter;

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

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return filter.getExecutionActions(scope, target, negative, positive, groupSupplier);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            filter.validate(dataset, validationHelper, validator);
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

    public static class Regex extends DocFilter {
        public final FieldSet field;
        public final String regex;

        public Regex(FieldSet field, String regex) {
            this.field = field;
            ValidationUtil.compileRegex(regex);
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

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return Collections.singletonList(new RegexAction(field, regex, target, positive, negative));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            if (!validationHelper.containsStringField(dataset, field.datasetFieldName(dataset))) {
                validator.error(ErrorMessages.missingStringField(dataset, field.datasetFieldName(dataset), this));
            }
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

    public static class NotRegex extends DocFilter {
        public final FieldSet field;
        public final String regex;

        public NotRegex(FieldSet field, String regex) {
            this.field = field;
            ValidationUtil.compileRegex(regex);
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

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return new Not(new Regex(field, regex)).getExecutionActions(scope, target, positive, negative, groupSupplier);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            final String fieldName = field.datasetFieldName(dataset);
            if (!validationHelper.containsStringField(dataset, fieldName)) {
                validator.error(ErrorMessages.missingStringField(dataset, fieldName, this));
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NotRegex notRegex = (NotRegex) o;
            return Objects.equals(field, notRegex.field) &&
                    Objects.equals(regex, notRegex.regex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, regex);
        }

        @Override
        public String toString() {
            return "NotRegex{" +
                    "field='" + field + '\'' +
                    ", regex='" + regex + '\'' +
                    '}';
        }
    }

    public static class FieldEqual extends DocFilter {
        public final FieldSet field1;
        public final FieldSet field2;

        public FieldEqual(FieldSet field1, FieldSet field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.FieldEqualMetric(field1, field2);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return Collections.<Action>singletonList(new MetricAction(scope.keySet(), this, target, positive, negative));
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            ValidationUtil.validateExistenceAndSameFieldType(dataset, field1.datasetFieldName(dataset), field2.datasetFieldName(dataset), validationHelper, validator);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final FieldEqual that = (FieldEqual) o;
            return com.google.common.base.Objects.equal(field1, that.field1) &&
                    com.google.common.base.Objects.equal(field2, that.field2);
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects.hashCode(field1, field2);
        }

        @Override
        public String toString() {
            return "FieldEqual{" +
                    "field1=" + field1 +
                    ", field2=" + field2 +
                    '}';
        }

    }

    public static class Qualified extends DocFilter {
        // TODO: Why is this a List<String> instead of a single string? A per-document thing can only be one dataset!
        public final List<String> scope;
        public final DocFilter filter;

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
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            if (scope.contains(dataset)) {
                filter.validate(dataset, validationHelper, validator);
            }
            ValidationUtil.validateScope(scope, validationHelper, validator);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Qualified qualified = (Qualified) o;
            return Objects.equals(scope, qualified.scope) &&
                    Objects.equals(filter, qualified.filter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(scope, filter);
        }

        @Override
        public String toString() {
            return "Qualified{" +
                    "scope=" + scope +
                    ", filter=" + filter +
                    '}';
        }
    }

    public static class Lucene extends DocFilter {
        public final String query;
        private final ScopedFieldResolver fieldResolver;
        private final DatasetsMetadata datasetsMetadata;

        public Lucene(String query, ScopedFieldResolver fieldResolver, DatasetsMetadata datasetsMetadata) {
            this.query = query;
            this.fieldResolver = fieldResolver;
            this.datasetsMetadata = datasetsMetadata;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
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
            return Collections.<Action>singletonList(new QueryAction(datasetToQuery, target, positive, negative));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            final Query flamdexQuery = ParserUtil.getFlamdexQuery(query, dataset, datasetsMetadata, fieldResolver);
            ValidationUtil.validateQuery(validationHelper, ImmutableMap.of(dataset, flamdexQuery), validator, this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Lucene lucene = (Lucene) o;
            return Objects.equals(query, lucene.query);
        }

        @Override
        public int hashCode() {
            return Objects.hash(query);
        }

        @Override
        public String toString() {
            return "Lucene{" +
                    "query='" + query + '\'' +
                    '}';
        }
    }

    public static class Sample extends DocFilter {
        public final FieldSet field;
        public final long numerator;
        public final long denominator;
        public final String seed;

        public Sample(FieldSet field, long numerator, long denominator, String seed) {
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
            throw new UnsupportedOperationException("Sample::asZeroOneMetric is not implemented");
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            Preconditions.checkState(scope.keySet().equals(field.datasets()));
            return Collections.<Action>singletonList(new SampleAction(field, (double) numerator / denominator, seed, target, positive, negative));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            final String fieldName = field.datasetFieldName(dataset);
            if (!validationHelper.containsField(dataset, fieldName)) {
                validator.error(ErrorMessages.missingField(dataset, fieldName, this));
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Sample sample = (Sample) o;
            return Objects.equals(numerator, sample.numerator) &&
                    Objects.equals(denominator, sample.denominator) &&
                    Objects.equals(field, sample.field) &&
                    Objects.equals(seed, sample.seed);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, numerator, denominator, seed);
        }

        @Override
        public String toString() {
            return "Sample{" +
                    "field='" + field + '\'' +
                    ", numerator=" + numerator +
                    ", denominator=" + denominator +
                    ", seed='" + seed + '\'' +
                    '}';
        }
    }

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
            return i.apply(new SampleDocMetric(metric.transform(g, i), numerator, denominator, seed));
        }

        @Override
        public DocMetric asZeroOneMetric(final String dataset) {
            throw new UnsupportedOperationException("SampleDocMetric::asZeroOneMetric is not implemented");
        }

        @Override
        public List<Action> getExecutionActions(final Map<String, String> scope,
                                                final int target,
                                                final int positive,
                                                final int negative,
                                                final GroupSupplier groupSupplier) {
            final Map<String, List<String>> perDatasetPushes = Maps.newHashMapWithExpectedSize(scope.size());
            for (final String dataset : scope.keySet()) {
                perDatasetPushes.put(dataset, metric.getPushes(dataset));
            }
            return Collections.singletonList(new SampleMetricAction(perDatasetPushes, (double) numerator / denominator, seed, target, positive, negative));
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(final String dataset,
                             final ValidationHelper validationHelper,
                             final Validator validator) {
            metric.validate(dataset, validationHelper, validator);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if ((o == null) || (getClass() != o.getClass())) {
                return false;
            }
            final SampleDocMetric sample = (SampleDocMetric) o;
            return Objects.equals(metric, sample.metric) &&
                    Objects.equals(numerator, sample.numerator) &&
                    Objects.equals(denominator, sample.denominator) &&
                    Objects.equals(seed, sample.seed);
        }

        @Override
        public int hashCode() {
            return Objects.hash(metric, numerator, denominator, seed);
        }

        @Override
        public String toString() {
            return "SampleDocMetric{" +
                    "metric=" + metric +
                    ", numerator=" + numerator +
                    ", denominator=" + denominator +
                    ", seed='" + seed + '\'' +
                    '}';
        }
    }

    public static class Always extends DocFilter {
        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.Constant(1);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return Collections.<Action>singletonList(new UnconditionalAction(scope.keySet(), target, positive));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {

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
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            return new DocMetric.Constant(0);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            return Collections.<Action>singletonList(new UnconditionalAction(scope.keySet(), target, negative));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {

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

    public static class StringFieldIn extends DocFilter {
        public final DatasetsMetadata datasetsMetadata;
        public final FieldSet field;
        public final Set<String> terms;

        public StringFieldIn(DatasetsMetadata datasetsMetadata, FieldSet field, Set<String> terms) {
            this.datasetsMetadata = datasetsMetadata;
            if (terms.isEmpty()) {
                throw new IqlKnownException.ParseErrorException("Cannot have empty set of terms!");
            }
            this.field = field;
            this.terms = ImmutableSet.copyOf(terms);
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            DocFilter filter = null;
            for (final String term : terms) {
                if (filter == null) {
                    filter = new FieldIs(field, Term.term(term));
                } else {
                    filter = new Or(filter, new FieldIs(field, Term.term(term)));
                }
            }
            return filter.asZeroOneMetric(dataset);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            Preconditions.checkState(scope.keySet().equals(field.datasets()));
            // TODO: Should this care about the keyword analyzer fields?
            return Collections.singletonList(new StringOrAction(field, terms, target, positive, negative));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            final String fieldName = this.field.datasetFieldName(dataset);
            if (!validationHelper.containsStringField(dataset, fieldName)) {
                validator.error(ErrorMessages.missingStringField(dataset, fieldName, this));
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StringFieldIn that = (StringFieldIn) o;
            return Objects.equals(field, that.field) &&
                    Objects.equals(terms, that.terms);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, terms);
        }

        @Override
        public String toString() {
            return "StringFieldIn{" +
                    "field='" + field + '\'' +
                    ", terms=" + terms +
                    '}';
        }
    }

    public static class IntFieldIn extends DocFilter {
        public final FieldSet field;
        public final Set<Long> terms;
        public final DatasetsMetadata datasetsMetadata;

        public IntFieldIn(DatasetsMetadata datasetsMetadata, FieldSet field, Set<Long> terms) {
            if (terms.isEmpty()) {
                throw new IqlKnownException.ParseErrorException("Cannot have empty set of terms!");
            }
            this.field = field;
            this.terms = new LongOpenHashSet(terms);
            this.datasetsMetadata = datasetsMetadata;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
        }

        @Override
        public DocMetric asZeroOneMetric(String dataset) {
            DocFilter filter = null;
            for (final long term : terms) {
                if (filter == null) {
                    filter = new FieldIs(field, Term.term(term));
                } else {
                    filter = new Or(filter, new FieldIs(field, Term.term(term)));
                }
            }
            return filter.asZeroOneMetric(dataset);
        }

        @Override
        public List<Action> getExecutionActions(Map<String, String> scope, int target, int positive, int negative, GroupSupplier groupSupplier) {
            Preconditions.checkState(scope.keySet().equals(field.datasets()));
            return Collections.singletonList(new IntOrAction(field, terms, target, positive, negative));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
            final String fieldName = field.datasetFieldName(dataset);
            if (!validationHelper.containsField(dataset, fieldName)) {
                validator.error(ErrorMessages.missingField(dataset, fieldName, this));
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IntFieldIn that = (IntFieldIn) o;
            return Objects.equals(field, that.field) &&
                    Objects.equals(terms, that.terms);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, terms);
        }

        @Override
        public String toString() {
            return "IntFieldIn{" +
                    "field='" + field + '\'' +
                    ", terms=" + terms +
                    '}';
        }
    }

    public static class ExplainFieldIn extends DocFilter {
        public final com.indeed.iql2.language.query.Query query;
        private final FieldSet field;
        private final boolean isNegated;

        public ExplainFieldIn(final com.indeed.iql2.language.query.Query query, FieldSet field, boolean isNegated) {
            this.query = query;
            this.field = field;
            this.isNegated = isNegated;
        }

        @Override
        public DocFilter transform(Function<DocMetric, DocMetric> g, Function<DocFilter, DocFilter> i) {
            return i.apply(this);
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
            return Collections.singletonList(new StringOrAction(field, fakeTerms, target, positive, negative));
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public void validate(String dataset, ValidationHelper validationHelper, Validator validator) {
        }

        @Override
        public String toString() {
            return "ExplainFieldIn{" +
                    "query=" + query +
                    ", field=" + field +
                    '}';
        }
    }

}
