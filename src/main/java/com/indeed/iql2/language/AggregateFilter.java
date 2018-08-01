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
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.util.ErrorMessages;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;

import java.util.Objects;
import java.util.Set;

public abstract class AggregateFilter extends AbstractPositional {
    public interface Visitor<T, E extends Throwable> {
        T visit(TermIs termIs) throws E;
        T visit(TermRegex termIsRegex) throws E;
        T visit(MetricIs metricIs) throws E;
        T visit(MetricIsnt metricIsnt) throws E;
        T visit(Gt gt) throws E;
        T visit(Gte gte) throws E;
        T visit(Lt lt) throws E;
        T visit(Lte lte) throws E;
        T visit(And and) throws E;
        T visit(Or or) throws E;
        T visit(Not not) throws E;
        T visit(Regex regex) throws E;
        T visit(Always always) throws E;
        T visit(Never never) throws E;
        T visit(IsDefaultGroup isDefaultGroup) throws E;
    }

    public abstract <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E;

    public abstract AggregateFilter transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction);

    public abstract AggregateFilter traverse1(Function<AggregateMetric, AggregateMetric> f);

    public abstract void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator);

    public abstract boolean isOrdered();

    public abstract com.indeed.iql2.execution.AggregateFilter toExecutionFilter(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet);

    public static class TermIs extends AggregateFilter {
        public final Term term;

        public TermIs(Term term) {
            this.term = term;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.AggregateFilter toExecutionFilter(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.AggregateFilter.TermEquals(term.toExecutionTerm());
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

    public static class TermRegex extends AggregateFilter {
        public final Term term;

        public TermRegex(Term term) {
            this.term = term;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.AggregateFilter toExecutionFilter(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.AggregateFilter.TermEqualsRegex(term.toExecutionTerm());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TermRegex termRegex = (TermRegex) o;
            return Objects.equals(term, termRegex.term);
        }

        @Override
        public int hashCode() {
            return Objects.hash(term);
        }

        @Override
        public String toString() {
            return "TermRegex{" +
                    "term=" + term +
                    '}';
        }
    }

    public static class MetricIs extends AggregateFilter {
        public final AggregateMetric m1;
        public final AggregateMetric m2;

        public MetricIs(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            m1.validate(scope, validationHelper, validator);
            m2.validate(scope, validationHelper, validator);
            if (m1.equals(new AggregateMetric.Constant(1.0))) {
                validator.warn("Direct comparison of aggregate [" + m2 + "] to 1 is likely in error. Consider what happens when multiple docs occur in the aggregate");
            }
            if (m2.equals(new AggregateMetric.Constant(1.0))) {
                validator.warn("Direct comparison of aggregate [" + m1 + "] to 1 is likely in error. Consider what happens when multiple docs occur in the aggregate");
            }
        }

        @Override
        public boolean isOrdered() {
            return m1.isOrdered() || m2.isOrdered();
        }

        @Override
        public com.indeed.iql2.execution.AggregateFilter toExecutionFilter(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.AggregateFilter.MetricEquals(
                    m1.toExecutionMetric(namedMetricLookup, groupKeySet),
                    m2.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
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

    public static class MetricIsnt extends AggregateFilter {
        public final AggregateMetric m1;
        public final AggregateMetric m2;

        public MetricIsnt(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            m1.validate(scope, validationHelper, validator);
            m2.validate(scope, validationHelper, validator);
        }

        @Override
        public boolean isOrdered() {
            return m1.isOrdered() || m2.isOrdered();
        }

        @Override
        public com.indeed.iql2.execution.AggregateFilter toExecutionFilter(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.AggregateFilter.MetricNotEquals(
                    m1.toExecutionMetric(namedMetricLookup, groupKeySet),
                    m2.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
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

    public static class Gt extends AggregateFilter {
        public final AggregateMetric m1;
        public final AggregateMetric m2;

        public Gt(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            m1.validate(scope, validationHelper, validator);
            m2.validate(scope, validationHelper, validator);
        }

        @Override
        public boolean isOrdered() {
            return m1.isOrdered() || m2.isOrdered();
        }

        @Override
        public com.indeed.iql2.execution.AggregateFilter toExecutionFilter(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.AggregateFilter.GreaterThan(
                    m1.toExecutionMetric(namedMetricLookup, groupKeySet),
                    m2.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
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

    public static class Gte extends AggregateFilter {
        public final AggregateMetric m1;
        public final AggregateMetric m2;

        public Gte(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            m1.validate(scope, validationHelper, validator);
            m2.validate(scope, validationHelper, validator);
        }

        @Override
        public boolean isOrdered() {
            return m1.isOrdered() || m2.isOrdered();
        }

        @Override
        public com.indeed.iql2.execution.AggregateFilter toExecutionFilter(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.AggregateFilter.GreaterThanOrEquals(
                    m1.toExecutionMetric(namedMetricLookup, groupKeySet),
                    m2.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
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

    public static class Lt extends AggregateFilter {
        public final AggregateMetric m1;
        public final AggregateMetric m2;

        public Lt(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            m1.validate(scope, validationHelper, validator);
            m2.validate(scope, validationHelper, validator);
        }

        @Override
        public boolean isOrdered() {
            return m1.isOrdered() || m2.isOrdered();
        }

        @Override
        public com.indeed.iql2.execution.AggregateFilter toExecutionFilter(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.AggregateFilter.LessThan(
                    m1.toExecutionMetric(namedMetricLookup, groupKeySet),
                    m2.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
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

    public static class Lte extends AggregateFilter {
        public final AggregateMetric m1;
        public final AggregateMetric m2;

        public Lte(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            m1.validate(scope, validationHelper, validator);
            m2.validate(scope, validationHelper, validator);
        }

        @Override
        public boolean isOrdered() {
            return m1.isOrdered() || m2.isOrdered();
        }

        @Override
        public com.indeed.iql2.execution.AggregateFilter toExecutionFilter(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.AggregateFilter.LessThanOrEquals(
                    m1.toExecutionMetric(namedMetricLookup, groupKeySet),
                    m2.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
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

    public static class And extends AggregateFilter {
        public final AggregateFilter f1;
        public final AggregateFilter f2;

        public And(AggregateFilter f1, AggregateFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            f1.validate(scope, validationHelper, validator);
            f2.validate(scope, validationHelper, validator);
        }

        @Override
        public boolean isOrdered() {
            return f1.isOrdered() || f2.isOrdered();
        }

        @Override
        public com.indeed.iql2.execution.AggregateFilter toExecutionFilter(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.AggregateFilter.And(
                    f1.toExecutionFilter(namedMetricLookup, groupKeySet),
                    f2.toExecutionFilter(namedMetricLookup, groupKeySet)
            );
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

    public static class Or extends AggregateFilter {
        public final AggregateFilter f1;
        public final AggregateFilter f2;

        public Or(AggregateFilter f1, AggregateFilter f2) {
            this.f1 = f1;
            this.f2 = f2;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            f1.validate(scope, validationHelper, validator);
            f2.validate(scope, validationHelper, validator);
        }

        @Override
        public boolean isOrdered() {
            return f1.isOrdered() || f2.isOrdered();
        }

        @Override
        public com.indeed.iql2.execution.AggregateFilter toExecutionFilter(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.AggregateFilter.Or(
                    f1.toExecutionFilter(namedMetricLookup, groupKeySet),
                    f2.toExecutionFilter(namedMetricLookup, groupKeySet)
            );
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

    public static class Not extends AggregateFilter {
        public final AggregateFilter filter;

        public Not(AggregateFilter filter) {
            this.filter = filter;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            filter.validate(scope, validationHelper, validator);
        }

        @Override
        public boolean isOrdered() {
            return filter.isOrdered();
        }

        @Override
        public com.indeed.iql2.execution.AggregateFilter toExecutionFilter(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.AggregateFilter.Not(
                    filter.toExecutionFilter(namedMetricLookup, groupKeySet)
            );
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

    public static class Regex extends AggregateFilter {
        public final Positioned<String> field;
        public final String regex;

        public Regex(Positioned<String> field, String regex) {
            this.field = field;
            ValidationUtil.compileRegex(regex);
            this.regex = regex;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            for (final String dataset : scope) {
                if (!validationHelper.containsField(dataset, field.unwrap())) {
                    validator.error(ErrorMessages.missingField(dataset, field.unwrap(), this));
                }
            }
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.AggregateFilter toExecutionFilter(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.AggregateFilter.RegexFilter(regex);
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

    public static class Always extends AggregateFilter {
        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {

        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.AggregateFilter toExecutionFilter(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.AggregateFilter.Constant(true);
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

    public static class Never extends AggregateFilter {
        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {

        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.AggregateFilter toExecutionFilter(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.AggregateFilter.Constant(false);
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

    public static class IsDefaultGroup extends AggregateFilter {
        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {

        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.AggregateFilter toExecutionFilter(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.AggregateFilter.IsDefaultGroup(groupKeySet);
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
