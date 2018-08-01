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
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;
import com.indeed.iql2.execution.metrics.aggregate.DocumentLevelMetric;
import com.indeed.iql2.execution.metrics.aggregate.MultiPerGroupConstant;
import com.indeed.iql2.execution.metrics.aggregate.ParentLag;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

// TODO: PerGroupConstants, IfThenElse ????
public abstract class AggregateMetric extends AbstractPositional {

    public static final String PRECOMPUTED_EXCEPTION = "Should be extracted by ExtractPrecomputed";

    public interface Visitor<T, E extends Throwable> {
        T visit(Add add) throws E;
        T visit(Log log) throws E;
        T visit(Negate negate) throws E;
        T visit(Abs abs) throws E;
        T visit(Subtract subtract) throws E;
        T visit(Multiply multiply) throws E;
        T visit(Divide divide) throws E;
        T visit(Modulus modulus) throws E;
        T visit(Power power) throws E;
        T visit(Parent parent) throws E;
        T visit(Lag lag) throws E;
        T visit(IterateLag iterateLag) throws E;
        T visit(Window window) throws E;
        T visit(Qualified qualified) throws E;
        T visit(DocStatsPushes docStatsPushes) throws E;
        T visit(DocStats docStats) throws E;
        T visit(Constant constant) throws E;
        T visit(Percentile percentile) throws E;
        T visit(Running running) throws E;
        T visit(Distinct distinct) throws E;
        T visit(Named named) throws E;
        T visit(GroupStatsLookup groupStatsLookup) throws E;
        T visit(GroupStatsMultiLookup groupStatsMultiLookup) throws E;
        T visit(SumAcross sumAcross) throws E;
        T visit(IfThenElse ifThenElse) throws E;
        T visit(FieldMin fieldMin) throws E;
        T visit(FieldMax fieldMax) throws E;
        T visit(Min min) throws E;
        T visit(Max max) throws E;
        T visit(Bootstrap bootstrap) throws E;
        T visit(DivideByCount divideByCount) throws E;
    }

    public abstract <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E;

    public abstract AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction);
    public abstract AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f);
    public abstract void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator);
    public abstract boolean isOrdered();
    public boolean requiresFTGS() { return false; }
    public abstract com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(
            Function<String, PerGroupConstant> namedMetricLookup,
            GroupKeySet groupKeySet
    );

    public abstract static class Unop extends AggregateMetric {
        public final AggregateMetric m1;

        public Unop(AggregateMetric m1) {
            this.m1 = m1;
        }

        @Override
        public boolean isOrdered() {
            return m1.isOrdered();
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            m1.validate(scope, validationHelper, validator);
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

    public static class Log extends Unop {
        public Log(AggregateMetric m1) {
            super(m1);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Log(m1.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Log(f.apply(m1));
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Log(m1.toExecutionMetric(namedMetricLookup, groupKeySet));
        }
    }

    public static class Negate extends Unop {
        public Negate(AggregateMetric m1) {
            super(m1);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Negate(m1.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Negate(f.apply(m1));
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Subtract(
                    new com.indeed.iql2.execution.metrics.aggregate.Constant(0.0),
                    m1.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
        }
    }

    public static class Abs extends Unop {
        public Abs(AggregateMetric m1) {
            super(m1);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Abs(m1.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Abs(f.apply(m1));
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Abs(m1.toExecutionMetric(namedMetricLookup, groupKeySet));
        }
    }

    public abstract static class Binop extends AggregateMetric {
        public final AggregateMetric m1;
        public final AggregateMetric m2;

        public Binop(AggregateMetric m1, AggregateMetric m2) {
            this.m1 = m1;
            this.m2 = m2;
        }

        @Override
        public boolean isOrdered() {
            return m1.isOrdered() || m2.isOrdered();
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            m1.validate(scope, validationHelper, validator);
            m2.validate(scope, validationHelper, validator);
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
    }

    public static class Add extends Binop {
        public Add(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Add(m1.transform(f, g, h, i, groupByFunction), m2.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Add(f.apply(m1), f.apply(m2));
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Add(
                    m1.toExecutionMetric(namedMetricLookup, groupKeySet),
                    m2.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
        }
    }

    public static class Subtract extends Binop {
        public Subtract(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Subtract(m1.transform(f, g, h, i, groupByFunction), m2.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Subtract(f.apply(m1), f.apply(m2));
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Subtract(
                    m1.toExecutionMetric(namedMetricLookup, groupKeySet),
                    m2.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
        }
    }

    public static class Multiply extends Binop {
        public Multiply(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Multiply(m1.transform(f, g, h, i, groupByFunction), m2.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Multiply(f.apply(m1), f.apply(m2));
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Multiply(
                    m1.toExecutionMetric(namedMetricLookup, groupKeySet),
                    m2.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
        }
    }

    public static class Divide extends Binop {
        public Divide(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Divide(m1.transform(f, g, h, i, groupByFunction), m2.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Divide(f.apply(m1), f.apply(m2));
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Divide(
                    m1.toExecutionMetric(namedMetricLookup, groupKeySet),
                    m2.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
        }
    }

    public static class Modulus extends Binop {
        public Modulus(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Modulus(m1.transform(f, g, h, i, groupByFunction), m2.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Modulus(f.apply(m1), f.apply(m2));
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Modulus(
                    m1.toExecutionMetric(namedMetricLookup, groupKeySet),
                    m2.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
        }
    }

    public static class Power extends Binop {
        public Power(AggregateMetric m1, AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Power(m1.transform(f, g, h, i, groupByFunction), m2.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Power(f.apply(m1), f.apply(m2));
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Power(
                    m1.toExecutionMetric(namedMetricLookup, groupKeySet),
                    m2.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
        }
    }

    public abstract static class RequiresFTGSMetric extends AggregateMetric {
        @Override
        public boolean requiresFTGS() {
            return true;
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            throw new IllegalStateException("Cannot serialize " + getClass().getSimpleName() + " -- it should be removed by ExtractPrecomputed!");
        }
    }

    public static class Parent extends AggregateMetric {
        public final AggregateMetric metric;

        public Parent(AggregateMetric metric) {
            this.metric = metric;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            metric.validate(scope, validationHelper, validator);
        }

        @Override
        public boolean isOrdered() {
            return metric.isOrdered();
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            throw new IllegalStateException(PRECOMPUTED_EXCEPTION);
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

    public static class Lag extends AggregateMetric {
        public final int lag;
        public final AggregateMetric metric;

        public Lag(int lag, AggregateMetric metric) {
            this.lag = lag;
            this.metric = metric;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            metric.validate(scope, validationHelper, validator);
        }

        @Override
        public boolean isOrdered() {
            return true;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new ParentLag(lag, metric.toExecutionMetric(namedMetricLookup, groupKeySet));
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

    public static class DivideByCount extends AggregateMetric {
        public final AggregateMetric metric;

        public DivideByCount(AggregateMetric metric) {
            this.metric = metric;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new DivideByCount(metric.transform(f, g, h, i, groupByFunction)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new DivideByCount(f.apply(metric));
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            throw new UnsupportedOperationException("Cannot / should not validate DivideByCount -- ExtractPrecomputed should transfer it to Divide!");
        }

        @Override
        public boolean isOrdered() {
            return metric.isOrdered();
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            throw new IllegalStateException(PRECOMPUTED_EXCEPTION);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final DivideByCount that = (DivideByCount) o;
            return com.google.common.base.Objects.equal(metric, that.metric);
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects.hashCode(metric);
        }

        @Override
        public String toString() {
            return "DivideByCount{" +
                    "metric=" + metric +
                    '}';
        }
    }

    public static class IterateLag extends AggregateMetric {
        public final int lag;
        public final AggregateMetric metric;

        public IterateLag(int lag, AggregateMetric metric) {
            this.lag = lag;
            this.metric = metric;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            metric.validate(scope, validationHelper, validator);
        }

        @Override
        public boolean isOrdered() {
            return true;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.IterateLag(
                    lag,
                    metric.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
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

    public static class Window extends AggregateMetric {
        public final int window;
        public final AggregateMetric metric;

        public Window(int window, AggregateMetric metric) {
            this.window = window;
            this.metric = metric;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            metric.validate(scope, validationHelper, validator);
        }

        @Override
        public boolean isOrdered() {
            return true;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Window(
                    window,
                    metric.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
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

    public static class Qualified extends AggregateMetric {
        public final List<String> scope;
        public final AggregateMetric metric;

        public Qualified(List<String> scope, AggregateMetric metric) {
            this.scope = scope;
            this.metric = metric;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            final Set<String> thisScope = Sets.newHashSet(this.scope);
            if (!scope.containsAll(thisScope)) {
                validator.error("Qualified scope is not a subset of outer scope! qualified scope = [" + this.scope + "], outer scope = [" + scope + "]");
            }
            metric.validate(thisScope, validationHelper, validator);
            ValidationUtil.validateScope(this.scope, validationHelper, validator);
        }

        @Override
        public boolean isOrdered() {
            return metric.isOrdered();
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            throw new IllegalStateException(PRECOMPUTED_EXCEPTION);
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

    public static class DocStatsPushes extends AggregateMetric {
        public final String dataset;
        public final DocMetric.PushableDocMetric pushes;

        public DocStatsPushes(String dataset, DocMetric.PushableDocMetric pushes) {
            this.dataset = dataset;
            this.pushes = pushes;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            pushes.validate(dataset, validationHelper, validator);
            ValidationUtil.validateDataset(dataset, validationHelper, validator);
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new DocumentLevelMetric(dataset, pushes.getPushes(dataset));
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

    /**
     * DocStats in which there is no explicit sum, but a single atomic, unambiguous atom.
     */
    public static class DocStats extends AggregateMetric {
        public final DocMetric docMetric;

        public DocStats(DocMetric docMetric) {
            this.docMetric = docMetric;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new DocStats(g.apply(docMetric)));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            for (final String dataset : scope) {
                docMetric.validate(dataset, validationHelper, validator);
            }
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            throw new IllegalStateException(PRECOMPUTED_EXCEPTION);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DocStats that = (DocStats) o;
            return Objects.equals(docMetric, that.docMetric);
        }

        @Override
        public int hashCode() {
            return Objects.hash(docMetric);
        }

        @Override
        public String toString() {
            return "DocStats{" +
                    "docMetric=" + docMetric +
                    '}';
        }
    }

    public static class Constant extends AggregateMetric {
        public final double value;

        public Constant(double value) {
            this.value = value;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {

        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Constant(value);
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

    public static class Percentile extends RequiresFTGSMetric {
        public final Positioned<String> field;
        public final double percentile;

        public Percentile(Positioned<String> field, double percentile) {
            this.field = field;
            this.percentile = percentile;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            throw new IllegalStateException(PRECOMPUTED_EXCEPTION);
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

    public static class Running extends AggregateMetric {
        public final int offset;
        public final AggregateMetric metric;

        public Running(int offset, AggregateMetric metric) {
            this.offset = offset;
            this.metric = metric;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            metric.validate(scope, validationHelper, validator);
        }

        @Override
        public boolean isOrdered() {
            return true;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Running(
                    metric.toExecutionMetric(namedMetricLookup, groupKeySet),
                    offset
            );
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

    public static class Distinct extends RequiresFTGSMetric {
        public final Positioned<String> field;
        public final Optional<AggregateFilter> filter;
        public final Optional<Integer> windowSize;

        public Distinct(Positioned<String> field, Optional<AggregateFilter> filter, Optional<Integer> windowSize) {
            this.field = field;
            this.filter = filter;
            this.windowSize = windowSize;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            throw new IllegalStateException(PRECOMPUTED_EXCEPTION);
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

    public static class Named extends AggregateMetric {
        public final AggregateMetric metric;
        public final Positioned<String> name;

        public Named(AggregateMetric metric, Positioned<String> name) {
            this.metric = metric;
            this.name = name;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            metric.validate(scope, validationHelper, validator);
        }

        @Override
        public boolean isOrdered() {
            return metric.isOrdered();
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            throw new IllegalStateException(PRECOMPUTED_EXCEPTION);
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

    public static class GroupStatsLookup extends AggregateMetric {
        public final String name;

        public GroupStatsLookup(String name) {
            this.name = name;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {

        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return namedMetricLookup.apply(name);
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

    public static class GroupStatsMultiLookup extends AggregateMetric {
        public final List<String> names;

        public GroupStatsMultiLookup(List<String> names) {
            this.names = names;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {

        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            final List<double[]> metrics = new ArrayList<>();
            for (final String name : names) {
                metrics.add(namedMetricLookup.apply(name).values);
            }
            return new MultiPerGroupConstant(metrics);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GroupStatsMultiLookup that = (GroupStatsMultiLookup) o;
            return com.google.common.base.Objects.equal(names, that.names);
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects.hashCode(names);
        }

        @Override
        public String toString() {
            return "GroupStatsMultiLookup{" +
                    "names=" + names +
                    '}';
        }
    }

    public static class SumAcross extends RequiresFTGSMetric {
        public final GroupBy groupBy;
        public final AggregateMetric metric;

        public SumAcross(GroupBy groupBy, AggregateMetric metric) {
            this.groupBy = groupBy;
            this.metric = metric;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            throw new IllegalStateException(PRECOMPUTED_EXCEPTION);
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

    public static class IfThenElse extends AggregateMetric {
        public final AggregateFilter condition;
        public final AggregateMetric trueCase;
        public final AggregateMetric falseCase;

        public IfThenElse(AggregateFilter condition, AggregateMetric trueCase, AggregateMetric falseCase) {
            this.condition = condition;
            this.trueCase = trueCase;
            this.falseCase = falseCase;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            condition.validate(scope, validationHelper, validator);
            trueCase.validate(scope, validationHelper, validator);
            falseCase.validate(scope, validationHelper, validator);
        }

        @Override
        public boolean isOrdered() {
            return condition.isOrdered() || trueCase.isOrdered() || falseCase.isOrdered();
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.IfThenElse(
                    condition.toExecutionFilter(namedMetricLookup, groupKeySet),
                    trueCase.toExecutionMetric(namedMetricLookup, groupKeySet),
                    falseCase.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
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

    public static class FieldMin extends RequiresFTGSMetric {
        public final Positioned<String> field;

        public FieldMin(Positioned<String> field) {
            this.field = field;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            throw new IllegalStateException(PRECOMPUTED_EXCEPTION);
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

    public static class FieldMax extends RequiresFTGSMetric {
        public final Positioned<String> field;

        public FieldMax(Positioned<String> field) {
            this.field = field;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            throw new IllegalStateException(PRECOMPUTED_EXCEPTION);
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

    public static class Min extends AggregateMetric {
        public final List<AggregateMetric> metrics;

        public Min(List<AggregateMetric> metrics) {
            this.metrics = metrics;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            for (final AggregateMetric metric : metrics) {
                metric.validate(scope, validationHelper, validator);
            }

            if (metrics.size() < 2) {
                validator.error("MIN requires at least 2 arguments. Did you mean FIELD_MIN()?");
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
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Min(
                    metrics
                        .stream()
                        .map(x -> x.toExecutionMetric(namedMetricLookup, groupKeySet))
                        .collect(Collectors.toList())
            );
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

    public static class Max extends AggregateMetric {
        public final List<AggregateMetric> metrics;

        public Max(List<AggregateMetric> metrics) {
            this.metrics = metrics;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, Validator validator) {
            for (final AggregateMetric metric : metrics) {
                metric.validate(scope, validationHelper, validator);
            }

            if (metrics.size() < 2) {
                validator.error("MAX requires at least 2 arguments. Did you mean FIELD_MAX()?");
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
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Max(
                    metrics
                        .stream()
                        .map(x -> x.toExecutionMetric(namedMetricLookup, groupKeySet))
                        .collect(Collectors.toList())
            );

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

    public static class Bootstrap extends RequiresFTGSMetric {
        public final Positioned<String> field;
        public final Optional<AggregateFilter> filter;
        public final String seed;
        public final AggregateMetric metric;
        public final int numBootstraps;
        public final List<String> varargs;

        public Bootstrap(Positioned<String> field, Optional<AggregateFilter> filter, String seed, AggregateMetric metric, int numBootstraps, List<String> varargs) {
            this.field = field;
            this.filter = filter;
            this.seed = seed;
            this.metric = metric;
            this.numBootstraps = numBootstraps;
            this.varargs = varargs;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            final Optional<AggregateFilter> filter;
            if (this.filter.isPresent()) {
                filter = Optional.of(this.filter.get().transform(f, g, h, i, groupByFunction));
            } else {
                filter = Optional.absent();
            }
            return f.apply(new Bootstrap(field, filter, seed, metric.transform(f, g, h, i, groupByFunction), numBootstraps, varargs));
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Bootstrap(field, filter, seed, f.apply(metric), numBootstraps, varargs);
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            throw new IllegalStateException(PRECOMPUTED_EXCEPTION);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Bootstrap bootstrap = (Bootstrap) o;
            return numBootstraps == bootstrap.numBootstraps &&
                    com.google.common.base.Objects.equal(field, bootstrap.field) &&
                    com.google.common.base.Objects.equal(filter, bootstrap.filter) &&
                    com.google.common.base.Objects.equal(seed, bootstrap.seed) &&
                    com.google.common.base.Objects.equal(metric, bootstrap.metric) &&
                    com.google.common.base.Objects.equal(varargs, bootstrap.varargs);
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects.hashCode(field, filter, seed, metric, numBootstraps, varargs);
        }

        @Override
        public String toString() {
            return "Bootstrap{" +
                    "field=" + field +
                    ", filter=" + filter +
                    ", seed='" + seed + '\'' +
                    ", metric=" + metric +
                    ", numBootstraps=" + numBootstraps +
                    ", varargs=" + varargs +
                    '}';
        }
    }
}
