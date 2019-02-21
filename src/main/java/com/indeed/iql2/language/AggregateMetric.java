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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.DocumentLevelMetric;
import com.indeed.iql2.execution.metrics.aggregate.ParentLag;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

// TODO: PerGroupConstants, IfThenElse ????
public abstract class AggregateMetric extends AbstractPositional {

    private static final String PRECOMPUTED_EXCEPTION = "Should be extracted by ExtractPrecomputed";

    public interface Visitor<T, E extends Throwable> {
        T visit(Add add) throws E;
        T visit(Log log) throws E;
        T visit(Negate negate) throws E;
        T visit(Abs abs) throws E;
        T visit(Floor floor) throws E;
        T visit(Ceil ceil) throws E;
        T visit(Round round) throws E;
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
        T visit(NeedsSubstitution needsSubstitution) throws E;
        T visit(GroupStatsLookup groupStatsLookup) throws E;
        T visit(SumAcross sumAcross) throws E;
        T visit(IfThenElse ifThenElse) throws E;
        T visit(FieldMin fieldMin) throws E;
        T visit(FieldMax fieldMax) throws E;
        T visit(Min min) throws E;
        T visit(Max max) throws E;
        T visit(DivideByCount divideByCount) throws E;
    }

    public abstract <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E;

    /**
     * @see Query#transform(Function, Function, Function, Function, Function)
     */
    public abstract AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction);

    /**
     * Implements a top-down short-circuiting replacement operation for the shallowest {@link AggregateMetric}s.
     * <p>
     * Further traversal/transformation of the objects found and their children can be performed
     * within the passed in function.
     * <p>
     * More precisely:
     * Performs a top down traversal of this AggregateMetric, searching for the first {@link AggregateMetric} on all branches.
     * Replaces the top {@link AggregateMetric} found on each branch with the result of applying the given function
     * to the metric, and rebuilds the containing objects.
     * Propagates all source information to the transformed and rebuilt objects.
     * <p>
     * When implementing traverse1() methods on new classes, the recipe is:
     * <ol>
     * <li>If this object contains any direct children that are {@link AggregateMetric} implementations,
     *    replace all of them with the result of applying f.apply(child)
     * <li>For all replaced children, propagate any source information ({@link Positional}) to
     *    the newly created object
     * <li>For all children that are NOT {@link AggregateMetric}s, call traverse1() on them if such a method exists, to
     *    replace those objects
     * <li>Construct a new version of this, where all appropriate children have been replaced with
     *    the newly constructed versions
     * <li>Propagate any source information ({@link Positional}) to the newly constructed version
     * <li>Return the object with all transforms and source propagation performed
     * </ol><p>
     * It may seem like this should not exist on {@link AggregateMetric}, but in fact it can be used
     * for pushing down a given function to lower levels, without having to visit() all possible alternatives
     * at the present level.
     *
     * @see AggregateFilter#traverse1
     * @see com.indeed.iql2.language.execution.ExecutionStep#traverse1
     *
     * @param f function to transform the shallowest AggregateMetrics by
     * @return the transformed AggregateMetric
     */
    public abstract AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f);
    public abstract void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector);
    public abstract boolean isOrdered();
    public boolean requiresFTGS() { return false; }
    public abstract com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(
            Function<String, PerGroupConstant> namedMetricLookup,
            GroupKeySet groupKeySet
    );

    @Override
    public abstract boolean equals(final Object other);
    @Override
    public abstract int hashCode();
    @Override
    public abstract String toString();

    @Override
    public AggregateMetric copyPosition(final Positional positional) {
        super.copyPosition(positional);
        return this;
    }

    @EqualsAndHashCode(callSuper = false)
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            m1.validate(scope, validationHelper, errorCollector);
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "{" +
                    "m1=" + m1 +
                    '}';
        }
    }

    @EqualsAndHashCode(callSuper = true)
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
            return f.apply(new Log(m1.transform(f, g, h, i, groupByFunction))).copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Log(f.apply(m1).copyPosition(m1)).copyPosition(this);
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Log(m1.toExecutionMetric(namedMetricLookup, groupKeySet));
        }
    }

    @EqualsAndHashCode(callSuper = true)
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
            return f.apply(new Negate(m1.transform(f, g, h, i, groupByFunction))).copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Negate(f.apply(m1).copyPosition(m1));
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Subtract(
                    new com.indeed.iql2.execution.metrics.aggregate.Constant(0.0),
                    m1.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
        }
    }

    @EqualsAndHashCode(callSuper = true)
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
            return f.apply(new Abs(m1.transform(f, g, h, i, groupByFunction))).copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Abs(f.apply(m1).copyPosition(m1)).copyPosition(this);
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Abs(m1.toExecutionMetric(namedMetricLookup, groupKeySet));
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class Floor extends FactorUnop {
        public Floor(final AggregateMetric metric, final int digits) {
            super(metric, digits);
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(final Function<AggregateMetric, AggregateMetric> f, final Function<DocMetric, DocMetric> g, final Function<AggregateFilter, AggregateFilter> h, final Function<DocFilter, DocFilter> i, final Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Floor(m1.transform(f, g, h, i, groupByFunction), f1)).copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(final Function<AggregateMetric, AggregateMetric> f) {
            return new Floor(f.apply(m1), f1).copyPosition(this);
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(final Function<String, PerGroupConstant> namedMetricLookup, final GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Floor(m1.toExecutionMetric(namedMetricLookup, groupKeySet), f1);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class Ceil extends FactorUnop {
        public Ceil(final AggregateMetric metric, final int digits) {
            super(metric, digits);
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(final Function<AggregateMetric, AggregateMetric> f, final Function<DocMetric, DocMetric> g, final Function<AggregateFilter, AggregateFilter> h, final Function<DocFilter, DocFilter> i, final Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Ceil(m1.transform(f, g, h, i, groupByFunction), f1)).copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(final Function<AggregateMetric, AggregateMetric> f) {
            return new Ceil(f.apply(m1), f1).copyPosition(this);
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(final Function<String, PerGroupConstant> namedMetricLookup, final GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Ceil(m1.toExecutionMetric(namedMetricLookup, groupKeySet), f1);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    public static class Round extends FactorUnop {
        public Round(final AggregateMetric metric, final int digits) {
            super(metric, digits);
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(final Function<AggregateMetric, AggregateMetric> f, final Function<DocMetric, DocMetric> g, final Function<AggregateFilter, AggregateFilter> h, final Function<DocFilter, DocFilter> i, final Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Round(m1.transform(f, g, h, i, groupByFunction), f1)).copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(final Function<AggregateMetric, AggregateMetric> f) {
            return new Round(f.apply(m1), f1).copyPosition(this);
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(final Function<String, PerGroupConstant> namedMetricLookup, final GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Round(m1.toExecutionMetric(namedMetricLookup, groupKeySet), f1);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    public abstract static class FactorUnop extends AggregateMetric {
        public final AggregateMetric m1;
        public final int f1;

        public FactorUnop(AggregateMetric m1, int f1) {
            this.m1 = m1;
            this.f1 = f1;
        }

        @Override
        public boolean isOrdered() {
            return m1.isOrdered();
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            m1.validate(scope, validationHelper, errorCollector);
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "{" +
                    "m1=" + m1 +
                    "f1=" + f1 +
                    '}';
        }
    }

    @EqualsAndHashCode(callSuper = false)
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            m1.validate(scope, validationHelper, errorCollector);
            m2.validate(scope, validationHelper, errorCollector);
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName() + "{" +
                    "m1=" + m1 +
                    ", m2=" + m2 +
                    '}';
        }
    }

    public abstract static class Multiple extends AggregateMetric {
        public final List<AggregateMetric> metrics;

        protected Multiple(final List<AggregateMetric> metrics) {
            this.metrics = metrics;
        }

        @Override
        public boolean isOrdered() {
            return metrics.stream().anyMatch(AggregateMetric::isOrdered);
        }

        @Override
        public void validate(
                final Set<String> scope,
                final ValidationHelper validationHelper,
                final ErrorCollector errorCollector) {
            for (final AggregateMetric metric : metrics) {
                metric.validate(scope, validationHelper, errorCollector);
            }
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if ((o == null) || (getClass() != o.getClass())) {
                return false;
            }
            final Multiple other = (Multiple) o;
            if (metrics.size() != other.metrics.size()) {
                return false;
            }
            for (int i = 0; i < metrics.size(); i++) {
                if (!Objects.equals(metrics.get(i), other.metrics.get(i))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hash = 0;
            for (final AggregateMetric metric : metrics) {
                hash = hash * 31 + metric.hashCode();
            }
            return hash;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append(this.getClass().getSimpleName()).append('{');
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

    public static class Add extends Multiple {
        private Add(final List<AggregateMetric> metrics) {
            super(metrics);
        }

        // create filter that is equivalent to '+' of all metrics and simplify it.
        public static AggregateMetric create(final List<AggregateMetric> original) {
            // unwrapping another Add if present.
            final List<AggregateMetric> unwrapAdd = new ArrayList<>(original.size());
            for (final AggregateMetric metric : original) {
                if (metric instanceof Add) {
                    unwrapAdd.addAll(((Multiple) metric).metrics);
                } else {
                    unwrapAdd.add(metric);
                }
            }
            final List<AggregateMetric> metrics = new ArrayList<>(unwrapAdd.size());
            double constant = 0;
            // iterating throw metrics and gathering all constants into one.
            for (final AggregateMetric metric : unwrapAdd) {
                if (metric instanceof Constant) {
                    constant += ((Constant)metric).value;
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

        public static AggregateMetric create(final AggregateMetric m1, final AggregateMetric m2) {
            return create(ImmutableList.of(m1, m2));
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(
                final Function<AggregateMetric, AggregateMetric> f,
                final Function<DocMetric, DocMetric> g,
                final Function<AggregateFilter, AggregateFilter> h,
                final Function<DocFilter, DocFilter> i,
                final Function<GroupBy, GroupBy> groupByFunction) {
            final List<AggregateMetric> transformed =
                    metrics.stream()
                    .map(m -> m.transform(f, g, h, i, groupByFunction))
                    .collect(Collectors.toList());
            return f.apply(create(transformed)).copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(final Function<AggregateMetric, AggregateMetric> f) {
            final List<AggregateMetric> traversed =
                    metrics.stream()
                    .map(m -> f.apply(m).copyPosition(m))
                    .collect(Collectors.toList());
            return create(traversed).copyPosition(this);
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            final List<com.indeed.iql2.execution.metrics.aggregate.AggregateMetric> executionMetrics =
                    metrics.stream()
                    .map(m -> m.toExecutionMetric(namedMetricLookup, groupKeySet))
                    .collect(Collectors.toList());
            return com.indeed.iql2.execution.metrics.aggregate.Add.create(executionMetrics);
        }
    }

    @EqualsAndHashCode(callSuper = true)
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
            return f.apply(new Subtract(m1.transform(f, g, h, i, groupByFunction), m2.transform(f, g, h, i, groupByFunction)))
                    .copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Subtract(f.apply(m1).copyPosition(m1), f.apply(m2).copyPosition(m2))
                    .copyPosition(this);
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Subtract(
                    m1.toExecutionMetric(namedMetricLookup, groupKeySet),
                    m2.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
        }
    }

    @EqualsAndHashCode(callSuper = true)
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
            return f.apply(new Multiply(m1.transform(f, g, h, i, groupByFunction), m2.transform(f, g, h, i, groupByFunction))).copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Multiply(f.apply(m1).copyPosition(m1), f.apply(m2).copyPosition(m2))
                    .copyPosition(this);
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Multiply(
                    m1.toExecutionMetric(namedMetricLookup, groupKeySet),
                    m2.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
        }
    }

    @EqualsAndHashCode(callSuper = true)
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
            return f.apply(new Divide(m1.transform(f, g, h, i, groupByFunction), m2.transform(f, g, h, i, groupByFunction)))
                    .copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Divide(f.apply(m1).copyPosition(m1), f.apply(m2).copyPosition(m2))
                    .copyPosition(this);
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Divide(
                    m1.toExecutionMetric(namedMetricLookup, groupKeySet),
                    m2.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
        }
    }

    @EqualsAndHashCode(callSuper = true)
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
            return f.apply(new Modulus(m1.transform(f, g, h, i, groupByFunction), m2.transform(f, g, h, i, groupByFunction)))
                    .copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Modulus(f.apply(m1).copyPosition(m1), f.apply(m2).copyPosition(m2))
                    .copyPosition(this);
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Modulus(
                    m1.toExecutionMetric(namedMetricLookup, groupKeySet),
                    m2.toExecutionMetric(namedMetricLookup, groupKeySet)
            );
        }
    }

    @EqualsAndHashCode(callSuper = true)
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
            return f.apply(new Power(m1.transform(f, g, h, i, groupByFunction), m2.transform(f, g, h, i, groupByFunction)))
                    .copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Power(f.apply(m1).copyPosition(m1), f.apply(m2).copyPosition(m2))
                    .copyPosition(this);
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
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            throw new IllegalStateException("Cannot serialize " + getClass().getSimpleName() + " -- it should be removed by ExtractPrecomputed!");
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Parent extends AggregateMetric {
        public final AggregateMetric metric;

        public Parent(final AggregateMetric metric) {
            this.metric = metric;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Parent(metric.transform(f, g, h, i, groupByFunction)))
                    .copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Parent(f.apply(metric).copyPosition(metric)).copyPosition(this);
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            metric.validate(scope, validationHelper, errorCollector);
        }

        @Override
        public boolean isOrdered() {
            return metric.isOrdered();
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            throw new IllegalStateException(PRECOMPUTED_EXCEPTION);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Lag extends AggregateMetric {
        public final int lag;
        public final AggregateMetric metric;

        public Lag(final int lag, final AggregateMetric metric) {
            this.lag = lag;
            this.metric = metric;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Lag(lag, metric.transform(f, g, h, i, groupByFunction)))
                    .copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Lag(lag, f.apply(metric).copyPosition(metric)).copyPosition(this);
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            metric.validate(scope, validationHelper, errorCollector);
        }

        @Override
        public boolean isOrdered() {
            return true;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new ParentLag(lag, metric.toExecutionMetric(namedMetricLookup, groupKeySet));
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class DivideByCount extends AggregateMetric {
        public final AggregateMetric metric;

        public DivideByCount(final AggregateMetric metric) {
            this.metric = metric;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new DivideByCount(metric.transform(f, g, h, i, groupByFunction)))
                    .copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new DivideByCount(f.apply(metric).copyPosition(metric))
                    .copyPosition(this);
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
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
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class IterateLag extends AggregateMetric {
        public final int lag;
        public final AggregateMetric metric;

        public IterateLag(final int lag, final AggregateMetric metric) {
            this.lag = lag;
            this.metric = metric;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new IterateLag(lag, metric.transform(f, g, h, i, groupByFunction)))
                    .copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new IterateLag(lag, f.apply(metric).copyPosition(metric))
                    .copyPosition(this);
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            metric.validate(scope, validationHelper, errorCollector);
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
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Window extends AggregateMetric {
        public final int window;
        public final AggregateMetric metric;

        public Window(final int window, final AggregateMetric metric) {
            this.window = window;
            this.metric = metric;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Window(window, metric.transform(f, g, h, i, groupByFunction)))
                    .copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Window(window, f.apply(metric).copyPosition(metric)).copyPosition(this);
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            metric.validate(scope, validationHelper, errorCollector);
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
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Qualified extends AggregateMetric {
        public final List<String> scope;
        public final AggregateMetric metric;

        public Qualified(final List<String> scope, final AggregateMetric metric) {
            this.scope = scope;
            this.metric = metric;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Qualified(scope, metric.transform(f, g, h, i, groupByFunction)))
                    .copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Qualified(scope, f.apply(metric).copyPosition(metric))
                    .copyPosition(this);
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            final Set<String> thisScope = Sets.newHashSet(this.scope);
            if (!scope.containsAll(thisScope)) {
                errorCollector.error("Qualified scope is not a subset of outer scope! qualified scope = [" + this.scope + "], outer scope = [" + scope + "]");
            }
            metric.validate(thisScope, validationHelper, errorCollector);
            ValidationUtil.validateScope(this.scope, validationHelper, errorCollector);
        }

        @Override
        public boolean isOrdered() {
            return metric.isOrdered();
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            throw new IllegalStateException(PRECOMPUTED_EXCEPTION);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class DocStatsPushes extends AggregateMetric {
        public final String dataset;
        public final DocMetric pushes;

        public DocStatsPushes(final String dataset, final DocMetric pushes) {
            this.dataset = dataset;
            this.pushes = pushes;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(this).copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            pushes.validate(dataset, validationHelper, errorCollector);
            ValidationUtil.validateDataset(dataset, validationHelper, errorCollector);
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new DocumentLevelMetric(dataset, pushes.getPushes(dataset));
        }
    }

    /**
     * DocStats in which there is no explicit sum, but a single atomic, unambiguous atom.
     */
    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class DocStats extends AggregateMetric {
        public final DocMetric docMetric;

        public DocStats(final DocMetric docMetric) {
            this.docMetric = docMetric;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new DocStats(docMetric.transform(g, i))).copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            for (final String dataset : scope) {
                docMetric.validate(dataset, validationHelper, errorCollector);
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
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Constant extends AggregateMetric {
        public final double value;

        public Constant(final double value) {
            this.value = value;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Constant(value)).copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {

        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return new com.indeed.iql2.execution.metrics.aggregate.Constant(value);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Percentile extends RequiresFTGSMetric {
        public final FieldSet field;
        public final double percentile;

        public Percentile(final FieldSet field, final double percentile) {
            this.field = field;
            this.percentile = percentile;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Percentile(field, percentile)).copyPosition(this);
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
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Running extends AggregateMetric {
        public final int offset;
        public final AggregateMetric metric;

        public Running(final int offset, final AggregateMetric metric) {
            this.offset = offset;
            this.metric = metric;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Running(offset, metric.transform(f, g, h, i, groupByFunction)))
                    .copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Running(offset, f.apply(metric).copyPosition(metric)).copyPosition(this);
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            metric.validate(scope, validationHelper, errorCollector);
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
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Distinct extends RequiresFTGSMetric {
        public final FieldSet field;
        public final Optional<AggregateFilter> filter;
        public final Optional<Integer> windowSize;

        public Distinct(final FieldSet field, final Optional<AggregateFilter> filter, final Optional<Integer> windowSize) {
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
                return f.apply(new Distinct(field, Optional.of(filter.get().transform(f, g, h, i, groupByFunction)), windowSize))
                        .copyPosition(this);
            } else {
                return f.apply(new Distinct(field, Optional.absent(), windowSize)).copyPosition(this);
            }
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            if (filter.isPresent()) {
                return new Distinct(field, Optional.of(filter.get().traverse1(f)), windowSize)
                        .copyPosition(this);
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
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Named extends AggregateMetric {
        public final AggregateMetric metric;
        public final Positioned<String> name;

        public Named(final AggregateMetric metric, final Positioned<String> name) {
            this.metric = metric;
            this.name = name;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new Named(metric.transform(f, g, h, i, groupByFunction), name))
                    .copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new Named(f.apply(metric).copyPosition(metric), name)
                    .copyPosition(this);
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            metric.validate(scope, validationHelper, errorCollector);
        }

        @Override
        public boolean isOrdered() {
            return metric.isOrdered();
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            throw new IllegalStateException(PRECOMPUTED_EXCEPTION);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class NeedsSubstitution extends AggregateMetric {
        public final String substitutionName;

        public NeedsSubstitution(final String substitutionName) {
            this.substitutionName = substitutionName;
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(final Function<AggregateMetric, AggregateMetric> f, final Function<DocMetric, DocMetric> g, final Function<AggregateFilter, AggregateFilter> h, final Function<DocFilter, DocFilter> i, final Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(this).copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(final Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public void validate(final Set<String> scope, final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
        }

        @Override
        public boolean isOrdered() {
            throw new UnsupportedOperationException("Should be replaced by something else");
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(final Function<String, PerGroupConstant> namedMetricLookup, final GroupKeySet groupKeySet) {
            throw new UnsupportedOperationException("Should be replaced by something else");
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class GroupStatsLookup extends AggregateMetric {
        public final String name;

        public GroupStatsLookup(final String name) {
            this.name = name;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(this).copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            if (!validationHelper.isComputed(name)) {
                errorCollector.error("Cannot use value that has not been computed yet. Likely cause: DISTINCT, PERCENTILE, FIELD_MIN, FIELD_MAX, or SUM_OVER used in a Top K BY clause.");
            }
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            return namedMetricLookup.apply(name);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class SumAcross extends RequiresFTGSMetric {
        public final GroupBy groupBy;
        public final AggregateMetric metric;

        public SumAcross(final GroupBy groupBy, final AggregateMetric metric) {
            this.groupBy = groupBy;
            this.metric = metric;
        }

        @Override
        public <T, E extends Throwable> T visit(Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
            return f.apply(new SumAcross(groupBy.transform(groupByFunction, f, g, h, i), metric.transform(f, g, h, i, groupByFunction)))
                    .copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new SumAcross(groupBy.traverse1(f), f.apply(metric).copyPosition(metric))
                    .copyPosition(this);
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
            throw new IllegalStateException(PRECOMPUTED_EXCEPTION);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class IfThenElse extends AggregateMetric {
        public final AggregateFilter condition;
        public final AggregateMetric trueCase;
        public final AggregateMetric falseCase;

        public IfThenElse(final AggregateFilter condition, final AggregateMetric trueCase, final AggregateMetric falseCase) {
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
            ).copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new IfThenElse(
                    condition.traverse1(f),
                    f.apply(trueCase).copyPosition(trueCase),
                    f.apply(falseCase).copyPosition(falseCase)
            ).copyPosition(this);
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            condition.validate(scope, validationHelper, errorCollector);
            trueCase.validate(scope, validationHelper, errorCollector);
            falseCase.validate(scope, validationHelper, errorCollector);
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
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class FieldMin extends RequiresFTGSMetric {
        public final FieldSet field;
        public final Optional<AggregateMetric> metric;
        public final Optional<AggregateFilter> filter;

        public FieldMin(final FieldSet field, final Optional<AggregateMetric> metric, final Optional<AggregateFilter> filter) {
            this.field = field;
            this.metric = metric;
            this.filter = filter;
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(
            final Function<AggregateMetric, AggregateMetric> f,
            final Function<DocMetric, DocMetric> g,
            final Function<AggregateFilter, AggregateFilter> h,
            final Function<DocFilter, DocFilter> i,
            final Function<GroupBy, GroupBy> groupByFunction
        ) {
            return f.apply(
                new FieldMin(
                    field,
                    metric.transform(m -> m.transform(f, g, h, i, groupByFunction)),
                    filter.transform(fil -> fil.transform(f, g, h, i, groupByFunction))
                )
            ).copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(final Function<AggregateMetric, AggregateMetric> f) {
            return new FieldMin(
                field,
                metric.transform(f),
                filter.isPresent() ? Optional.of(filter.get().traverse1(f)) : filter
            );
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(
            final Function<String, PerGroupConstant> namedMetricLookup,
            final GroupKeySet groupKeySet
        ) {
            throw new IllegalStateException(PRECOMPUTED_EXCEPTION);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class FieldMax extends RequiresFTGSMetric {
        public final FieldSet field;
        public final Optional<AggregateMetric> metric;
        public final Optional<AggregateFilter> filter;

        public FieldMax(final FieldSet field, final Optional<AggregateMetric> metric, final Optional<AggregateFilter> filter) {
            this.field = field;
            this.metric = metric;
            this.filter = filter;
        }

        @Override
        public <T, E extends Throwable> T visit(final Visitor<T, E> visitor) throws E {
            return visitor.visit(this);
        }

        @Override
        public AggregateMetric transform(
            final Function<AggregateMetric, AggregateMetric> f,
            final Function<DocMetric, DocMetric> g,
            final Function<AggregateFilter, AggregateFilter> h,
            final Function<DocFilter, DocFilter> i,
            final Function<GroupBy, GroupBy> groupByFunction
        ) {
            return f.apply(
                new FieldMax(
                    field,
                    metric.transform(m -> m.transform(f, g, h, i, groupByFunction)),
                    filter.transform(fil -> fil.transform(f, g, h, i, groupByFunction))
                )
            ).copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(final Function<AggregateMetric, AggregateMetric> f) {
            return new FieldMax(
                field,
                metric.transform(f),
                filter.isPresent() ? Optional.of(filter.get().traverse1(f)) : filter
            );
        }

        @Override
        public boolean isOrdered() {
            return false;
        }

        @Override
        public com.indeed.iql2.execution.metrics.aggregate.AggregateMetric toExecutionMetric(
            final Function<String, PerGroupConstant> namedMetricLookup,
            final GroupKeySet groupKeySet
        ) {
            throw new IllegalStateException(PRECOMPUTED_EXCEPTION);
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Min extends AggregateMetric {
        public final List<AggregateMetric> metrics;

        public Min(final List<AggregateMetric> metrics) {
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
            return f.apply(new Min(newMetrics)).copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            final List<AggregateMetric> newMetrics = Lists.newArrayListWithCapacity(metrics.size());
            for (final AggregateMetric metric : metrics) {
                newMetrics.add(f.apply(metric).copyPosition(metric));
            }
            return new Min(newMetrics).copyPosition(this);
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            for (final AggregateMetric metric : metrics) {
                metric.validate(scope, validationHelper, errorCollector);
            }

            if (metrics.size() < 2) {
                errorCollector.error("MIN requires at least 2 arguments. Did you mean FIELD_MIN()?");
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
    }

    @EqualsAndHashCode(callSuper = false)
    @ToString
    public static class Max extends AggregateMetric {
        public final List<AggregateMetric> metrics;

        public Max(final List<AggregateMetric> metrics) {
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
            return f.apply(new Max(newMetrics)).copyPosition(this);
        }

        @Override
        public AggregateMetric traverse1(Function<AggregateMetric, AggregateMetric> f) {
            final List<AggregateMetric> newMetrics = Lists.newArrayListWithCapacity(metrics.size());
            for (final AggregateMetric metric : metrics) {
                newMetrics.add(f.apply(metric).copyPosition(metric));
            }
            return new Max(newMetrics).copyPosition(this);
        }

        @Override
        public void validate(Set<String> scope, ValidationHelper validationHelper, ErrorCollector errorCollector) {
            for (final AggregateMetric metric : metrics) {
                metric.validate(scope, validationHelper, errorCollector);
            }

            if (metrics.size() < 2) {
                errorCollector.error("MAX requires at least 2 arguments. Did you mean FIELD_MAX()?");
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
    }
}
