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

package com.indeed.iql2.execution;

import com.google.common.collect.Sets;
import com.indeed.imhotep.automaton.Automaton;
import com.indeed.imhotep.metrics.aggregate.AggregateStatTree;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.iql2.language.util.ValidationUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author jwolfe
 */
public interface AggregateFilter extends Pushable {
    boolean[] getGroupStats(long[][] stats, int numGroups);

    boolean allow(String term, long[] stats, int group);
    boolean allow(long term, long[] stats, int group);

    AggregateStatTree toImhotep(final Map<QualifiedPush, AggregateStatTree> atomicStats);

    // return true if terms are expected in sorted order
    boolean needSorted();
    // if needGroup() returns false then group value is ignored inside allow(...) methods
    // and it's valid to pass any value as group
    boolean needGroup();
    // if needStats() returns false then stats value is ignored inside allow(...) methods
    // and it's valid to pass any array or null as stats
    boolean needStats();

    // Base class for unary operation on AggregateFilter
    abstract class Unary implements AggregateFilter {
        protected final AggregateFilter operand;
        Unary(final AggregateFilter operand) {
            this.operand = operand;
        }

        @Override
        public Set<QualifiedPush> requires() {
            return operand.requires();
        }

        @Override
        public void register(final Map<QualifiedPush, Integer> metricIndexes, final GroupKeySet groupKeySet) {
            operand.register(metricIndexes, groupKeySet);
        }

        @Override
        public final AggregateStatTree toImhotep(final Map<QualifiedPush, AggregateStatTree> atomicStats) {
            return toImhotep(operand.toImhotep(atomicStats));
        }

        abstract AggregateStatTree toImhotep(final AggregateStatTree operand);

        @Override
        public boolean needSorted() {
            return operand.needSorted();
        }

        @Override
        public boolean needGroup() {
            return operand.needGroup();
        }

        @Override
        public boolean needStats() {
            return operand.needStats();
        }
    }

    // Base class for binary operations on AggregateFilter
    abstract class Binary implements AggregateFilter {
        private final AggregateFilter left;
        private final AggregateFilter right;
        public Binary(final AggregateFilter left, final AggregateFilter right) {
            this.left = left;
            this.right = right;
        }

        abstract boolean eval(final boolean left, final boolean right);

        @Override
        public Set<QualifiedPush> requires() {
            return Sets.union(left.requires(), right.requires());
        }

        @Override
        public void register(final Map<QualifiedPush, Integer> metricIndexes, final GroupKeySet groupKeySet) {
            left.register(metricIndexes, groupKeySet);
            right.register(metricIndexes, groupKeySet);
        }

        @Override
        public boolean[] getGroupStats(final long[][] stats, final int numGroups) {
            final boolean[] l = left.getGroupStats(stats, numGroups);
            final boolean[] r = right.getGroupStats(stats, numGroups);
            final boolean[] result = new boolean[numGroups + 1];
            for (int i = 1; i <= numGroups; i++) {
                result[i] = eval(l[i], r[i]);
            }
            return result;
        }

        @Override
        public boolean allow(final String term, final long[] stats, final int group) {
            return eval(left.allow(term, stats, group), right.allow(term, stats, group));
        }

        @Override
        public boolean allow(final long term, final long[] stats, final int group) {
            return eval(left.allow(term, stats, group), right.allow(term, stats, group));
        }

        @Override
        public final AggregateStatTree toImhotep(final Map<QualifiedPush, AggregateStatTree> atomicStats) {
            return toImhotep(left.toImhotep(atomicStats), right.toImhotep(atomicStats));
        }

        abstract AggregateStatTree toImhotep(final AggregateStatTree lhs, final AggregateStatTree rhs);

        @Override
        public boolean needSorted() {
            return left.needSorted() || right.needSorted();
        }

        @Override
        public boolean needGroup() {
            return left.needGroup() || right.needGroup();
        }

        @Override
        public boolean needStats() {
            return left.needStats() || right.needStats();
        }
    }

    // Base class for operations on multiple AggregateFilters
    abstract class Multiary implements AggregateFilter {
        protected final List<AggregateFilter> filters;

        protected Multiary(final List<AggregateFilter> filters) {
            if (filters.size() < 2) {
                throw new IllegalArgumentException("2 or more filters expected");
            }
            this.filters = filters;
        }

        abstract boolean eval(final boolean left, final boolean right);

        @Override
        public Set<QualifiedPush> requires() {
            return filters.stream().map(Pushable::requires).reduce(Sets::union).get();
        }

        @Override
        public void register(final Map<QualifiedPush, Integer> metricIndexes, final GroupKeySet groupKeySet) {
            filters.forEach(f -> f.register(metricIndexes, groupKeySet));
        }

        @Override
        public boolean[] getGroupStats(final long[][] stats, final int numGroups) {
            return filters
                    .stream()
                    .map(f -> f.getGroupStats(stats, numGroups))
                    .reduce((left, right) -> {
                        for (int i = 0; i < left.length; i++) {
                            left[i] = eval(left[i], right[i]);
                        }
                        return left;
                    }).get();
        }

        @Override
        public boolean allow(final String term, final long[] stats, final int group) {
            return filters
                    .stream()
                    .map(f -> f.allow(term, stats, group))
                    .reduce(this::eval)
                    .get();
        }

        @Override
        public boolean allow(final long term, final long[] stats, final int group) {
            return filters
                    .stream()
                    .map(f -> f.allow(term, stats, group))
                    .reduce(this::eval)
                    .get();
        }

        abstract AggregateStatTree toImhotep(final AggregateStatTree lhs, final AggregateStatTree rhs);

        @Override
        public AggregateStatTree toImhotep(final Map<QualifiedPush, AggregateStatTree> atomicStats) {
            return filters
                    .stream()
                    .map(f -> f.toImhotep(atomicStats))
                    .reduce(this::toImhotep)
                    .get();
        }

        @Override
        public boolean needSorted() {
            return filters.stream().anyMatch(AggregateFilter::needSorted);
        }

        @Override
        public boolean needGroup() {
            return filters.stream().anyMatch(AggregateFilter::needGroup);
        }

        @Override
        public boolean needStats() {
            return filters.stream().anyMatch(AggregateFilter::needStats);
        }
    }

    // Base class for AggregateFilter that is function over two AggregateMetric
    abstract class BinaryMetric implements AggregateFilter {
        private final AggregateMetric left;
        private final AggregateMetric right;
        public BinaryMetric(final AggregateMetric left, final AggregateMetric right) {
            this.left = left;
            this.right = right;
        }

        abstract boolean eval(final double left, final double right);

        @Override
        public Set<QualifiedPush> requires() {
            return Sets.union(left.requires(), right.requires());
        }

        @Override
        public void register(final Map<QualifiedPush, Integer> metricIndexes, final GroupKeySet groupKeySet) {
            left.register(metricIndexes, groupKeySet);
            right.register(metricIndexes, groupKeySet);
        }

        @Override
        public boolean[] getGroupStats(final long[][] stats, final int numGroups) {
            final double[] l = left.getGroupStats(stats, numGroups);
            final double[] r = right.getGroupStats(stats, numGroups);
            final boolean[] result = new boolean[numGroups + 1];
            for (int i = 1; i <= numGroups; i++) {
                result[i] = eval(l[i], r[i]);
            }
            return result;
        }

        @Override
        public boolean allow(final String term, final long[] stats, final int group) {
            return eval(left.apply(term, stats, group), right.apply(term, stats, group));
        }

        @Override
        public boolean allow(final long term, final long[] stats, final int group) {
            return eval(left.apply(term, stats, group), right.apply(term, stats, group));
        }

        @Override
        public final AggregateStatTree toImhotep(final Map<QualifiedPush, AggregateStatTree> atomicStats) {
            return toImhotep(left.toImhotep(atomicStats), right.toImhotep(atomicStats));
        }

        abstract AggregateStatTree toImhotep(final AggregateStatTree lhs, final AggregateStatTree rhs);

        @Override
        public boolean needSorted() {
            return left.needSorted() || right.needSorted();
        }

        @Override
        public boolean needGroup() {
            return left.needGroup() || right.needGroup();
        }

        @Override
        public boolean needStats() {
            return left.needStats() || right.needStats();
        }
    }

    class TermEqualsRegex implements AggregateFilter {
        private final Automaton automaton;
        private final String rawRegex;

        public TermEqualsRegex(final Term value) {
            rawRegex = value.asString();
            automaton = ValidationUtil.compileRegex(rawRegex);
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Collections.emptySet();
        }

        @Override
        public void register(final Map<QualifiedPush, Integer> metricIndexes,
                             final GroupKeySet groupKeySet) {
        }

        @Override
        public boolean[] getGroupStats(final long[][] stats, final int numGroups) {
            throw new IllegalArgumentException("Cannot use TermEqualsRegex in a getGroupStats");
        }

        @Override
        public boolean allow(final String term, final long[] stats, final int group) {
            return automaton.run(term);
        }

        @Override
        public boolean allow(final long term, final long[] stats, final int group) {
            return automaton.run(String.valueOf(term));
        }

        @Override
        public AggregateStatTree toImhotep(final Map<QualifiedPush, AggregateStatTree> atomicStats) {
            return AggregateStatTree.termRegex(rawRegex);
        }

        @Override
        public boolean needSorted() {
            return false;
        }

        @Override
        public boolean needGroup() {
            return false;
        }

        @Override
        public boolean needStats() {
            return false;
        }
    }

    class TermEquals implements AggregateFilter {
        private final Term value;
        private final String stringValue;

        public TermEquals(final Term value) {
            this.value = value;
            stringValue = value.asString();
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Collections.emptySet();
        }

        @Override
        public void register(final Map<QualifiedPush, Integer> metricIndexes,
                             final GroupKeySet groupKeySet) {
        }

        @Override
        public boolean[] getGroupStats(final long[][] stats, final int numGroups) {
            throw new IllegalArgumentException("Cannot use TermEquals in a getGroupStats");
        }

        @Override
        public boolean allow(final String term, final long[] stats, final int group) {
            return term.equals(stringValue);
        }

        @Override
        public boolean allow(final long term, final long[] stats, final int group) {
            return value.isIntTerm && (term == value.intTerm);
        }

        @Override
        public AggregateStatTree toImhotep(final Map<QualifiedPush, AggregateStatTree> atomicStats) {
            if (value.isSafeAsInt()) {
                return AggregateStatTree.termEquals(value.intTerm);
            } else {
                return AggregateStatTree.termEquals(value.stringTerm);
            }
        }

        @Override
        public boolean needSorted() {
            return false;
        }

        @Override
        public boolean needGroup() {
            return false;
        }

        @Override
        public boolean needStats() {
            return false;
        }
    }

    class Not extends AggregateFilter.Unary {
        public Not(final AggregateFilter f) {
            super(f);
        }

        @Override
        public boolean[] getGroupStats(final long[][] stats, final int numGroups) {
            final boolean[] inner = operand.getGroupStats(stats, numGroups);
            final boolean[] copy = Arrays.copyOf(inner, inner.length);
            for (int i = 0; i < copy.length; i++) {
                copy[i] = !copy[i];
            }
            return copy;
        }

        @Override
        public boolean allow(final String term, final long[] stats, final int group) {
            return !operand.allow(term, stats, group);
        }

        @Override
        public boolean allow(final long term, final long[] stats, final int group) {
            return !operand.allow(term, stats, group);
        }

        @Override
        AggregateStatTree toImhotep(final AggregateStatTree operand) {
            return operand.not();
        }
    }

    class MetricEquals extends BinaryMetric {
        public MetricEquals(final AggregateMetric m1, final AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public boolean eval(final double left, final double right) {
            return left == right;
        }

        @Override
        AggregateStatTree toImhotep(final AggregateStatTree lhs, final AggregateStatTree rhs) {
            return lhs.eq(rhs);
        }
    }

    class MetricNotEquals extends BinaryMetric {
        public MetricNotEquals(final AggregateMetric m1, final AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public boolean eval(final double left, final double right) {
            return left != right;
        }

        @Override
        AggregateStatTree toImhotep(final AggregateStatTree lhs, final AggregateStatTree rhs) {
            return lhs.neq(rhs);
        }
    }

    class GreaterThan extends BinaryMetric {
        public GreaterThan(final AggregateMetric m1, final AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public boolean eval(final double left, final double right) {
            return left > right;
        }

        @Override
        AggregateStatTree toImhotep(final AggregateStatTree lhs, final AggregateStatTree rhs) {
            return lhs.gt(rhs);
        }
    }

    class GreaterThanOrEquals extends BinaryMetric {
        public GreaterThanOrEquals(final AggregateMetric m1, final AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public boolean eval(final double left, final double right) {
            return left >= right;
        }

        @Override
        AggregateStatTree toImhotep(final AggregateStatTree lhs, final AggregateStatTree rhs) {
            return lhs.gte(rhs);
        }
    }

    class LessThan extends BinaryMetric {
        public LessThan(final AggregateMetric m1, final AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public boolean eval(final double left, final double right) {
            return left < right;
        }

        @Override
        AggregateStatTree toImhotep(final AggregateStatTree lhs, final AggregateStatTree rhs) {
            return lhs.lt(rhs);
        }
    }

    class LessThanOrEquals extends BinaryMetric {
        public LessThanOrEquals(final AggregateMetric m1, final AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public boolean eval(final double left, final double right) {
            return left <= right;
        }

        @Override
        AggregateStatTree toImhotep(final AggregateStatTree lhs, final AggregateStatTree rhs) {
            return lhs.lte(rhs);
        }
    }

    class And extends Multiary {
        private And(final List<AggregateFilter> filters) {
            super(filters);
        }

        public static AggregateFilter create(final List<AggregateFilter> filters) {
            return new And(filters);
        }

        @Override
        boolean eval(final boolean left, final boolean right) {
            return left && right;
        }

        @Override
        AggregateStatTree toImhotep(final AggregateStatTree lhs, final AggregateStatTree rhs) {
            return lhs.and(rhs);
        }
    }

    class Or extends Multiary {
        private Or(final List<AggregateFilter> filters) {
            super(filters);
        }

        public static AggregateFilter create(final List<AggregateFilter> filters) {
            return new Or(filters);
        }

        @Override
        boolean eval(final boolean left, final boolean right) {
            return left || right;
        }

        @Override
        AggregateStatTree toImhotep(final AggregateStatTree lhs, final AggregateStatTree rhs) {
            return lhs.or(rhs);
        }
    }

    class RegexFilter implements AggregateFilter {
        private final String regex;
        private final Automaton automaton;

        public RegexFilter(final String regex) {
            this.regex = regex;
            automaton = ValidationUtil.compileRegex(this.regex);
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Collections.emptySet();
        }

        @Override
        public void register(final Map<QualifiedPush, Integer> metricIndexes,
                             final GroupKeySet groupKeySet) {
        }

        @Override
        public boolean[] getGroupStats(final long[][] stats, final int numGroups) {
            throw new UnsupportedOperationException("Cannot use getGroupStats on a RegexFilter");
        }

        @Override
        public boolean allow(final String term, final long[] stats, final int group) {
            return automaton.run(term);
        }

        @Override
        public boolean allow(final long term, final long[] stats, final int group) {
            return automaton.run(String.valueOf(term));
        }

        @Override
        public AggregateStatTree toImhotep(final Map<QualifiedPush, AggregateStatTree> atomicStats) {
            return AggregateStatTree.termRegex(regex);
        }

        @Override
        public boolean needSorted() {
            return false;
        }

        @Override
        public boolean needGroup() {
            return false;
        }

        @Override
        public boolean needStats() {
            return false;
        }
    }

    class Constant implements AggregateFilter {
        private final boolean value;

        public Constant(final boolean value) {
            this.value = value;
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Collections.emptySet();
        }

        @Override
        public void register(final Map<QualifiedPush, Integer> metricIndexes, final GroupKeySet groupKeySet) {
        }

        @Override
        public boolean[] getGroupStats(final long[][] stats, final int numGroups) {
            final boolean[] result = new boolean[numGroups + 1];
            Arrays.fill(result, value);
            return result;
        }

        @Override
        public boolean allow(final String term, final long[] stats, final int group) {
            return value;
        }

        @Override
        public boolean allow(final long term, final long[] stats, final int group) {
            return value;
        }

        @Override
        public AggregateStatTree toImhotep(final Map<QualifiedPush, AggregateStatTree> atomicStats) {
            return AggregateStatTree.constant(value ? 1.0 : 0.0);
        }

        @Override
        public boolean needSorted() {
            return false;
        }

        @Override
        public boolean needGroup() {
            return false;
        }

        @Override
        public boolean needStats() {
            return false;
        }
    }

    class IsDefaultGroup implements AggregateFilter {
        private final GroupKeySet keySet;

        public IsDefaultGroup(final GroupKeySet keySet) {
            this.keySet = keySet;
        }

        @Override
        public boolean[] getGroupStats(final long[][] stats, final int numGroups) {
            final boolean[] result = new boolean[numGroups + 1];
            for (int i = 0; i <= numGroups; i++) {
                result[i] = keySet.isPresent(i) && keySet.groupKey(i).isDefault();
            }
            return result;
        }

        @Override
        public boolean allow(final String term, final long[] stats, final int group) {
            return keySet.groupKey(group).isDefault();
        }

        @Override
        public boolean allow(final long term, final long[] stats, final int group) {
            return keySet.groupKey(group).isDefault();
        }

        @Override
        public AggregateStatTree toImhotep(final Map<QualifiedPush, AggregateStatTree> atomicStats) {
            final double[] values = new double[keySet.numGroups()];
            for (int i = 1; i <= keySet.numGroups(); i++) {
                values[i] = keySet.groupKey(i).isDefault() ? 1.0 : 0.0;
            }
            return AggregateStatTree.perGroupConstant(values);
        }

        @Override
        public boolean needSorted() {
            return false;
        }

        @Override
        public boolean needGroup() {
            return true;
        }

        @Override
        public boolean needStats() {
            return false;
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Collections.emptySet();
        }

        @Override
        public void register(final Map<QualifiedPush, Integer> metricIndexes,
                             final GroupKeySet groupKeySet) {
        }
    }
}
