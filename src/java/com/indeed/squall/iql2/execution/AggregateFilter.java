package com.indeed.squall.iql2.execution;

import com.google.common.collect.Sets;
import com.indeed.imhotep.automaton.Automaton;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.squall.iql2.language.util.ValidationUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author jwolfe
 */
public interface AggregateFilter extends Pushable {
    boolean[] getGroupStats(long[][] stats, int numGroups);

    boolean allow(String term, long[] stats, int group);
    boolean allow(long term, long[] stats, int group);
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
        public boolean needGroup() {
            return operand.needGroup();
        }

        @Override
        public boolean needStats() {
            return operand.needStats();
        }
    }

    // Base class for binary opetations on AggregateFilter
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
        public boolean needGroup() {
            return left.needGroup() || right.needGroup();
        }

        @Override
        public boolean needStats() {
            return left.needStats() || right.needStats();
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
            return Sets.union(left.requires(), left.requires());
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

        public TermEqualsRegex(final Term value) {
            automaton = ValidationUtil.compileRegex(value.isIntTerm ? String.valueOf(value.intTerm) : value.stringTerm);
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

        public TermEquals(final Term value) {
            this.value = value;
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
            return term.equals(value.stringTerm);
        }

        @Override
        public boolean allow(final long term, final long[] stats, final int group) {
            return term == value.intTerm;
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
    }

    class MetricEquals extends BinaryMetric {
        public MetricEquals(final AggregateMetric m1, final AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public boolean eval(final double left, final double right) {
            return left == right;
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
    }

    class GreaterThan extends BinaryMetric {
        public GreaterThan(final AggregateMetric m1, final AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public boolean eval(final double left, final double right) {
            return left > right;
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
    }

    class LessThan extends BinaryMetric {
        public LessThan(final AggregateMetric m1, final AggregateMetric m2) {
            super(m1, m2);
        }

        @Override
        public boolean eval(final double left, final double right) {
            return left < right;
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
    }

    class And extends Binary {
        public And(final AggregateFilter f1, final AggregateFilter f2) {
            super(f1, f2);
        }

        @Override
        boolean eval(final boolean left, final boolean right) {
            return left && right;
        }
    }

    class Or extends Binary {
        public Or(final AggregateFilter f1, final AggregateFilter f2) {
            super(f1, f2);
        }

        @Override
        boolean eval(final boolean left, final boolean right) {
            return left || right;
        }
    }

    class RegexFilter implements AggregateFilter {
        private final Automaton automaton;

        public RegexFilter(final String regex) {
            automaton = ValidationUtil.compileRegex(regex);
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
