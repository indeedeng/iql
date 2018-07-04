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

package com.indeed.squall.iql2.execution.metrics.aggregate;

import com.google.common.collect.Sets;
import com.indeed.squall.iql2.execution.Pushable;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;

import java.util.Map;
import java.util.Set;

/**
 * @author jwolfe
 */
public interface AggregateMetric extends Pushable {
    double[] getGroupStats(long[][] stats, int numGroups);

    double apply(String term, long[] stats, int group);
    double apply(long term, long[] stats, int group);
    // return true if terms are expected in sorted order
    boolean needSorted();
    // if needGroup() returns false then group value is ignored inside apply(...) methods
    // and it's valid to pass any value as group
    boolean needGroup();
    // if needStats() returns false then stats value is ignored inside apply(...) methods
    // and it's valid to pass any array or null as stats
    boolean needStats();

    // Base class for unary function
    abstract class Unary implements AggregateMetric {
        private final AggregateMetric operand;

        public Unary(final AggregateMetric operand) {
            this.operand = operand;
        }

        abstract double eval(final double value);

        @Override
        public Set<QualifiedPush> requires() {
            return operand.requires();
        }

        @Override
        public void register(final Map<QualifiedPush, Integer> metricIndexes,
                             final GroupKeySet groupKeySet) {
            operand.register(metricIndexes, groupKeySet);
        }

        @Override
        public double[] getGroupStats(final long[][] stats, final int numGroups) {
            final double[] result = operand.getGroupStats(stats, numGroups);
            for (int i = 0; i < result.length; i++) {
                result[i] = eval(result[i]);
            }
            return result;
        }

        @Override
        public double apply(final String term, final long[] stats, final int group) {
            return eval(operand.apply(term, stats, group));
        }

        @Override
        public double apply(final long term, final long[] stats, final int group) {
            return eval(operand.apply(term, stats, group));
        }

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

    // Base class for binary function
    abstract class Binary implements AggregateMetric {
        private final AggregateMetric left;
        private final AggregateMetric right;

        public Binary(final AggregateMetric left, final AggregateMetric right) {
            this.left = left;
            this.right = right;
        }

        abstract double eval(final double left, final double right);

        @Override
        public Set<QualifiedPush> requires() {
            return Sets.union(left.requires(), right.requires());
        }

        @Override
        public void register(final Map<QualifiedPush, Integer> metricIndexes,
                             final GroupKeySet groupKeySet) {
            left.register(metricIndexes, groupKeySet);
            right.register(metricIndexes, groupKeySet);
        }

        @Override
        public double[] getGroupStats(final long[][] stats, final int numGroups) {
            final double[] lhs = left.getGroupStats(stats, numGroups);
            final double[] rhs = right.getGroupStats(stats, numGroups);
            for (int i = 0; i < rhs.length; i++) {
                lhs[i] = eval(lhs[i], rhs[i]);
            }
            return lhs;
        }

        @Override
        public double apply(final String term, final long[] stats, final int group) {
            return eval(left.apply(term, stats, group), right.apply(term, stats, group));
        }

        @Override
        public double apply(final long term, final long[] stats, final int group) {
            return eval(left.apply(term, stats, group), right.apply(term, stats, group));
        }

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

}
