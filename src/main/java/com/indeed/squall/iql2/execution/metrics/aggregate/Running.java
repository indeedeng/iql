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

import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;

import java.util.Map;
import java.util.Set;

public class Running implements AggregateMetric {
    private final AggregateMetric inner;
    private final int offset;
    private final Map<Integer, Double> groupSums = new Int2DoubleOpenHashMap();
    private int[] groupToRealGroup;

    public Running(final AggregateMetric inner, final int offset) {
        this.inner = inner;
        this.offset = offset;
    }

    @Override
    public Set<QualifiedPush> requires() {
        return inner.requires();
    }

    @Override
    public void register(final Map<QualifiedPush, Integer> metricIndexes, final GroupKeySet groupKeySet) {
        inner.register(metricIndexes, groupKeySet);
        groupToRealGroup = new int[groupKeySet.numGroups() + 1];
        for (int group = 1; group <= groupKeySet.numGroups(); group++) {
            GroupKeySet keyset = groupKeySet;
            int index = group;
            for (int i = 0; i < offset; i++) {
                index = keyset.parentGroup(index);
                keyset = keyset.previous();
            }
            groupToRealGroup[group] = index;
        }
    }

    @Override
    public double[] getGroupStats(final long[][] stats, final int numGroups) {
        final double[] innerResult = inner.getGroupStats(stats, numGroups);
        double sum = 0;
        int currentParent = -1;
        final double[] result = new double[numGroups + 1];
        for (int i = 1; i <= numGroups; i++) {
            final int parent = groupToRealGroup[i];
            if (parent != currentParent) {
                sum = 0;
                currentParent = parent;
            }
            sum += innerResult[i];
            result[i] = sum;
        }
        return result;
    }

    @Override
    public double apply(final String term, final long[] stats, final int group) {
        final double val = inner.apply(term, stats, group);
        return getResult(groupToRealGroup[group], val);
    }

    @Override
    public double apply(final long term, final long[] stats, final int group) {
        final double val = inner.apply(term, stats, group);
        return getResult(groupToRealGroup[group], val);
    }

    @Override
    public boolean needGroup() {
        return true;
    }

    @Override
    public boolean needStats() {
        return inner.needStats();
    }

    private double getResult(final int group, final double val) {
        final Double v = groupSums.get(group);
        final double result;
        if (v != null) {
            result = v + val;
        } else {
            result = val;
        }
        groupSums.put(group, result);
        return result;
    }
}
