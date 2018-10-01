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

package com.indeed.iql2.execution.metrics.aggregate;

import com.indeed.imhotep.metrics.aggregate.AggregateStatTree;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class Window implements AggregateMetric {
    private final int size;
    private final AggregateMetric inner;

    private boolean iterationStarted = false;
    private long currentIntTerm = 0;
    private String currentStringTerm = null;
    private int lastGroup = 0;

    private double[] groupToWindowSum;
    private GroupKeySet groupKeySet;

    public Window(final int size, final AggregateMetric inner) {
        this.size = size;
        this.inner = inner;
    }

    @Override
    public Set<QualifiedPush> requires() {
        return inner.requires();
    }

    @Override
    public void register(final Map<QualifiedPush, Integer> metricIndexes, final GroupKeySet groupKeySet) {
        inner.register(metricIndexes, groupKeySet);
        groupToWindowSum = new double[groupKeySet.numGroups() + 1];
        this.groupKeySet = groupKeySet;
    }

    @Override
    public double[] getGroupStats(final long[][] stats, final int numGroups) {
        final double[] innerResult = inner.getGroupStats(stats, numGroups);
        final double[] result = new double[numGroups + 1];
        double sum = 0;
        int currentParent = -1;
        int count = 0;
        for (int i = 1; i <= numGroups; i++) {
            final int parent = groupKeySet.parentGroup(i);
            if (parent != currentParent) {
                currentParent = parent;
                sum = 0;
                count = 0;
            }
            sum += innerResult[i];
            count += 1;
            if (count > size) {
                sum -= innerResult[i - size];
            }
            result[i] = sum;
        }
        return result;
    }

    @Override
    public double apply(final String term, final long[] stats, final int group) {
        if (iterationStarted && !term.equals(currentStringTerm)) {
            clear();
        }
        currentStringTerm = term;
        final double value = inner.apply(term, stats, group);
        return handle(group, value);
    }

    @Override
    public double apply(final long term, final long[] stats, final int group) {
        if (iterationStarted && term != currentIntTerm) {
            clear();
        }
        currentIntTerm = term;
        final double value = inner.apply(term, stats, group);
        return handle(group, value);
    }

    @Override
    public AggregateStatTree toImhotep(Map<QualifiedPush, AggregateStatTree> atomicStats) {
        throw new UnsupportedOperationException("Window cannot be pushed up to Imhotep.");
    }

    @Override
    public boolean needSorted() {
        return true;
    }

    @Override
    public boolean needGroup() {
        return true;
    }

    @Override
    public boolean needStats() {
        return inner.needStats();
    }

    private void clear() {
        for (int i = lastGroup + 1; i <= lastGroup + size; i++) {
            if (i < groupToWindowSum.length && groupToWindowSum[i] != 0) {
                throw new IqlKnownException.ExecutionException("Cannot use window where the window overlaps missing data.");
            }
        }
        Arrays.fill(groupToWindowSum, 0.0);
    }

    private double handle(final int group, final double value) {
        iterationStarted = true;
        final int parentGroup = groupKeySet.parentGroup(group);
        for (int offset = 0; offset < size; offset++) {
            if (group + offset <= groupKeySet.numGroups() && groupKeySet.parentGroup(group + offset) == parentGroup) {
                groupToWindowSum[group + offset] += value;
            }
        }
        lastGroup = group;
        return groupToWindowSum[group];
    }
}
