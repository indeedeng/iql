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

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Set;

/**
 * TODO: This has a total hack in which it assumes that values are going to come in in time sorted order.
 *       So long as this is true, we only ever need to store the previous [delay] values and their GroupKeys.
 */
public class ParentLag implements AggregateMetric {
    private final int delay;
    private final AggregateMetric metric;

    private final ArrayDeque<Double> prevScores; // dear god this is terrible
    private final ArrayDeque<Integer> prevGroupKeys; // really bad..

    private GroupKeySet groupKeySet;

    public ParentLag(final int delay, final AggregateMetric metric) {
        this.delay = delay;
        this.metric = metric;
        prevScores = new ArrayDeque<>(delay + 1);
        prevGroupKeys = new ArrayDeque<>(delay + 1);
    }

    @Override
    public Set<QualifiedPush> requires() {
        return metric.requires();
    }

    @Override
    public void register(final Map<QualifiedPush, Integer> metricIndexes, final GroupKeySet groupKeySet) {
        metric.register(metricIndexes, groupKeySet);
        this.groupKeySet = groupKeySet;
    }

    @Override
    public double[] getGroupStats(final long[][] stats, final int numGroups) {
        final double[] innerResult = metric.getGroupStats(stats, numGroups);
        final double[] result = new double[numGroups + 1];
        for (int i = 1; i <= numGroups; i++) {
            result[i] = handle(i, innerResult[i]);
        }
        return result;
    }

    @Override
    public double apply(final String term, final long[] stats, final int group) {
        return handle(group, metric.apply(term, stats, group));
    }

    @Override
    public double apply(final long term, final long[] stats, final int group) {
        return handle(group, metric.apply(term, stats, group));
    }

    @Override
    public boolean needGroup() {
        return true;
    }

    @Override
    public boolean needStats() {
        return metric.needStats();
    }

    // TODO: This doesn't seem right...
    private double handle(final int group, final double metricResult) {
        final int parent = groupKeySet.parentGroup(group);
        int targetGroupKey = -1;
        for (final int key : prevGroupKeys) {
            if (groupKeySet.parentGroup(key) == parent && key == group - delay) {
                targetGroupKey = key;
                break;
            }
        }

        double result = 0.0;
        if (targetGroupKey != -1) {
            while (true) {
                final int key = prevGroupKeys.removeFirst();
                final double score = prevScores.removeFirst();
                if (key == targetGroupKey) {
                    result = score;
                    break;
                }
            }
        }

        prevScores.addLast(metricResult);
        prevGroupKeys.addLast(group);
        if (prevScores.size() > delay) {
            prevScores.removeFirst();
            prevGroupKeys.removeFirst();
        }

        return result;
    }
}
