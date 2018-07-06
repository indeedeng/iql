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
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Set;

public class IterateLag implements AggregateMetric {
    private final int delay;
    private final AggregateMetric metric;

    private final Int2ObjectMap<ArrayDeque<Double>> prevScores; // dear god this is terrible

    public IterateLag(final int delay, final AggregateMetric metric) {
        this.delay = delay;
        this.metric = metric;
        this.prevScores = new Int2ObjectOpenHashMap<>();
    }

    @Override
    public Set<QualifiedPush> requires() {
        return metric.requires();
    }

    @Override
    public void register(final Map<QualifiedPush, Integer> metricIndexes,
                         final GroupKeySet groupKeySet) {
        metric.register(metricIndexes, groupKeySet);
    }

    @Override
    public double[] getGroupStats(final long[][] stats, final int numGroups) {
        throw new UnsupportedOperationException("Shouldn't hit IterateLag in GetGroupStats");
    }

    @Override
    public double apply(final String term, final long[] stats, final int group) {
        final double value = metric.apply(term, stats, group);
        return handle(value, group);
    }

    @Override
    public double apply(final long term, final long[] stats, final int group) {
        final double value = metric.apply(term, stats, group);
        return handle(value, group);
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
        return metric.needStats();
    }

    private double handle(final double value, final int group) {
        ArrayDeque<Double> groupPrevScores = prevScores.get(group);
        if (groupPrevScores == null) {
            groupPrevScores = new ArrayDeque<>(delay + 1);
            prevScores.put(group, groupPrevScores);
        }
        groupPrevScores.addLast(value);
        if (groupPrevScores.size() > delay) {
            return groupPrevScores.pollFirst();
        } else {
            return 0;
        }
    }
}
