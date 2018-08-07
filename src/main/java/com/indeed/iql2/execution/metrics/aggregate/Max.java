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

import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Max implements AggregateMetric {
    private final List<AggregateMetric> metrics;

    public Max(final List<AggregateMetric> metrics) {
        this.metrics = metrics;
    }

    @Override
    public double[] getGroupStats(final long[][] stats, final int numGroups) {
        final double[] resultStats = metrics.get(0).getGroupStats(stats, numGroups);
        for (int metricIndex = 1; metricIndex < metrics.size(); metricIndex++) {
            final double[] mStats = metrics.get(metricIndex).getGroupStats(stats, numGroups);
            for (int i = 0; i <= numGroups; i++) {
                resultStats[i] = Math.max(resultStats[i], mStats[i]);
            }
        }
        return resultStats;
    }

    @Override
    public double apply(final String term, final long[] stats, final int group) {
        double result = metrics.get(0).apply(term, stats, group);
        for (int i = 1; i < metrics.size(); i++) {
            result = Math.max(result, metrics.get(i).apply(term, stats, group));
        }
        return result;
    }

    @Override
    public double apply(final long term, final long[] stats, final int group) {
        double result = metrics.get(0).apply(term, stats, group);
        for (int i = 1; i < metrics.size(); i++) {
            result = Math.max(result, metrics.get(i).apply(term, stats, group));
        }
        return result;
    }

    @Override
    public boolean needSorted() {
        return metrics.stream().anyMatch(AggregateMetric::needSorted);
    }

    @Override
    public boolean needGroup() {
        return metrics.stream().anyMatch(AggregateMetric::needGroup);
    }

    @Override
    public boolean needStats() {
        return metrics.stream().anyMatch(AggregateMetric::needStats);
    }

    @Override
    public Set<QualifiedPush> requires() {
        final Set<QualifiedPush> result = new HashSet<>();
        metrics.forEach(metric -> result.addAll(metric.requires()));
        return result;
    }

    @Override
    public void register(final Map<QualifiedPush, Integer> metricIndexes, final GroupKeySet groupKeySet) {
        metrics.forEach(metric -> metric.register(metricIndexes, groupKeySet));
    }
}
