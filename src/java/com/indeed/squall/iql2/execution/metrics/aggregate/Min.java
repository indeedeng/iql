package com.indeed.squall.iql2.execution.metrics.aggregate;

import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Min implements AggregateMetric {
    private final List<AggregateMetric> metrics;

    public Min(List<AggregateMetric> metrics) {
        this.metrics = metrics;
    }

    @Override
    public double[] getGroupStats(long[][] stats, int numGroups) {
        final double[] resultStats = new double[numGroups + 1];
        for (final AggregateMetric metric : metrics) {
            final double[] mStats = metric.getGroupStats(stats, numGroups);
            for (int i = 0; i <= numGroups; i++) {
                resultStats[i] = Math.min(resultStats[i], mStats[i]);
            }
        }
        return resultStats;
    }

    @Override
    public double apply(String term, long[] stats, int group) {
        double result = metrics.get(0).apply(term, stats, group);
        for (int i = 1; i < metrics.size(); i++) {
            result = Math.min(result, metrics.get(i).apply(term, stats, group));
        }
        return result;
    }

    @Override
    public double apply(long term, long[] stats, int group) {
        double result = metrics.get(0).apply(term, stats, group);
        for (int i = 1; i < metrics.size(); i++) {
            result = Math.min(result, metrics.get(i).apply(term, stats, group));
        }
        return result;
    }

    @Override
    public Set<QualifiedPush> requires() {
        final Set<QualifiedPush> result = new HashSet<>();
        for (final AggregateMetric metric : metrics) {
            result.addAll(metric.requires());
        }
        return result;
    }

    @Override
    public void register(Map<QualifiedPush, Integer> metricIndexes, GroupKeySet groupKeySet) {
        for (final AggregateMetric metric : metrics) {
            metric.register(metricIndexes, groupKeySet);
        }
    }
}
