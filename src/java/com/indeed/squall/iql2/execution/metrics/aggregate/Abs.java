package com.indeed.squall.iql2.execution.metrics.aggregate;

import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;

import java.util.Map;
import java.util.Set;

public class Abs implements AggregateMetric {
    private final AggregateMetric value;

    public Abs(AggregateMetric value) {
        this.value = value;
    }

    @Override
    public Set<QualifiedPush> requires() {
        return value.requires();
    }

    @Override
    public void register(Map<QualifiedPush, Integer> metricIndexes, GroupKeySet groupKeySet) {
        value.register(metricIndexes, groupKeySet);
    }

    @Override
    public double[] getGroupStats(long[][] stats, int numGroups) {
        final double[] result = value.getGroupStats(stats, numGroups);
        for (int i = 0; i < result.length; i++) {
            result[i] = Math.abs(result[i]);
        }
        return result;
    }

    @Override
    public double apply(String term, long[] stats, int group) {
        return Math.abs(value.apply(term, stats, group));
    }

    @Override
    public double apply(long term, long[] stats, int group) {
        return Math.abs(value.apply(term, stats, group));
    }
}
