package com.indeed.squall.iql2.execution.metrics.aggregate;

import com.google.common.collect.Sets;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;

import java.util.Map;
import java.util.Set;

public class Modulus implements AggregateMetric {
    private final AggregateMetric x;
    private final AggregateMetric y;

    public Modulus(AggregateMetric x, AggregateMetric y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public Set<QualifiedPush> requires() {
        return Sets.union(x.requires(), y.requires());
    }

    @Override
    public void register(Map<QualifiedPush, Integer> metricIndexes, GroupKeySet groupKeySet) {
        x.register(metricIndexes, groupKeySet);
        y.register(metricIndexes, groupKeySet);
    }

    @Override
    public double[] getGroupStats(long[][] stats, int numGroups) {
        final double[] lhs = x.getGroupStats(stats, numGroups);
        final double[] rhs = y.getGroupStats(stats, numGroups);
        for (int i = 0; i < rhs.length; i++) {
            lhs[i] %= rhs[i];
        }
        return lhs;
    }

    @Override
    public double apply(String term, long[] stats, int group) {
        return x.apply(term, stats, group) % y.apply(term, stats, group);
    }

    @Override
    public double apply(long term, long[] stats, int group) {
        return x.apply(term, stats, group) % y.apply(term, stats, group);
    }
}
