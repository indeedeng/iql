package com.indeed.squall.iql2.execution.metrics.aggregate;

import com.google.common.collect.Sets;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;

import java.util.Map;
import java.util.Set;

public class Subtract implements AggregateMetric {
    private final AggregateMetric m1;
    private final AggregateMetric m2;

    public Subtract(AggregateMetric m1, AggregateMetric m2) {
        this.m1 = m1;
        this.m2 = m2;
    }

    @Override
    public Set<QualifiedPush> requires() {
        return Sets.union(m1.requires(), m2.requires());
    }

    @Override
    public void register(Map<QualifiedPush, Integer> metricIndexes, GroupKeySet groupKeySet) {
        m1.register(metricIndexes, groupKeySet);
        m2.register(metricIndexes, groupKeySet);
    }

    @Override
    public double[] getGroupStats(long[][] stats, int numGroups) {
        final double[] lhs = m1.getGroupStats(stats, numGroups);
        final double[] rhs = m2.getGroupStats(stats, numGroups);
        for (int i = 0; i < rhs.length; i++) {
            lhs[i] -= rhs[i];
        }
        return lhs;
    }

    @Override
    public double apply(String term, long[] stats, int group) {
        return m1.apply(term, stats, group) - m2.apply(term, stats, group);
    }

    @Override
    public double apply(long term, long[] stats, int group) {
        return m1.apply(term, stats, group) - m2.apply(term, stats, group);
    }
}
