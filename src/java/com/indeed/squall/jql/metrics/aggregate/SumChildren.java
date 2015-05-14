package com.indeed.squall.jql.metrics.aggregate;

import com.indeed.squall.jql.QualifiedPush;
import com.indeed.squall.jql.Session;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SumChildren implements AggregateMetric {
    private final AggregateMetric metric;

    private List<Session.GroupKey> groupKeys;

    public SumChildren(AggregateMetric metric) {
        this.metric = metric;
    }

    @Override
    public Set<QualifiedPush> requires() {
        return metric.requires();
    }

    @Override
    public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
        this.groupKeys = groupKeys;
        metric.register(metricIndexes, groupKeys);
    }

    @Override
    public double[] getGroupStats(long[][] stats, int numGroups) {
        final double[] innerStats = metric.getGroupStats(stats, numGroups);
        final double[] result = new double[numGroups + 1];
        Session.GroupKey currentParent = null;
        int start = 1;
        double sum = 0;
        for (int i = 1; i <= numGroups; i++) {
            final Session.GroupKey parent = groupKeys.get(i).parent;
            if (parent != currentParent) {
                if (start != -1) {
                    Arrays.fill(result, start, i, sum);
                }
                currentParent = parent;
                start = i;
                sum = 0;
            }
            sum += innerStats[i];
        }
        Arrays.fill(result, start, numGroups + 1, sum);
        return result;
    }

    @Override
    public double apply(String term, long[] stats, int group) {
        throw new IllegalArgumentException("Cannot stream SumChildren");
    }

    @Override
    public double apply(long term, long[] stats, int group) {
        throw new IllegalArgumentException("Cannot stream SumChildren");
    }
}
