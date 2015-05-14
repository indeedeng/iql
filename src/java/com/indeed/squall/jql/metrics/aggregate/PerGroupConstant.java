package com.indeed.squall.jql.metrics.aggregate;

import com.indeed.squall.jql.QualifiedPush;
import com.indeed.squall.jql.Session;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PerGroupConstant implements AggregateMetric {
    private final double[] values;

    public PerGroupConstant(double[] values) {
        this.values = values;
    }

    @Override
    public Set<QualifiedPush> requires() {
        return Collections.emptySet();
    }

    @Override
    public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
    }

    @Override
    public double[] getGroupStats(long[][] stats, int numGroups) {
        return Arrays.copyOf(values, numGroups + 1);
    }

    @Override
    public double apply(String term, long[] stats, int group) {
        return values[group];
    }

    @Override
    public double apply(long term, long[] stats, int group) {
        return values[group];
    }
}
