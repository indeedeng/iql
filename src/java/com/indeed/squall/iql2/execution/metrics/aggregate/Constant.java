package com.indeed.squall.iql2.execution.metrics.aggregate;

import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class Constant implements AggregateMetric {
    private final double value;

    public Constant(final double value) {
        this.value = value;
    }

    @Override
    public Set<QualifiedPush> requires() {
        return Collections.emptySet();
    }

    @Override
    public void register(final Map<QualifiedPush, Integer> metricIndexes, final GroupKeySet groupKeySet) {
    }

    @Override
    public double[] getGroupStats(final long[][] stats, final int numGroups) {
        final double[] result = new double[numGroups + 1];
        Arrays.fill(result, value);
        return result;
    }

    @Override
    public double apply(final String term, final long[] stats, final int group) {
        return value;
    }

    @Override
    public double apply(final long term, final long[] stats, final int group) {
        return value;
    }

    @Override
    public boolean needGroup() {
        return false;
    }

    @Override
    public boolean needStats() {
        return false;
    }
}
