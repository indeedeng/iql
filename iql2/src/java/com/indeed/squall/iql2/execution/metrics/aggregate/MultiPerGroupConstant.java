package com.indeed.squall.iql2.execution.metrics.aggregate;

import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MultiPerGroupConstant implements AggregateMetric {
    public final List<double[]> values;

    public MultiPerGroupConstant(final List<double[]> values) {
        this.values = values;
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
        throw new UnsupportedOperationException();
    }

    @Override
    public double apply(final String term, final long[] stats, final int group) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double apply(final long term, final long[] stats, final int group) {
        throw new UnsupportedOperationException();
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
