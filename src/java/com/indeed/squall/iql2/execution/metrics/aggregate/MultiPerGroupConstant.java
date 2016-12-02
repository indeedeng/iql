package com.indeed.squall.iql2.execution.metrics.aggregate;

import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MultiPerGroupConstant implements AggregateMetric {
    public final List<double[]> values;

    public MultiPerGroupConstant(List<double[]> values) {
        this.values = values;
    }

    @Override
    public Set<QualifiedPush> requires() {
        return Collections.emptySet();
    }

    @Override
    public void register(Map<QualifiedPush, Integer> metricIndexes, GroupKeySet groupKeySet) {
    }

    @Override
    public double[] getGroupStats(long[][] stats, int numGroups) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double apply(String term, long[] stats, int group) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double apply(long term, long[] stats, int group) {
        throw new UnsupportedOperationException();
    }
}
