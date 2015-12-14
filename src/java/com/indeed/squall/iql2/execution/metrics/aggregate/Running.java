package com.indeed.squall.iql2.execution.metrics.aggregate;

import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.groupkeys.GroupKeySet;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;

import java.util.Map;
import java.util.Set;

public class Running implements AggregateMetric {
    private final AggregateMetric inner;
    private final int offset;
    private final Map<Integer, Double> groupSums = new Int2DoubleOpenHashMap();
    private int[] groupToRealGroup;

    public Running(AggregateMetric inner, int offset) {
        this.inner = inner;
        this.offset = offset;
    }

    @Override
    public Set<QualifiedPush> requires() {
        return inner.requires();
    }

    @Override
    public void register(Map<QualifiedPush, Integer> metricIndexes, GroupKeySet groupKeySet) {
        inner.register(metricIndexes, groupKeySet);
        this.groupToRealGroup = new int[groupKeySet.numGroups()];
        for (int group = 1; group < groupKeySet.numGroups(); group++) {
            GroupKeySet keyset = groupKeySet;
            int index = group;
            for (int i = 0; i < offset; i++) {
                index = keyset.parentGroup(index);
                keyset = keyset.previous();
            }
            groupToRealGroup[group] = index;
        }
    }

    @Override
    public double[] getGroupStats(long[][] stats, int numGroups) {
        final double[] innerResult = inner.getGroupStats(stats, numGroups);
        double sum = 0;
        int currentParent = -1;
        final double[] result = new double[numGroups + 1];
        for (int i = 1; i <= numGroups; i++) {
            final int parent = groupToRealGroup[i];
            if (parent != currentParent) {
                sum = 0;
                currentParent = parent;
            }
            sum += innerResult[i];
            result[i] = sum;
        }
        return result;
    }

    @Override
    public double apply(String term, long[] stats, int group) {
        final double val = inner.apply(term, stats, group);
        return getResult(groupToRealGroup[group], val);
    }

    @Override
    public double apply(long term, long[] stats, int group) {
        final double val = inner.apply(term, stats, group);
        return getResult(groupToRealGroup[group], val);
    }

    private double getResult(int group, double val) {
        Double v = groupSums.get(group);
        final double result;
        if (v != null) {
            result = v + val;
        } else {
            result = val;
        }
        groupSums.put(group, result);
        return result;
    }
}
