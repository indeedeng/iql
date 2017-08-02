package com.indeed.squall.iql2.execution.metrics.aggregate;

import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class Window implements AggregateMetric {
    private final int size;
    private final AggregateMetric inner;

    private boolean iterationStarted = false;
    private long currentIntTerm = 0;
    private String currentStringTerm = null;
    private int lastGroup = 0;

    private double[] groupToWindowSum;
    private GroupKeySet groupKeySet;

    public Window(int size, AggregateMetric inner) {
        this.size = size;
        this.inner = inner;
    }

    @Override
    public Set<QualifiedPush> requires() {
        return inner.requires();
    }

    @Override
    public void register(Map<QualifiedPush, Integer> metricIndexes, GroupKeySet groupKeySet) {
        inner.register(metricIndexes, groupKeySet);
        groupToWindowSum = new double[groupKeySet.numGroups() + 1];
        this.groupKeySet = groupKeySet;
    }

    @Override
    public double[] getGroupStats(long[][] stats, int numGroups) {
        final double[] innerResult = inner.getGroupStats(stats, numGroups);
        final double[] result = new double[numGroups + 1];
        double sum = 0;
        int currentParent = -1;
        int count = 0;
        for (int i = 1; i <= numGroups; i++) {
            final int parent = groupKeySet.parentGroup(i);
            if (parent != currentParent) {
                currentParent = parent;
                sum = 0;
                count = 0;
            }
            sum += innerResult[i];
            count += 1;
            if (count > size) {
                sum -= innerResult[i - size];
            }
            result[i] = sum;
        }
        return result;
    }

    @Override
    public double apply(String term, long[] stats, int group) {
        if (iterationStarted && !term.equals(currentStringTerm)) {
            clear();
        }
        currentStringTerm = term;
        final double value = inner.apply(term, stats, group);
        return handle(group, value);
    }

    @Override
    public double apply(long term, long[] stats, int group) {
        if (iterationStarted && term != currentIntTerm) {
            clear();
        }
        currentIntTerm = term;
        final double value = inner.apply(term, stats, group);
        return handle(group, value);
    }

    private void clear() {
        for (int i = lastGroup + 1; i <= lastGroup + size; i++) {
            if (i < groupToWindowSum.length && groupToWindowSum[i] != 0) {
                throw new IllegalStateException("Cannot use window where the window overlaps missing data.");
            }
        }
        Arrays.fill(groupToWindowSum, 0.0);
    }

    private double handle(int group, double value) {
        iterationStarted = true;
        final int parentGroup = groupKeySet.parentGroup(group);
        for (int offset = 0; offset < size; offset++) {
            if (group + offset <= groupKeySet.numGroups() && groupKeySet.parentGroup(group + offset) == parentGroup) {
                groupToWindowSum[group + offset] += value;
            }
        }
        lastGroup = group;
        return groupToWindowSum[group];
    }
}
