package com.indeed.squall.jql.metrics.aggregate;

import com.indeed.squall.jql.QualifiedPush;
import com.indeed.squall.jql.Session;

import java.util.Arrays;
import java.util.List;
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
    private List<Session.GroupKey> groupKeys;

    public Window(int size, AggregateMetric inner) {
        this.size = size;
        this.inner = inner;
    }

    @Override
    public Set<QualifiedPush> requires() {
        return inner.requires();
    }

    @Override
    public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
        inner.register(metricIndexes, groupKeys);
        groupToWindowSum = new double[groupKeys.size()];
        this.groupKeys = groupKeys;
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
        final Session.GroupKey parent = groupKeys.get(group).parent;
        for (int offset = 0; offset < size; offset++) {
            if (group + offset < groupKeys.size() && groupKeys.get(group + offset).parent == parent) {
                groupToWindowSum[group + offset] += value;
            }
        }
        lastGroup = group;
        return groupToWindowSum[group];
    }
}
