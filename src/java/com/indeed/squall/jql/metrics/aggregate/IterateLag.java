package com.indeed.squall.jql.metrics.aggregate;

import com.indeed.squall.jql.QualifiedPush;
import com.indeed.squall.jql.Session;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IterateLag implements AggregateMetric {
    private final int delay;
    private final AggregateMetric metric;

    private final Int2ObjectMap<ArrayDeque<Double>> prevScores; // dear god this is terrible

    public IterateLag(int delay, AggregateMetric metric) {
        this.delay = delay;
        this.metric = metric;
        this.prevScores = new Int2ObjectOpenHashMap<>();
    }

    @Override
    public Set<QualifiedPush> requires() {
        return metric.requires();
    }

    @Override
    public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
        metric.register(metricIndexes, groupKeys);
    }

    @Override
    public double[] getGroupStats(long[][] stats, int numGroups) {
        throw new UnsupportedOperationException("Shouldn't hit IterateLag in GetGroupStats");
    }

    @Override
    public double apply(String term, long[] stats, int group) {
        final double value = metric.apply(term, stats, group);
        return this.handle(value, group);
    }

    @Override
    public double apply(long term, long[] stats, int group) {
        final double value = metric.apply(term, stats, group);
        return this.handle(value, group);
    }

    private double handle(double value, int group) {
        final ArrayDeque<Double> groupPrevScores = prevScores.computeIfAbsent(group, k -> new ArrayDeque<>(delay + 1));
        groupPrevScores.addLast(value);
        if (groupPrevScores.size() > delay) {
            return groupPrevScores.pollFirst();
        } else {
            return 0;
        }
    }
}
