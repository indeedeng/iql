package com.indeed.squall.jql.commands;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.indeed.common.util.Pair;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.jql.Session;

import java.util.List;
import java.util.Map;

public class MetricRegroup {
    public final ImmutableMap<String, ImmutableList<String>> perDatasetMetric;
    public final long min;
    public final long max;
    public final long interval;

    public MetricRegroup(Map<String, List<String>> perDatasetMetric, long min, long max, long interval) {
        final ImmutableMap.Builder<String, ImmutableList<String>> copy = ImmutableMap.builder();
        perDatasetMetric.forEach((k,v) -> copy.put(k, ImmutableList.copyOf(v)));
        this.perDatasetMetric = copy.build();
        this.min = min;
        this.max = max;
        this.interval = interval;
    }

    public static void metricRegroup(MetricRegroup metricRegroup, Session session) throws ImhotepOutOfMemoryException {
        final long max = metricRegroup.max;
        final long min = metricRegroup.min;
        final long interval = metricRegroup.interval;
        final Map<String, ? extends List<String>> perDatasetMetrics = metricRegroup.perDatasetMetric;

        final int numBuckets = 2 + (int) Math.ceil(((double) max - min) / interval);

        // TODO: Do these in parallel?
        perDatasetMetrics.forEach((name, pushes) -> Session.unchecked(() -> {
            final ImhotepSession s = session.sessions.get(name).session;
            final int numStats = s.pushStats(pushes);
            if (numStats != 1) {
                throw new IllegalStateException("Pushed more than one stat!: " + pushes);
            }
            s.metricRegroup(0, min, max, interval);
            s.popStat();
        }));

        session.densify(group -> {
            final int oldGroup = 1 + (group - 1) / numBuckets;
            final int innerGroup = (group - 1) % numBuckets;
            final String key;
            if (innerGroup == numBuckets - 1) {
                key = "[" + (min + interval * (numBuckets - 2)) + ", " + Session.INFINITY_SYMBOL + ")";
            } else if (innerGroup == numBuckets - 2) {
                key = "[-" + Session.INFINITY_SYMBOL + ", " + min + ")";
            } else if (interval == 1) {
                key = String.valueOf(min + innerGroup);
            } else {
                final long minInclusive = min + innerGroup * interval;
                final long maxExclusive = min + (innerGroup + 1) * interval;
                key = "[" + minInclusive + ", " + maxExclusive + ")";
            }
            return Pair.of(key, session.groupKeys.get(oldGroup));
        });
    }
}
