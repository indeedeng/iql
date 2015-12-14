package com.indeed.squall.iql2.execution.commands;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.SessionCallback;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.sets.MetricRangeGroupKeySet;
import com.indeed.util.core.TreeTimer;

import java.util.List;
import java.util.Map;

public class MetricRegroup implements Command {
    public final ImmutableMap<String, ImmutableList<String>> perDatasetMetric;
    public final long min;
    public final long max;
    public final long interval;
    public final boolean excludeGutters;

    public MetricRegroup(Map<String, List<String>> perDatasetMetric, long min, long max, long interval, boolean excludeGutters) {
        this.excludeGutters = excludeGutters;
        final ImmutableMap.Builder<String, ImmutableList<String>> copy = ImmutableMap.builder();
        for (final Map.Entry<String, List<String>> entry : perDatasetMetric.entrySet()) {
            copy.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
        }
        this.perDatasetMetric = copy.build();
        this.min = min;
        this.max = max;
        this.interval = interval;
    }

    @Override
    public void execute(final Session session, Consumer<String> out) throws ImhotepOutOfMemoryException {
        final long max = this.max;
        final long min = this.min;
        final long interval = this.interval;
        final Map<String, ? extends List<String>> perDatasetMetrics = this.perDatasetMetric;

        final int numBuckets = (excludeGutters ? 0 : 2) + (int) Math.ceil(((double) max - min) / interval);

        session.checkGroupLimit(numBuckets * session.numGroups);

        session.process(new SessionCallback() {
            @Override
            public void handle(TreeTimer timer, String name, ImhotepSession session) throws ImhotepOutOfMemoryException {
                if (!perDatasetMetrics.containsKey(name)) {
                    return;
                }
                final List<String> pushes = perDatasetMetrics.get(name);

                timer.push("pushStats");
                final int numStats = session.pushStats(pushes);
                timer.pop();

                if (numStats != 1) {
                    throw new IllegalStateException("Pushed more than one stat!: " + pushes);
                }

                timer.push("metricRegroup");
                session.metricRegroup(0, min, max, interval, excludeGutters);
                timer.pop();

                timer.push("popStat");
                session.popStat();
                timer.pop();
            }
        });

        session.densify(new MetricRangeGroupKeySet(session.groupKeySet, numBuckets, excludeGutters, min, interval));

        session.currentDepth += 1;

        out.accept("success");
    }
}
