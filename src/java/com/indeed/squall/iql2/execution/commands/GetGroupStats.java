package com.indeed.squall.iql2.execution.commands;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GetGroupStats implements Command {
    public final List<AggregateMetric> metrics;
    public final boolean returnGroupKeys;

    public GetGroupStats(List<AggregateMetric> metrics, boolean returnGroupKeys) {
        this.metrics = metrics;
        this.returnGroupKeys = returnGroupKeys;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        out.accept(Session.MAPPER.writeValueAsString(evaluate(session)));
    }

    public List<Session.GroupStats> evaluate(Session session) throws ImhotepOutOfMemoryException {
        final List<Session.GroupKey> groupKeys = session.groupKeys;
        final Map<String, ImhotepSession> sessions = session.getSessionsMapRaw();
        final int numGroups = session.numGroups;

        session.timer.push("determining pushes");
        final Set<QualifiedPush> pushesRequired = Sets.newHashSet();
        for (final AggregateMetric metric : this.metrics) {
            pushesRequired.addAll(metric.requires());
        }
        final Map<QualifiedPush, Integer> metricIndexes = Maps.newHashMap();
        final Map<String, IntList> sessionMetricIndexes = Maps.newHashMap();
        session.timer.pop();
        session.timer.push("pushing stats");
        int numStats = 0;
        for (final QualifiedPush push : pushesRequired) {
            final int index = numStats++;
            metricIndexes.put(push, index);
            final String sessionName = push.sessionName;
            sessions.get(sessionName).pushStats(push.pushes);
            IntList metricIndex = sessionMetricIndexes.get(sessionName);
            if (metricIndex == null) {
                metricIndex = new IntArrayList();
                sessionMetricIndexes.put(sessionName, metricIndex);
            }
            metricIndex.add(index);
        }
        session.timer.pop();
        session.timer.push("registering stats");
        for (final AggregateMetric metric : this.metrics) {
            metric.register(metricIndexes, groupKeys);
        }
        session.timer.pop();

        session.timer.push("getGroupStats");
        final long[][] allStats = new long[numStats][];
        for (final Map.Entry<String, IntList> entry : sessionMetricIndexes.entrySet()) {
            final String name = entry.getKey();
            final IntList positions = entry.getValue();
            final ImhotepSession s = sessions.get(name);
            for (int i = 0; i < positions.size(); i++) {
                allStats[positions.get(i)] = s.getGroupStats(i);
            }
        }
        session.timer.pop();

        session.timer.push("computing aggregated stats");
        final List<AggregateMetric> selectedMetrics = this.metrics;
        final double[][] results = new double[numGroups][selectedMetrics.size()];

        for (int i = 0; i < selectedMetrics.size(); i++) {
            final double[] statGroups = selectedMetrics.get(i).getGroupStats(allStats, numGroups);
            for (int j = 1; j <= numGroups; j++) {
                results[j - 1][i] = statGroups[j];
            }
        }
        session.timer.pop();

        session.timer.push("creating result");
        final List<Session.GroupStats> groupStats = Lists.newArrayList();
        for (int i = 0; i < numGroups; i++) {
            final Session.GroupKey groupKey;
            if (returnGroupKeys) {
                groupKey = groupKeys.get(i + 1);
            } else {
                groupKey = null;
            }
            groupStats.add(new Session.GroupStats(groupKey, results[i]));
        }
        session.timer.pop();

        session.popStats();

        return groupStats;
    }

    @Override
    public String toString() {
        return "GetGroupStats{" +
                "metrics=" + metrics +
                ", returnGroupKeys=" + returnGroupKeys +
                '}';
    }
}
