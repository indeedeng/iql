package com.indeed.squall.jql.commands;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.jql.metrics.aggregate.AggregateMetric;
import com.indeed.squall.jql.QualifiedPush;
import com.indeed.squall.jql.Session;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class GetGroupStats {
    public final List<AggregateMetric> metrics;
    public final boolean returnGroupKeys;

    public GetGroupStats(List<AggregateMetric> metrics, boolean returnGroupKeys) {
        this.metrics = metrics;
        this.returnGroupKeys = returnGroupKeys;
    }

    public List<Session.GroupStats> execute(Session session) throws ImhotepOutOfMemoryException {
        final List<Session.GroupKey> groupKeys = session.groupKeys;
        final Map<String, ImhotepSession> sessions = session.getSessionsMapRaw();
        final int numGroups = session.numGroups;

        session.timer.push("determining pushes");
        final Set<QualifiedPush> pushesRequired = Sets.newHashSet();
        this.metrics.forEach(metric -> pushesRequired.addAll(metric.requires()));
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
            sessionMetricIndexes.computeIfAbsent(sessionName, k -> new IntArrayList()).add(index);
        }
        session.timer.pop();
        session.timer.push("registering stats");
        this.metrics.forEach(metric -> metric.register(metricIndexes, groupKeys));
        session.timer.pop();

        session.timer.push("getGroupStats");
        final long[][] allStats = new long[numStats][];
        sessionMetricIndexes.forEach((name, positions) -> {
            final ImhotepSession s = sessions.get(name);
            for (int i = 0; i < positions.size(); i++) {
                allStats[positions.get(i)] = s.getGroupStats(i);
            }
        });
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

        session.timer.push("popStat");
        sessions.values().forEach(s -> {
            while (s.getNumStats() > 0) {
                s.popStat();
            }
        });
        session.timer.pop();

        return groupStats;
    }

}
