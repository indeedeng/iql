package com.indeed.squall.jql.commands;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.jql.AggregateMetric;
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

    public List<Session.GroupStats> execute(List<Session.GroupKey> groupKeys, Map<String, ImhotepSession> sessions, int numGroups, boolean returnGroupKeys) throws ImhotepOutOfMemoryException {
        final Set<QualifiedPush> pushesRequired = Sets.newHashSet();
        this.metrics.forEach(metric -> pushesRequired.addAll(metric.requires()));
        final Map<QualifiedPush, Integer> metricIndexes = Maps.newHashMap();
        final Map<String, IntList> sessionMetricIndexes = Maps.newHashMap();
        int numStats = 0;
        for (final QualifiedPush push : pushesRequired) {
            final int index = numStats++;
            metricIndexes.put(push, index);
            final String sessionName = push.sessionName;
            sessions.get(sessionName).pushStats(push.pushes);
            sessionMetricIndexes.computeIfAbsent(sessionName, k -> new IntArrayList()).add(index);
        }

        this.metrics.forEach(metric -> metric.register(metricIndexes, groupKeys));

        final long[][] allStats = new long[numStats][];
        sessionMetricIndexes.forEach((name, positions) -> {
            final ImhotepSession session = sessions.get(name);
            for (int i = 0; i < positions.size(); i++) {
                allStats[positions.get(i)] = session.getGroupStats(i);
            }
        });

        final List<AggregateMetric> selectedMetrics = this.metrics;
        final double[][] results = new double[numGroups][selectedMetrics.size()];
        final long[] groupStatsBuf = new long[allStats.length];
        for (int group = 1; group <= numGroups; group++) {
            for (int j = 0; j < allStats.length; j++) {
                groupStatsBuf[j] = allStats[j].length > group ? allStats[j][group] : 0L;
            }
            for (int j = 0; j < selectedMetrics.size(); j++) {
                results[group - 1][j] = selectedMetrics.get(j).apply(0, groupStatsBuf, group);
            }
        }

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

        sessions.values().forEach(session -> {
            while (session.getNumStats() > 0) {
                session.popStat();
            }
        });

        return groupStats;
    }

}
