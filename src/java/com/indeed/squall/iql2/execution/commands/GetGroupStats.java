package com.indeed.squall.iql2.execution.commands;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.squall.iql2.execution.metrics.aggregate.MultiPerGroupConstant;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GetGroupStats implements Command {
    public final List<AggregateMetric> metrics;
    public final List<Optional<String>> formatStrings;
    public final boolean returnGroupKeys;

    public GetGroupStats(List<AggregateMetric> metrics, List<Optional<String>> formatStrings, boolean returnGroupKeys) {
        this.metrics = metrics;
        this.formatStrings = formatStrings;
        this.returnGroupKeys = returnGroupKeys;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        out.accept(stringify(evaluate(session)));
    }

    private String stringify(List<Session.GroupStats> groupStatses) {
        final String[] formatStrings = new String[metrics.size()];
        for (int i = 0; i < formatStrings.length; i++) {
            formatStrings[i] = this.formatStrings.get(i).orNull();
        }

        final StringBuilder sb = new StringBuilder();
        // TODO: This is all horrible. Like seriously.
        sb.append('[');
        boolean firstGs = true;
        for (final Session.GroupStats gs : groupStatses) {
            if (!firstGs) {
                sb.append(',');
            }
            firstGs = false;
            sb.append('{');
            sb.append("\"group\":").append(gs.group);
            sb.append(',').append("\"stats\":");
            sb.append('[');
            double[] stats = gs.stats;
            for (int i = 0; i < stats.length; i++) {
                final double stat = stats[i];
                if (i > 0) {
                    sb.append(',');
                }
                if (formatStrings[i] == null) {
                    sb.append(String.valueOf(stat));
                } else {
                    sb.append(String.format(formatStrings[i], stat));
                }
            }
            sb.append(']');
            sb.append('}');
        }
        sb.append(']');
        return sb.toString();
    }

    public List<Session.GroupStats> evaluate(Session session) throws ImhotepOutOfMemoryException {
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
        session.timer.push("ordering stats");
        int numStats = 0;
        final Map<String, List<QualifiedPush>> sessionPushes = Maps.newHashMap();
        for (final QualifiedPush push : pushesRequired) {
            final int index = numStats++;
            metricIndexes.put(push, index);
            final String sessionName = push.sessionName;
            if (!sessionPushes.containsKey(sessionName)) {
                sessionPushes.put(sessionName, Lists.<QualifiedPush>newArrayList());
            }
            sessionPushes.get(sessionName).add(push);
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
            metric.register(metricIndexes, session.groupKeySet);
        }
        session.timer.pop();

        session.timer.push("pushing / getGroupStats");
        final long[][] allStats = new long[numStats][];
        // TODO: Parallelize acrosss sessions.
        for (final Map.Entry<String, IntList> entry : sessionMetricIndexes.entrySet()) {
            final String name = entry.getKey();
            final List<QualifiedPush> pushesForSession = sessionPushes.get(name);
            final IntList positions = entry.getValue();
            final ImhotepSession s = sessions.get(name);
            for (int i = 0; i < positions.size(); i++) {
                s.pushStats(pushesForSession.get(i).pushes);
                allStats[positions.get(i)] = s.getGroupStats(0);
                s.popStat();
            }
        }
        session.timer.pop();

        session.timer.push("computing aggregated stats");
        final List<AggregateMetric> selectedMetrics = this.metrics;

        int totalStats = 0;
        for (final AggregateMetric metric : selectedMetrics) {
            if (metric instanceof MultiPerGroupConstant) {
                totalStats += ((MultiPerGroupConstant) metric).values.size();
            } else {
                totalStats += 1;
            }
        }

        final double[][] results = new double[numGroups][totalStats];
        int statIndex = 0;
        for (final AggregateMetric metric : selectedMetrics) {
            if (metric instanceof MultiPerGroupConstant) {
                for (final double[] value : ((MultiPerGroupConstant) metric).values) {
                    for (int j = 1; j <= numGroups; j++) {
                        results[j - 1][statIndex] = value[j];
                    }
                    statIndex += 1;
                }
            } else {
                final double[] statGroups = metric.getGroupStats(allStats, numGroups);
                for (int j = 1; j <= numGroups; j++) {
                    results[j - 1][statIndex] = statGroups[j];
                }
                statIndex += 1;
            }
        }
        session.timer.pop();

        session.timer.push("creating result");
        final List<Session.GroupStats> groupStats = Lists.newArrayList();
        for (int i = 0; i < numGroups; i++) {
            groupStats.add(new Session.GroupStats(i + 1, results[i]));
        }
        session.timer.pop();

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
