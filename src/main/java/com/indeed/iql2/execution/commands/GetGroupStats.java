/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.iql2.execution.commands;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.ImhotepSessionHolder;
import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.iql2.execution.metrics.aggregate.MultiPerGroupConstant;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

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
    public void execute(final Session session) {
        // this Command needs special processing since it returns some data.
        throw new IllegalStateException("Call evaluate() method instead");
    }

    public List<Session.GroupStats> evaluate(Session session) throws ImhotepOutOfMemoryException {
        final Map<String, ImhotepSessionHolder> sessions = session.getSessionsMapRaw();
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
            final ImhotepSessionHolder s = sessions.get(name);
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
