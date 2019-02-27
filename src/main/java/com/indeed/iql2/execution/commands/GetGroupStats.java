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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.ImhotepSessionHolder;
import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.metrics.aggregate.AggregateMetric;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    // returns result as double[numStats][numGroups]
    public double[][] evaluate(final Session session) throws ImhotepOutOfMemoryException {
        final Map<String, ImhotepSessionHolder> sessions = session.getSessionsMapRaw();
        final int numGroups = session.numGroups;

        session.timer.push("determining pushes");
        final Set<QualifiedPush> pushesRequired = Sets.newHashSet();
        for (final AggregateMetric metric : metrics) {
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
                sessionPushes.put(sessionName, Lists.newArrayList());
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
        for (final AggregateMetric metric : metrics) {
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
                allStats[positions.get(i)] = s.getGroupStats(pushesForSession.get(i).pushes);
            }
        }
        session.timer.pop();

        session.timer.push("computing aggregated stats");
        final List<AggregateMetric> selectedMetrics = metrics;

        final double[][] groupStats = new double[selectedMetrics.size()][];
        for (int i = 0; i < selectedMetrics.size(); i++) {
            final AggregateMetric metric = selectedMetrics.get(i);
            final double[] statGroups = metric.getGroupStats(allStats, numGroups);
            groupStats[i] = statGroups;
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
