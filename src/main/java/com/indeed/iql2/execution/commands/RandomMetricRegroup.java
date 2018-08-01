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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.SessionCallback;
import com.indeed.iql2.execution.compat.Consumer;
import com.indeed.iql2.execution.groupkeys.sets.RandomGroupKeySet;
import com.indeed.util.core.TreeTimer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RandomMetricRegroup implements Command {
    public final ImmutableMap<String, ImmutableList<String>> perDatasetMetric;
    private final int k;
    private final String salt;

    public RandomMetricRegroup(final Map<String, ? extends List<String>> perDatasetMetric,
                               final int k,
                               final String salt) {
        final ImmutableMap.Builder<String, ImmutableList<String>> copy = ImmutableMap.builder();
        for (final Map.Entry<String, ? extends List<String>> entry : perDatasetMetric.entrySet()) {
            copy.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
        }
        this.perDatasetMetric = copy.build();
        this.k = k;
        this.salt = salt;
    }

    @Override
    public void execute(final Session session, final Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final int numGroups = session.numGroups;
        if (numGroups != 1) {
            throw new IllegalArgumentException("Can only use RANDOM() regroup as first GROUP BY");
        }
        final double[] percentages = new double[k - 1];
        final int[] resultGroups = new int[k];
        for (int i = 0; i < (k - 1); i++) {
            final double end = ((double)(i + 1)) / k;
            percentages[i] = end;
            resultGroups[i] = i + 2;
        }
        resultGroups[k - 1] = k + 1;

        final Map<String, ? extends List<String>> perDatasetMetrics = this.perDatasetMetric;

        session.process(new SessionCallback() {
            @Override
            public void handle(final TreeTimer timer, final String name, final ImhotepSession session) throws ImhotepOutOfMemoryException {
                if (!perDatasetMetrics.containsKey(name)) {
                    return;
                }
                final List<String> pushes = new ArrayList<>(perDatasetMetrics.get(name));

                final int numStats = Session.pushStatsWithTimer(session, pushes, timer);

                if (numStats != 1) {
                    throw new IllegalStateException("Pushed more than one stat!: " + pushes);
                }

                timer.push("randomMetricMultiRegroup");
                session.randomMetricMultiRegroup(0, salt, 1, percentages, resultGroups);
                timer.pop();

                timer.push("popStat");
                session.popStat();
                timer.pop();
            }
        });

        session.assumeDense(new RandomGroupKeySet(session.groupKeySet, k + 1));

        out.accept("success");
    }
}
