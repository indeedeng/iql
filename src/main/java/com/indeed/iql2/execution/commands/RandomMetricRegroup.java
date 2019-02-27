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
import com.indeed.iql2.execution.ImhotepSessionHolder;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.SessionCallback;
import com.indeed.iql2.execution.groupkeys.sets.RandomGroupKeySet;
import com.indeed.util.logging.TracingTreeTimer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RandomMetricRegroup implements Command {
    private final ImmutableMap<String, ImmutableList<String>> perDatasetMetric;
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
    public void execute(final Session session) throws ImhotepOutOfMemoryException {
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
            public void handle(final TracingTreeTimer timer, final String name, final ImhotepSessionHolder session) throws ImhotepOutOfMemoryException {
                if (!perDatasetMetrics.containsKey(name)) {
                    return;
                }

                timer.push("randomMetricMultiRegroup");
                session.randomMetricMultiRegroup(perDatasetMetrics.get(name), salt, 1, percentages, resultGroups);
                timer.pop();
            }
        });

        session.assumeDense(new RandomGroupKeySet(session.groupKeySet, k + 1, session.formatter));
    }
}
