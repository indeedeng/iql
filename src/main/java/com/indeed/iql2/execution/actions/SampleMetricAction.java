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

package com.indeed.iql2.execution.actions;

import com.google.common.collect.Lists;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.ImhotepSessionHolder;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.SessionCallback;
import com.indeed.util.logging.TracingTreeTimer;

import java.util.List;
import java.util.Map;

public class SampleMetricAction implements Action {
    public final Map<String, ? extends List<String>> perDatasetMetric;
    public final double probability;
    public final String seed;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public SampleMetricAction(final Map<String, ? extends List<String>> perDatasetMetric,
                              final double probability,
                              final String seed,
                              final int targetGroup,
                              final int positiveGroup,
                              final int negativeGroup) {
        this.perDatasetMetric = perDatasetMetric;
        this.probability = probability;
        this.seed = seed;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void apply(final Session session) throws ImhotepOutOfMemoryException {
        if ((targetGroup == 1) && (session.numGroups == 1) && ((positiveGroup == 1 && negativeGroup == 0) || (positiveGroup == 0 && negativeGroup == 1))) {
            // TODO: Parallelize
            session.process(new SessionCallback() {
                @Override
                public void handle(final TracingTreeTimer timer, final String name, final ImhotepSessionHolder session) throws ImhotepOutOfMemoryException {
                    if (!perDatasetMetric.containsKey(name)) {
                        return;
                    }
                    final List<String> pushes = Lists.newArrayList(perDatasetMetric.get(name));

                    final int numStats = Session.pushStatsWithTimer(session, pushes, timer);

                    if (numStats != 1) {
                        throw new IllegalStateException("Pushed more than one stat!: " + pushes);
                    }

                    timer.push("randomMetricRegroup");
                    session.randomMetricRegroup(0, seed, 1.0 - probability, targetGroup, negativeGroup, positiveGroup);
                    timer.pop();

                    timer.push("popStat");
                    session.popStat();
                    timer.pop();
                }
            });
        } else {
            throw new UnsupportedOperationException("Can only do SampleMetricAction filters when negativeGroup or positive group > 1.");
        }
    }

    @Override
    public String toString() {
        return "SampleMetricAction{" +
                "perDatasetMetric=" + perDatasetMetric +
                ", probability=" + probability +
                ", seed='" + seed + '\'' +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }
}
