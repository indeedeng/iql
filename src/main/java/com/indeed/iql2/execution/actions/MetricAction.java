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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.ImhotepSessionHolder;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.SessionCallback;
import com.indeed.util.logging.TracingTreeTimer;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetricAction implements Action {
    public final ImmutableSet<String> scope;
    public final Map<String, List<String>> perDatasetPushes;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public MetricAction(Set<String> scope, Map<String, List<String>> perDatasetPushes, int targetGroup, int positiveGroup, int negativeGroup) {
        this.scope = ImmutableSet.copyOf(scope);
        // TODO: Defensively copy inner lists?
        this.perDatasetPushes = ImmutableMap.copyOf(perDatasetPushes);
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void apply(Session session) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        session.process(new SessionCallback() {
            @Override
            public void handle(TracingTreeTimer timer, String name, ImhotepSessionHolder session) throws ImhotepOutOfMemoryException {
                if (scope.contains(name)) {
                    final List<String> pushes = Lists.newArrayList(perDatasetPushes.get(name));

                    Session.pushStatsWithTimer(session, pushes, timer);

                    timer.push("metricFilter");
                    session.metricFilter(0, 1, 1, targetGroup, negativeGroup, positiveGroup);
                    timer.pop();

                    timer.push("popStat");
                    session.popStat();
                    timer.pop();
                }
            }
        });
    }

    @Override
    public String toString() {
        return "MetricAction{" +
                "scope=" + scope +
                ", perDatasetPushes=" + perDatasetPushes +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }
}
