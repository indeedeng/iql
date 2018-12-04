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
        if (targetGroup == 1 && session.numGroups == 1 && ((positiveGroup == 1 && negativeGroup == 0) || (positiveGroup == 0 && negativeGroup == 1))) {
            // TODO: Parallelize
            session.process(new SessionCallback() {
                @Override
                public void handle(TracingTreeTimer timer, String name, ImhotepSessionHolder session) throws ImhotepOutOfMemoryException {
                    if (scope.contains(name)) {
                        final List<String> pushes = Lists.newArrayList(perDatasetPushes.get(name));

                        final int index = Session.pushStatsWithTimer(session, pushes, timer);

                        if (index != 1) {
                            throw new IllegalArgumentException(
                                    "Didn't end up with 1 stat after pushing in index named \"" + name + "\", pushes = [" + pushes + "]");
                        }

                        timer.push("metricFilter");
                        session.metricFilter(0, 1, 1, positiveGroup == 0);
                        timer.pop();

                        timer.push("popStat");
                        session.popStat();
                        timer.pop();
                    }
                }
            });
        } else {
            throw new UnsupportedOperationException("Can only do MetricAction filters when negativeGroup or positive group > 1. Must implement targeted metricFilter/regroup first! Probable cause: a metric inequality inside of or after an OR in the query");
        }
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
