package com.indeed.squall.iql2.execution.actions;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;

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
        if (targetGroup == 1 && positiveGroup == 1 && negativeGroup == 0 && session.numGroups == 1) {
            // TODO: Parallelize
            for (final Map.Entry<String, Session.ImhotepSessionInfo> entry : session.sessions.entrySet()) {
                if (scope.contains(entry.getKey())) {
                    final Session.ImhotepSessionInfo v = entry.getValue();
                    final List<String> pushes = perDatasetPushes.get(entry.getKey());

                    session.timer.push("pushStats");
                    v.session.pushStats(pushes);
                    session.timer.pop();

                    session.timer.push("metricFilter");
                    v.session.metricFilter(0, 1, 1, false);
                    session.timer.pop();

                    session.timer.push("popStat");
                    v.session.popStat();
                    session.timer.pop();
                }
            }
        } else {
            throw new UnsupportedOperationException("Can only do MetricAction filters when targetGroup=positiveGroup=1 and negativeGroup=0 and numGroups=1. Must implement targeted metricFilter/regroup first! Probable cause: a metric inequality inside of or after an OR in the query");
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
