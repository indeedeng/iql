package com.indeed.squall.iql2.execution.actions;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.SessionCallback;
import com.indeed.util.core.TreeTimer;

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
            session.process(new SessionCallback() {
                @Override
                public void handle(TreeTimer timer, String name, ImhotepSession session) throws ImhotepOutOfMemoryException {
                    if (scope.contains(name)) {
                        final List<String> pushes = perDatasetPushes.get(name);

                        timer.push("pushStats");
                        session.pushStats(pushes);
                        timer.pop();

                        timer.push("metricFilter");
                        session.metricFilter(0, 1, 1, false);
                        timer.pop();

                        timer.push("popStat");
                        session.popStat();
                        timer.pop();
                    }
                }
            });
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
