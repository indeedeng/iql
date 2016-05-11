package com.indeed.squall.iql2.execution.actions;

import com.google.common.collect.ImmutableSet;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.SessionCallback;
import com.indeed.util.core.TreeTimer;

import java.util.Map;
import java.util.Set;

public class QueryAction implements Action {
    public final ImmutableSet<String> scope;
    public final Map<String, Query> perDatasetQuery;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public QueryAction(Set<String> scope, Map<String, Query> perDatasetQuery, int targetGroup, int positiveGroup, int negativeGroup) {
        this.scope = ImmutableSet.copyOf(scope);
        this.perDatasetQuery = perDatasetQuery;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void apply(Session session) throws ImhotepOutOfMemoryException {
        session.process(new SessionCallback() {
            @Override
            public void handle(TreeTimer timer, String name, ImhotepSession session) throws ImhotepOutOfMemoryException {
                if (!scope.contains(name)) {
                    return;
                }

                timer.push("regroup");
                final Query query = perDatasetQuery.get(name);
                session.regroup(new QueryRemapRule(targetGroup, query, negativeGroup, positiveGroup));
                timer.pop();
            }
        });
    }

    @Override
    public String toString() {
        return "QueryAction{" +
                "scope=" + scope +
                ", perDatasetQuery=" + perDatasetQuery +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }
}
