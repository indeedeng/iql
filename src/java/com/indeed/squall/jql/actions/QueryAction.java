package com.indeed.squall.jql.actions;

import com.google.common.collect.ImmutableSet;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.jql.Session;

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
        for (final Map.Entry<String, Session.ImhotepSessionInfo> entry : session.sessions.entrySet()) {
            if (scope.contains(entry.getKey())) {
                final Session.ImhotepSessionInfo v = entry.getValue();
                final Query query = perDatasetQuery.get(entry.getKey());
                v.session.regroup(new QueryRemapRule(targetGroup, query, negativeGroup, positiveGroup));
            }
        }
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
