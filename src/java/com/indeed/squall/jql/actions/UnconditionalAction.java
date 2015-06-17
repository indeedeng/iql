package com.indeed.squall.jql.actions;

import com.google.common.collect.ImmutableSet;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.jql.Session;

import java.util.Map;
import java.util.Set;

public class UnconditionalAction implements Action {
    public final ImmutableSet<String> scope;
    public final int targetGroup;
    public final int newGroup;

    public UnconditionalAction(Set<String> scope, int targetGroup, int newGroup) {
        this.scope = ImmutableSet.copyOf(scope);
        this.targetGroup = targetGroup;
        this.newGroup = newGroup;
    }

    @Override
    public void apply(Session session) throws ImhotepOutOfMemoryException {
        session.timer.push("UnconditionalAction");
        for (final Map.Entry<String, Session.ImhotepSessionInfo> entry : session.sessions.entrySet()) {
            if (scope.contains(entry.getKey())) {
                final Session.ImhotepSessionInfo v = entry.getValue();
                v.session.regroup(new QueryRemapRule(targetGroup, Query.newTermQuery(new Term("fakeField123", true, 0L, "")), newGroup, newGroup));
            }
        }
        session.timer.pop();
    }
}
