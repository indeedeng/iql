package com.indeed.squall.iql2.execution.actions;

import com.google.common.collect.ImmutableSet;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;

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
        session.regroup(new QueryRemapRule(targetGroup, Query.newTermQuery(new Term("fakeField123", true, 0L, "")), newGroup, newGroup), scope);
    }

    @Override
    public String toString() {
        return "UnconditionalAction{" +
                "scope=" + scope +
                ", targetGroup=" + targetGroup +
                ", newGroup=" + newGroup +
                '}';
    }
}
