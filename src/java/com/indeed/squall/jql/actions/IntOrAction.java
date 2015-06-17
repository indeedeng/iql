package com.indeed.squall.jql.actions;

import com.google.common.collect.ImmutableSet;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.jql.Session;

import java.util.Map;
import java.util.Set;

public class IntOrAction implements Action {
    public final ImmutableSet<String> scope;
    public final String field;
    public final ImmutableSet<Long> terms;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public IntOrAction(Set<String> scope, String field, Set<Long> terms, int targetGroup, int positiveGroup, int negativeGroup) {
        this.scope = ImmutableSet.copyOf(scope);
        this.field = field;
        this.terms = ImmutableSet.copyOf(terms);
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void apply(Session session) throws ImhotepOutOfMemoryException {
        final long[] terms = new long[this.terms.size()];
        int i = 0;
        for (final long term : this.terms) {
            terms[i] = term;
            i++;
        }
        for (final Map.Entry<String, Session.ImhotepSessionInfo> entry : session.sessions.entrySet()) {
            if (scope.contains(entry.getKey())) {
                final ImhotepSession s = entry.getValue().session;
                s.intOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
            }
        }
    }

    @Override
    public String toString() {
        return "IntOrAction{" +
                "scope=" + scope +
                ", field='" + field + '\'' +
                ", terms=" + terms +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }
}
