package com.indeed.squall.iql2.execution.actions;

import com.google.common.collect.ImmutableSet;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.Session;

import java.util.Arrays;
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
        session.timer.push("sort terms");
        final long[] terms = new long[this.terms.size()];
        int i = 0;
        for (final long term : this.terms) {
            terms[i] = term;
            i++;
        }
        Arrays.sort(terms);
        session.timer.pop();
        session.intOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup, scope);
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
