package com.indeed.squall.iql2.execution.actions;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class IntOrAction implements Action {
    public final ImmutableSet<String> scope;
    public final String field;
    public final ImmutableSet<Long> terms;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    private @Nullable Set<String> stringifiedTerms;

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
        for (final Map.Entry<String, Session.ImhotepSessionInfo> e : session.sessions.entrySet()) {
            if (scope.contains(e.getKey())) {
                final Session.ImhotepSessionInfo sessionInfo = e.getValue();
                if (!sessionInfo.intFields.contains(field)) {
                    new StringOrAction(Collections.singleton(e.getKey()), field, this.stringifiedTerms(), targetGroup, positiveGroup, negativeGroup).apply(session);
                } else {
                    session.timer.push("sort terms");
                    final long[] terms = new long[this.terms.size()];
                    int i = 0;
                    for (final long term : this.terms) {
                        terms[i] = term;
                        i++;
                    }
                    Arrays.sort(terms);
                    session.timer.pop();
                    session.intOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup, Collections.singleton(e.getKey()));
                }
            }
        }
    }

    private Set<String> stringifiedTerms() {
        if (stringifiedTerms == null) {
            stringifiedTerms = Sets.newHashSetWithExpectedSize(terms.size());
            for (final long term : terms) {
                stringifiedTerms.add(String.valueOf(term));
            }
        }
        return stringifiedTerms;
    }

    @Override
    public String toString() {
        return "IntOrAction{" +
                "scope=" + scope +
                ", field='" + field + '\'' +
                ", terms=" + renderTerms() +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }

    private String renderTerms() {
        if (terms.size() <= 10) {
            return terms.toString();
        } else {
            return "(" + terms.size() + " terms)";
        }
    }
}
