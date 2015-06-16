package com.indeed.jql.language.actions;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class IntOrAction implements Action {
    public final ImmutableSet<String> scope;
    public final String field;
    public final ImmutableSet<Integer> terms;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public IntOrAction(Set<String> scope, String field, Set<Integer> terms, int targetGroup, int positiveGroup, int negativeGroup) {
        this.scope = ImmutableSet.copyOf(scope);
        this.field = field;
        this.terms = ImmutableSet.copyOf(terms);
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }
}
