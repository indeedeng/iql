package com.indeed.jql.language.actions;

import com.google.common.collect.ImmutableSet;

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
}
