package com.indeed.jql.language.actions;

import java.util.Set;

public class UnconditionalAction {
    public final Set<String> scope;
    public final int targetGroup;
    public final int newGroup;

    public UnconditionalAction(Set<String> scope, int targetGroup, int newGroup) {
        this.scope = scope;
        this.targetGroup = targetGroup;
        this.newGroup = newGroup;
    }
}
