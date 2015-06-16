package com.indeed.jql.language.actions;

import com.google.common.collect.ImmutableSet;
import com.indeed.jql.language.DocFilter;

import java.util.Set;

public class MetricAction implements Action {
    public final ImmutableSet<String> scope;
    public final DocFilter filter;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public MetricAction(Set<String> scope, DocFilter filter, int targetGroup, int positiveGroup, int negativeGroup) {
        this.scope = ImmutableSet.copyOf(scope);
        this.filter = filter;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }
}
