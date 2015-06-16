package com.indeed.jql.language.actions;

import com.indeed.jql.language.DocFilter;

import java.util.Set;

public class MetricAction {
    public final Set<String> scope;
    public final DocFilter filter;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public MetricAction(Set<String> scope, DocFilter filter, int targetGroup, int positiveGroup, int negativeGroup) {
        this.scope = scope;
        this.filter = filter;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }
}
