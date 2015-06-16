package com.indeed.jql.language.actions;

import com.indeed.flamdex.query.Query;

import java.util.Set;

public class QueryAction implements Action {
    public final Set<String> scope;
    public final Query query;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public QueryAction(Set<String> scope, Query query, int targetGroup, int positiveGroup, int negativeGroup) {
        this.scope = scope;
        this.query = query;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }
}
