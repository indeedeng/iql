package com.indeed.squall.iql2.execution.actions;

import com.google.common.collect.ImmutableSet;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;

import java.util.Set;

public class RegexAction implements Action {
    public final ImmutableSet<String> scope;
    public final String field;
    public final String regex;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public RegexAction(Set<String> scope, String field, String regex, int targetGroup, int positiveGroup, int negativeGroup) {
        this.scope = ImmutableSet.copyOf(scope);
        this.field = field;
        this.regex = regex;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void apply(Session session) throws ImhotepOutOfMemoryException {
        session.regexRegroup(field, regex, targetGroup, negativeGroup, positiveGroup, scope);
    }

    @Override
    public String toString() {
        return "RegexAction{" +
                "scope=" + scope +
                ", field='" + field + '\'' +
                ", regex='" + regex + '\'' +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }
}
