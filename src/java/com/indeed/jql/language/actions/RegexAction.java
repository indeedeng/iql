package com.indeed.jql.language.actions;

import java.util.Set;

public class RegexAction {
    public final Set<String> scope;
    public final String field;
    public final String regex;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public RegexAction(Set<String> scope, String field, String regex, int targetGroup, int positiveGroup, int negativeGroup) {
        this.scope = scope;
        this.field = field;
        this.regex = regex;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }
}
