package com.indeed.jql.language.actions;

import java.util.Set;

public class SampleAction implements Action {
    public final Set<String> scope;
    public final String field;
    public final double probability;
    public final String seed;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public SampleAction(Set<String> scope, String field, double probability, String seed, int targetGroup, int positiveGroup, int negativeGroup) {
        this.scope = scope;
        this.field = field;
        this.probability = probability;
        this.seed = seed;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }
}
