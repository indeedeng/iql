package com.indeed.squall.iql2.execution.actions;

import com.google.common.collect.ImmutableSet;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;

import java.util.Set;

public class SampleAction implements Action {
    public final ImmutableSet<String> scope;
    public final String field;
    public final double probability;
    public final String seed;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public SampleAction(Set<String> scope, String field, double probability, String seed, int targetGroup, int positiveGroup, int negativeGroup) {
        this.scope = ImmutableSet.copyOf(scope);
        this.field = field;
        this.probability = probability;
        this.seed = seed;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void apply(Session session) throws ImhotepOutOfMemoryException {
        final boolean isIntField;
        if (session.isIntField(field)) {
            isIntField = true;
        } else if (session.isStringField(field)) {
            isIntField = false;
        } else {
            throw new IllegalArgumentException("field is not valid: " + field);
        }
        session.randomRegroup(field, isIntField, seed, probability, targetGroup, positiveGroup, negativeGroup, scope);
    }

    @Override
    public String toString() {
        return "SampleAction{" +
                "scope=" + scope +
                ", field='" + field + '\'' +
                ", probability=" + probability +
                ", seed='" + seed + '\'' +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }
}
