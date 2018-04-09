package com.indeed.squall.iql2.execution.actions;

import com.google.common.collect.ImmutableSet;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;

import java.util.Set;

public class SampleDocIdAction implements Action {
    public final ImmutableSet<String> scope;
    public final double probability;
    public final String seed;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public SampleDocIdAction(final Set<String> scope,
                             final double probability,
                             final String seed,
                             final int targetGroup,
                             final int positiveGroup,
                             final int negativeGroup) {
        this.scope = ImmutableSet.copyOf(scope);
        this.probability = probability;
        this.seed = seed;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void apply(final Session session) throws ImhotepOutOfMemoryException {
        session.randomDocIdRegroup(seed, 1.0 - probability, targetGroup, positiveGroup, negativeGroup, scope);
    }

    @Override
    public String toString() {
        return "SampleDocIdAction{" +
                "scope=" + scope +
                ", probability=" + probability +
                ", seed='" + seed + '\'' +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }
}
