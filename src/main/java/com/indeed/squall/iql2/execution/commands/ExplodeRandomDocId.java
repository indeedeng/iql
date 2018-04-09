package com.indeed.squall.iql2.execution.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.sets.RandomGroupKeySet;

import java.io.IOException;

public class ExplodeRandomDocId implements Command {
    private final int k;
    private final String salt;

    public ExplodeRandomDocId(final int k, final String salt) {
        this.k = k;
        this.salt = salt;
    }

    @Override
    public void execute(final Session session, final Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final int numGroups = session.numGroups;
        if (numGroups != 1) {
            throw new IllegalArgumentException("Can only use RANDOM() regroup as first GROUP BY");
        }
        final double[] percentages = new double[k - 1];
        final int[] resultGroups = new int[k];
        for (int i = 0; i < k - 1; i++) {
            final double end = ((double)(i + 1)) / k;
            percentages[i] = end;
            resultGroups[i] = i + 2;
        }
        resultGroups[k - 1] = k + 1;
        session.randomDocIdMultiRegroup(salt, 1, percentages, resultGroups);
        session.assumeDense(new RandomGroupKeySet(session.groupKeySet, k + 1));
    }
}
