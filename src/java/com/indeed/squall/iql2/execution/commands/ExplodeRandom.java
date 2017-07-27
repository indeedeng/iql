package com.indeed.squall.iql2.execution.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.SessionCallback;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.sets.RandomGroupKeySet;
import com.indeed.util.core.TreeTimer;

import java.io.IOException;

/**
 *
 */
public class ExplodeRandom implements Command {
    private final String field;
    private final int k;
    private final String salt;

    public ExplodeRandom(String field, int k, String salt) {
        this.field = field;
        this.k = k;
        this.salt = salt;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final boolean isIntField = session.isIntField(field);
        final int numGroups = session.numGroups;
        if (numGroups != 1) {
            throw new IllegalArgumentException("Can only use RANDOM() regroup as first GROUP BY");
        }
        final double[] percentages = new double[k - 1];
        final int[] resultGroups = new int[k];
        for (int i = 0; i < k - 1; i++) {
            final double end = ((double)(i + 1)) / k;
            percentages[i] = end;
            resultGroups[i] = i + 1;
        }
        resultGroups[k - 1] = k;
        session.process(new SessionCallback() {
            @Override
            public void handle(TreeTimer timer, String name, ImhotepSession session) throws ImhotepOutOfMemoryException {
                timer.push("randomMultiRegroup");
                session.randomMultiRegroup(field, isIntField, salt, 1, percentages, resultGroups);
                timer.pop();
            }
        });

        session.assumeDense(new RandomGroupKeySet(session.groupKeySet, k));
    }
}
