package com.indeed.squall.iql2.execution.commands;

import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.commands.misc.IterateHandler;
import com.indeed.squall.iql2.execution.commands.misc.IterateHandlerable;
import com.indeed.squall.iql2.execution.commands.misc.IterateHandlers;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class GetGroupPercentiles implements IterateHandlerable<long[][]>, Command {
    public final Set<String> scope;
    public final String field;
    public final double[] percentiles;

    public GetGroupPercentiles(Set<String> scope, String field, double[] percentiles) {
        this.scope = scope;
        this.field = field;
        this.percentiles = percentiles;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final long[][] results = IterateHandlers.executeSingle(session, field, iterateHandler(session));
        out.accept(Session.MAPPER.writeValueAsString(results));
    }

    @Override
    public IterateHandler<long[][]> iterateHandler(Session session) throws ImhotepOutOfMemoryException {
        session.timer.push("compute counts");
        final double[] percentiles = this.percentiles;
        final long[] counts = new long[session.numGroups + 1];
        for (final String sessionName : scope) {
            final ImhotepSession s = session.sessions.get(sessionName).session;
            s.pushStat("hasintfield " + field);
            final long[] stats = s.getGroupStats(0);
            for (int i = 0; i < stats.length; i++) {
                counts[i] += stats[i];
            }
            s.popStat();
        }
        session.timer.pop();

        session.timer.push("compute boundaries");
        final double[][] requiredCounts = new double[counts.length][];
        for (int i = 1; i < counts.length; i++) {
            requiredCounts[i] = new double[percentiles.length];
            for (int j = 0; j < percentiles.length; j++) {
                requiredCounts[i][j] = (percentiles[j] / 100.0) * (double)counts[i];
            }
        }
        session.timer.pop();
        return new IterateHandlerImpl(session.numGroups, requiredCounts);
    }

    private class IterateHandlerImpl implements IterateHandler<long[][]> {
        private final IntSet relevantIndexes = new IntArraySet();
        private final long[][] results;
        private final long[] runningCounts;
        private final double[][] requiredCounts;

        public IterateHandlerImpl(int numGroups, double[][] requiredCounts) {
            this.requiredCounts = requiredCounts;
            this.results = new long[percentiles.length][numGroups];
            this.runningCounts = new long[numGroups + 1];
        }

        @Override
        public Set<String> scope() {
            return scope;
        }

        @Override
        public Set<QualifiedPush> requires() {
            final Set<QualifiedPush> pushes = Sets.newHashSetWithExpectedSize(scope.size());
            for (final String name : scope) {
                pushes.add(new QualifiedPush(name, Collections.singletonList("count()")));
            }
            return pushes;
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, GroupKeySet groupKeySet) {
            for (final String name : scope) {
                relevantIndexes.add(metricIndexes.get(new QualifiedPush(name, Collections.singletonList("count()"))));
            }
        }

        @Override
        public Session.IntIterateCallback intIterateCallback() {
            return new IntIterateCallback();
        }

        @Override
        public Session.StringIterateCallback stringIterateCallback() {
            throw new IllegalArgumentException("Cannot do GetGroupPercentiles over a string field");
        }

        @Override
        public long[][] finish() throws ImhotepOutOfMemoryException {
            return results;
        }

        private class IntIterateCallback implements Session.IntIterateCallback {
            @Override
            public void term(long term, long[] stats, int group) {
                final long oldCount = runningCounts[group];
                long termCount = 0L;
                for (final int index : relevantIndexes) {
                    termCount += stats[index];
                }
                final long newCount = oldCount + termCount;

                final double[] groupRequiredCountsArray = requiredCounts[group];
                for (int i = 0; i < percentiles.length; i++) {
                    final double minRequired = groupRequiredCountsArray[i];
                    if (newCount >= minRequired && oldCount < minRequired) {
                        results[i][group - 1] = term;
                    }
                }

                runningCounts[group] = newCount;
            }
        }
    }
}
