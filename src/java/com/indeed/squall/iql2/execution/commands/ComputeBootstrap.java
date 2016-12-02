package com.indeed.squall.iql2.execution.commands;

import com.google.common.base.Optional;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.commands.misc.IterateHandler;
import com.indeed.squall.iql2.execution.commands.misc.IterateHandlerable;
import com.indeed.squall.iql2.execution.commands.misc.IterateHandlers;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 *
 */
public class ComputeBootstrap implements Command, IterateHandlerable<Void> {
    public static final int MAX_ENTRIES = 500_000_000 / 8;
    public final Set<String> scope;
    public final String field;
    public final String seed;
    public final AggregateMetric metric;
    public final int numBootstraps;
    public final List<String> varargs;

    public ComputeBootstrap(Set<String> scope, String field, String seed, AggregateMetric metric, int numBootstraps, List<String> varargs) {
        this.scope = scope;
        this.field = field;
        this.seed = seed;
        this.metric = metric;
        this.numBootstraps = numBootstraps;
        this.varargs = varargs;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        IterateHandlers.executeSingle(session, field, new IterateHandlerImpl(session));
    }

    @Override
    public IterateHandler<Void> iterateHandler(Session session) throws ImhotepOutOfMemoryException, IOException {
        return new IterateHandlerImpl(session);
    }

    private class IterateHandlerImpl implements IterateHandler<Void> {
        private final List<List<LongArrayList>> groupToStatToValues = new ArrayList<>();
        private final int[] groupTermCounts;
        private final List<QualifiedPush> requires;
        private final int[] statIndexes;
        private final Session session;
        private long entriesSeen = 0;

        public IterateHandlerImpl(Session session) {
            this.session = session;
            this.requires = new ArrayList<>(metric.requires());
            for (int i = 0; i <= session.numGroups; i++) {
                final ArrayList<LongArrayList> statsStorage = new ArrayList<>(this.requires.size());
                groupToStatToValues.add(statsStorage);
                for (int j = 0; j < this.requires.size(); j++) {
                    statsStorage.add(new LongArrayList());
                }
            }
            this.statIndexes = new int[this.requires.size()];
            this.groupTermCounts = new int[session.numGroups + 1];
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Sets.newHashSet(requires);
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, GroupKeySet groupKeySet) {
            metric.register(metricIndexes, groupKeySet);
            for (int i = 0; i < requires.size(); i++) {
                this.statIndexes[i] = metricIndexes.get(requires.get(i));
            }
        }

        @Override
        public Set<String> scope() {
            return scope;
        }

        @Override
        public Session.IntIterateCallback intIterateCallback() {
            return new IterateCallback();
        }

        @Override
        public Session.StringIterateCallback stringIterateCallback() {
            return new IterateCallback();
        }

        @Override
        public Void finish() throws ImhotepOutOfMemoryException, IOException {
            final int maxStatIndex = Ints.max(statIndexes);
            // TODO: Not hash the seed...
            final Random rng = new Random(seed.hashCode());
            final double[][] groupResults = new double[groupTermCounts.length][];
            for (int group = 1; group < groupTermCounts.length; group++) {
                final int numTerms = groupTermCounts[group];
                final List<LongArrayList> statToValues = groupToStatToValues.get(group);
                final double[] bootstrapResults = new double[numBootstraps];
                for (int i = 0; i < numBootstraps; i++) {
                    final long[] sampledStatSums = new long[maxStatIndex + 1];
                    // Choose a term N times
                    for (int j = 0; j < numTerms; j++) {
                        final int termIndex = rng.nextInt(numTerms);
                        // Add all stats for the chosen term
                        for (int statNum = 0; statNum < statIndexes.length; statNum++) {
                            sampledStatSums[statIndexes[statNum]] += statToValues.get(statNum).get(termIndex);
                        }
                    }
                    // Compute aggregate value
                    bootstrapResults[i] = metric.apply(-1, sampledStatSums, group);
                }
                Arrays.sort(bootstrapResults);
                groupResults[group] = bootstrapResults;
            }

            final double[] means = new double[session.numGroups];
            final double[] variances = new double[session.numGroups];
            for (int group = 0; group < session.numGroups; group++) {
                final double[] samples = groupResults[group + 1];
                double sum = 0;
                for (final double sample : samples) {
                    sum += sample;
                }
                final double mean = sum / samples.length;
                means[group] = mean;
                double varianceSum = 0;
                for (final double sample : samples) {
                    varianceSum += (sample - mean) * (sample - mean);
                }
                final double variance = varianceSum / samples.length;
                variances[group] = variance;
            }

            for (final String vararg : varargs) {
                switch (vararg) {
                    case "\"min\"": {
                        final double[] storage = new double[session.numGroups + 1];
                        for (int group = 1; group <= session.numGroups; group++) {
                            storage[group] = groupResults[group][0];
                        }
                        new CreateGroupStatsLookup(storage, Optional.of(seed + "[" + numBootstraps + "].\"min\"")).execute(session, new Consumer.NoOpConsumer<String>());
                        break;
                    }
                    case "\"max\"": {
                        final double[] storage = new double[session.numGroups + 1];
                        for (int group = 1; group <= session.numGroups; group++) {
                            storage[group] = groupResults[group][numBootstraps - 1];
                        }
                        new CreateGroupStatsLookup(storage, Optional.of(seed + "[" + numBootstraps + "].\"max\"")).execute(session, new Consumer.NoOpConsumer<String>());
                        break;
                    }
                    case "\"all\"": {
                        for (int b = 0; b < numBootstraps; b++) {
                            final double[] groupValues = new double[session.numGroups + 1];
                            for (int group = 1; group <= session.numGroups; group++) {
                                groupValues[group] = groupResults[group][b];
                            }
                            new CreateGroupStatsLookup(groupValues, Optional.of(seed + "[" + numBootstraps + "].values[" + b + "]")).execute(session, new Consumer.NoOpConsumer<String>());
                        }
                        break;
                    }
                    case "\"numTerms\"": {
                        final double[] storage = new double[session.numGroups + 1];
                        for (int group = 1; group <= session.numGroups; group++) {
                            storage[group] = groupTermCounts[group];
                        }
                        new CreateGroupStatsLookup(storage, Optional.of(seed + "[" + numBootstraps + "].\"numTerms\"")).execute(session, new Consumer.NoOpConsumer<String>());
                        break;
                    }
                    default: {
                        throw new IllegalArgumentException("don't do that yet.");
                    }
                }
            }

            return null;
        }

        private class IterateCallback implements Session.IntIterateCallback, Session.StringIterateCallback {
            @Override
            public void term(long term, long[] stats, int group) {
                term(stats, group);
            }

            @Override
            public void term(String term, long[] stats, int group) {
                term(stats, group);
            }

            private void term(long[] stats, int group) {
                entriesSeen += 1;
                groupTermCounts[group] += 1;
                final List<LongArrayList> statToValues = groupToStatToValues.get(group);
                for (int i = 0; i < statIndexes.length; i++) {
                    final long stat = stats[statIndexes[i]];
                    statToValues.get(i).add(stat);
                }
            }
        }
    }
}
