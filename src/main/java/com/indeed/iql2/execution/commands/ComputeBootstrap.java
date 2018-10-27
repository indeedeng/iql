/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.iql2.execution.commands;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.AggregateFilter;
import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.commands.misc.IterateHandler;
import com.indeed.iql2.execution.commands.misc.IterateHandlerable;
import com.indeed.iql2.execution.commands.misc.IterateHandlers;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.AggregateMetric;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.commons.math3.special.Erf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

;

/**
 *
 */
public class ComputeBootstrap implements Command, IterateHandlerable<Void> {
    public static final int MAX_MEMORY_BYTES = 500_000_000;
    public final Set<String> scope;
    public final String field;
    public final Optional<AggregateFilter> filter;
    public final String seed;
    public final AggregateMetric metric;
    public final int numBootstraps;
    public final List<String> varargs;

    public ComputeBootstrap(Set<String> scope, String field, Optional<AggregateFilter> filter, String seed, AggregateMetric metric, int numBootstraps, List<String> varargs) {
        this.scope = scope;
        this.field = field;
        this.filter = filter;
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
        private final int[] groupSkippedTermCounts;
        private final List<QualifiedPush> metricRequires;
        private final int[] statIndexes;
        private final Session session;
        private final long maxEntries;
        private long entriesSeen = 0;

        public IterateHandlerImpl(Session session) {
            this.session = session;
            this.metricRequires = new ArrayList<>(metric.requires());
            for (int i = 0; i <= session.numGroups; i++) {
                final ArrayList<LongArrayList> statsStorage = new ArrayList<>(this.metricRequires.size());
                groupToStatToValues.add(statsStorage);
                for (int j = 0; j < this.metricRequires.size(); j++) {
                    statsStorage.add(new LongArrayList());
                }
            }
            this.statIndexes = new int[this.metricRequires.size()];
            this.groupTermCounts = new int[session.numGroups + 1];
            this.groupSkippedTermCounts = new int[session.numGroups + 1];
            this.maxEntries = MAX_MEMORY_BYTES / (this.metricRequires.size() * 8);
        }

        @Override
        public Set<QualifiedPush> requires() {
            final Set<QualifiedPush> requires = Sets.newHashSet(metricRequires);
            if (filter.isPresent()) {
                requires.addAll(filter.get().requires());
            }
            return requires;
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, GroupKeySet groupKeySet) {
            metric.register(metricIndexes, groupKeySet);
            for (int i = 0; i < metricRequires.size(); i++) {
                this.statIndexes[i] = metricIndexes.get(metricRequires.get(i));
            }
            if (filter.isPresent()) {
                filter.get().register(metricIndexes, groupKeySet);
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
            // TODO: Multi-thread?
            session.timer.push("sample metrics");
            final int maxStatIndex = Ints.max(statIndexes);
            // TODO: Not hash the seed...
            final double[][] groupResults = new double[groupTermCounts.length][];

            for (int group = 1; group < groupTermCounts.length; group++) {
                final Random rng = new Random(seed.hashCode() + group);
                final int numTerms = groupTermCounts[group];
                final List<LongArrayList> statToValues = groupToStatToValues.get(group);
                final double[] bootstrapResults = new double[numBootstraps];
                groupResults[group] = bootstrapResults;
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
            }
            session.timer.pop();

            session.timer.push("compute mean and variance");
            final double[] means = new double[session.numGroups + 1];
            final double[] variances = new double[session.numGroups + 1];
            for (int group = 1; group <= session.numGroups; group++) {
                final double[] samples = groupResults[group];
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
            session.timer.pop();

            session.timer.push("create metrics");
            for (final String vararg : varargs) {
                switch (vararg) {
                    case "\"min\"": {
                        final double[] storage = new double[session.numGroups + 1];
                        for (int group = 1; group <= session.numGroups; group++) {
                            storage[group] = groupResults[group][0];
                        }
                        new CreateGroupStatsLookup(storage, Optional.of(seed + "[" + numBootstraps + "].\"min\"")).execute(session);
                        break;
                    }
                    case "\"max\"": {
                        final double[] storage = new double[session.numGroups + 1];
                        for (int group = 1; group <= session.numGroups; group++) {
                            storage[group] = groupResults[group][numBootstraps - 1];
                        }
                        new CreateGroupStatsLookup(storage, Optional.of(seed + "[" + numBootstraps + "].\"max\"")).execute(session);
                        break;
                    }
                    case "\"all\"": {
                        for (int b = 0; b < numBootstraps; b++) {
                            final double[] groupValues = new double[session.numGroups + 1];
                            for (int group = 1; group <= session.numGroups; group++) {
                                groupValues[group] = groupResults[group][b];
                            }
                            new CreateGroupStatsLookup(groupValues, Optional.of(seed + "[" + numBootstraps + "].values[" + b + "]")).execute(session);
                        }
                        break;
                    }
                    case "\"numTerms\"": {
                        final double[] storage = new double[session.numGroups + 1];
                        for (int group = 1; group <= session.numGroups; group++) {
                            storage[group] = groupTermCounts[group];
                        }
                        new CreateGroupStatsLookup(storage, Optional.of(seed + "[" + numBootstraps + "].\"numTerms\"")).execute(session);
                        break;
                    }
                    case "\"skippedTerms\"": {
                        final double[] storage = new double[session.numGroups + 1];
                        for (int group = 1; group <= session.numGroups; group++) {
                            storage[group] = groupSkippedTermCounts[group];
                        }
                        new CreateGroupStatsLookup(storage, Optional.of(seed + "[" + numBootstraps + "].\"skippedTerms\"")).execute(session);
                        break;
                    }
                    case "\"mean\"": {
                        new CreateGroupStatsLookup(means, Optional.of(seed + "[" + numBootstraps + "].\"mean\"")).execute(session);
                        break;
                    }
                    case "\"variance\"": {
                        new CreateGroupStatsLookup(variances, Optional.of(seed + "[" + numBootstraps + "].\"variance\"")).execute(session);
                        break;
                    }
                    default: {
                        try {
                            final double percentile = Double.parseDouble(vararg);
                            System.out.println("percentile = " + percentile);
                            final double z = -Math.sqrt(2) * Erf.erfcInv(2 * percentile);
                            System.out.println("z = " + z);
                            final double[] storage = new double[session.numGroups + 1];
                            for (int group = 1; group <= session.numGroups; group++) {
                                storage[group] = means[group] + z * variances[group];
                            }
                            new CreateGroupStatsLookup(storage, Optional.of(seed + "[" + numBootstraps + "]." + vararg)).execute(session);
                            break;
                        } catch (final NumberFormatException e) {
                        }
                        throw new IllegalArgumentException("Unsupported argument: [" + vararg + "]");
                    }
                }
            }
            session.timer.pop();

            return null;
        }

        private class IterateCallback implements Session.IntIterateCallback, Session.StringIterateCallback {
            @Override
            public void term(final long term, final long[] stats, final int group) {
                if (!filter.isPresent() || filter.get().allow(term, stats, group)) {
                    term(stats, group);
                } else {
                    groupSkippedTermCounts[group] += 1;
                }
            }

            @Override
            public void term(final String term, final long[] stats, final int group) {
                if (!filter.isPresent() || filter.get().allow(term, stats, group)) {
                    term(stats, group);
                } else {
                    groupSkippedTermCounts[group] += 1;
                }
            }

            @Override
            public boolean needSorted() {
                // We need sorted since we use Random inside and result depends on terms order.
                return true;
            }

            @Override
            public boolean needGroup() {
                return true;
            }

            @Override
            public boolean needStats() {
                return (statIndexes.length > 0)
                        || (filter.isPresent() && filter.get().needStats());
            }

            private void term(final long[] stats, final int group) {
                entriesSeen += 1;
                if (entriesSeen >= maxEntries) {
                    throw new IllegalStateException("Too many entries in BOOTSTRAP() execution. maxEntries = " + maxEntries);
                }
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
