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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.AggregateFilter;
import com.indeed.iql2.execution.Pushable;
import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.commands.misc.IterateHandler;
import com.indeed.iql2.execution.commands.misc.IterateHandlerable;
import com.indeed.iql2.execution.commands.misc.IterateHandlers;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GetGroupDistinctsWindowed implements IterateHandlerable<long[]>, Command {
    public final FieldSet field;
    public final Optional<AggregateFilter> filter;
    public final int windowSize;

    public GetGroupDistinctsWindowed(FieldSet field, Optional<AggregateFilter> filter, int windowSize) {
        Preconditions.checkArgument(windowSize > 1, "Should not use GetGroupDistinctsWindowed with windowSize=1!");
        this.field = field;
        this.filter = filter;
        this.windowSize = windowSize;
    }

    @Override
    public void execute(final Session session) {
        // this Command needs special processing since it returns some data.
        throw new IllegalStateException("Call evaluate() method instead");
    }

    public long[] evaluate(final Session session) throws ImhotepOutOfMemoryException, IOException {
        return IterateHandlers.executeSingle(session, field, iterateHandler(session));
    }

    public IterateHandler<long[]> iterateHandler(Session session) {
        return new IterateHandlerImpl(session);
    }

    private interface GroupStatsChecker {
        boolean allow(final long[] stats, final int group);
    }

    // This could be accomplished with a circular buffer of size numStats*windowSize, saving a ton of RAM,
    // but this leads to substantial code complexity and isn't actually worth it.
    // We're likely spending megabytes on the heap on this, but gigabytes on tmpfs allocations of the FTGS stream anyway.
    // We don't need to save those megabytes.
    private class IterateHandlerImpl implements IterateHandler<long[]> {
        private final List<QualifiedPush> filterPushes;
        private final int[] statIndexes;

        private final BitSet groupsSeen;

        // g = group
        // { g0s0, g0s1, g1s0, g1s1, ... }
        private final long[] groupStatValues;

        // Used to hold `numStats` values while processing locally.
        // No invariants about state after use -- clear before use.
        private final long[] tmpRollingSumsBuffer;

        private final int numStats;
        private final int numGroups;

        private final long[] groupCounts;
        private final int[] parentGroups;

        private GroupStatsChecker groupStatsChecker;

        private IterateHandlerImpl(final Session session) {
            this.filterPushes = Lists.newArrayList(filter.transform(Pushable::requires).or(Collections.emptySet()));
            this.statIndexes = new int[filterPushes.size()];
            this.numStats = filterPushes.size();
            this.numGroups = session.numGroups;
            this.groupStatValues = new long[numStats * (numGroups + 1)];
            this.tmpRollingSumsBuffer = new long[numStats];
            this.groupsSeen = new BitSet();
            this.groupCounts = new long[numGroups + 1];
            this.parentGroups = new int[numGroups + 1];
            Arrays.setAll(parentGroups, group -> session.groupKeySet.parentGroup(group));
        }

        @Override
        public Set<QualifiedPush> requires() {
            return Sets.newHashSet(filterPushes);
        }

        @Override
        public void register(final Map<QualifiedPush, Integer> metricIndexes, final GroupKeySet groupKeySet) {
            Arrays.setAll(statIndexes, x -> metricIndexes.get(filterPushes.get(x)));
            if (filter.isPresent()) {
                final Map<QualifiedPush, Integer> localIndexes = new HashMap<>();
                for (int i = 0; i < filterPushes.size(); i++) {
                    localIndexes.put(filterPushes.get(i), i);
                }
                filter.get().register(localIndexes, groupKeySet);
            }
        }

        @Override
        public Set<String> scope() {
            return field.datasets();
        }

        private void copyStats(final int group, final long[] groupStats) {
            for (int stat = 0; stat < numStats; stat++) {
                groupStatValues[group * numStats + stat] = groupStats[statIndexes[stat]];
            }
        }

        private void newTerm() {
            int maxGroupExcl = 0;
            int group = 0;
            while (true) {
                group = groupsSeen.nextSetBit(group);
                if (group == -1) {
                    break;
                }

                final int startGroup = group;
                final int parentGroup = parentGroups[group];
                // No need to look backwards because any values that weren't in the bitset also won't have stats set.
                System.arraycopy(groupStatValues, group * numStats, tmpRollingSumsBuffer, 0, numStats);

                // Process all values at this group and moving forward until we hit the end of the parent group
                // or a place not covered by the union of the windows.
                while (true) {
                    if (groupsSeen.get(group)) {
                        maxGroupExcl = group + windowSize;
                    }
                    if (!filter.isPresent() || groupStatsChecker.allow(tmpRollingSumsBuffer, group)) {
                        groupCounts[group] += 1;
                    }

                    group += 1;

                    if ((group > numGroups) || (group >= maxGroupExcl) || (parentGroups[group] != parentGroup)) {
                        break;
                    } else {
                        final int subtractedGroup = group - windowSize;
                        // Subtract out `group - windowSize` if necessary
                        if (subtractedGroup >= startGroup) {
                            for (int stat = 0; stat < numStats; stat++) {
                                tmpRollingSumsBuffer[stat] -= groupStatValues[(subtractedGroup * numStats) + stat];
                            }
                        }

                        // Add in new `group`
                        for (int stat = 0; stat < numStats; stat++) {
                            tmpRollingSumsBuffer[stat] += groupStatValues[(group * numStats) + stat];
                        }
                    }
                }
            }
            // TODO: Is this or zeroing all the ones in `groupsSeen` better?
            Arrays.fill(groupStatValues, 0L);
            groupsSeen.clear();
        }

        @Override
        public long[] finish() {
            newTerm();
            return groupCounts;
        }

        @Override
        public Session.IntIterateCallback intIterateCallback() {
            Preconditions.checkState(groupStatsChecker == null, "groupStatsChecker must be null before this point!");
            final IterateHandlerImpl.IntIterateCallback result = new IterateHandlerImpl.IntIterateCallback();
            this.groupStatsChecker = result;
            return result;
        }

        @Override
        public Session.StringIterateCallback stringIterateCallback() {
            Preconditions.checkState(groupStatsChecker == null, "groupStatsChecker must be null before this point!");
            final IterateHandlerImpl.StringIterateCallback result = new IterateHandlerImpl.StringIterateCallback();
            this.groupStatsChecker = result;
            return result;
        }


        private class IntIterateCallback implements Session.IntIterateCallback, GroupStatsChecker {
            private long currentTerm = 0;

            @Override
            public void term(final long term, final long[] stats, final int group) {
                if (term != currentTerm) {
                    newTerm();
                }
                currentTerm = term;
                if (numStats > 0) {
                    copyStats(group, stats);
                }
                groupsSeen.set(group);
            }

            @Override
            public boolean needSorted() {
                return filter.isPresent() && filter.get().needSorted();
            }

            @Override
            public boolean needGroup() {
                return true;
            }

            @Override
            public boolean needStats() {
                return filter.isPresent() && filter.get().needStats();
            }

            @Override
            public boolean allow(final long[] stats, final int group) {
                return filter.get().allow(currentTerm, stats, group);
            }
        }

        private class StringIterateCallback implements Session.StringIterateCallback, GroupStatsChecker {
            private String currentTerm = null;

            @Override
            public void term(final String term, final long[] stats, final int group) {
                if (!term.equals(currentTerm)) {
                    newTerm();
                }
                currentTerm = term;
                if (numStats > 0) {
                    copyStats(group, stats);
                }
                groupsSeen.set(group);
            }

            @Override
            public boolean needSorted() {
                return filter.isPresent() && filter.get().needSorted();
            }

            @Override
            public boolean needGroup() {
                return true;
            }

            @Override
            public boolean needStats() {
                return filter.isPresent() && filter.get().needStats();
            }

            @Override
            public boolean allow(final long[] stats, final int group) {
                return filter.get().allow(currentTerm, stats, group);
            }
        }
    }

    @Override
    public String toString() {
        return "GetGroupDistinctsWindowed{" +
                "field=" + field +
                ", filter=" + filter +
                ", windowSize=" + windowSize +
                '}';
    }
}
