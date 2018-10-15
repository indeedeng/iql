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
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.AggregateFilter;
import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.commands.misc.IterateHandler;
import com.indeed.iql2.execution.commands.misc.IterateHandlerable;
import com.indeed.iql2.execution.commands.misc.IterateHandlers;
import java.util.function.Consumer;;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class GetGroupDistincts implements IterateHandlerable<long[]>, Command {
    public final Set<String> scope;
    public final String field;
    public final Optional<AggregateFilter> filter;
    public final int windowSize;

    public GetGroupDistincts(Set<String> scope, String field, Optional<AggregateFilter> filter, int windowSize) {
        this.scope = scope;
        this.field = field;
        this.filter = filter;
        this.windowSize = windowSize;
    }

    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final long[] groupCounts = IterateHandlers.executeSingle(session, field, iterateHandler(session));
        out.accept(Session.MAPPER.writeValueAsString(groupCounts));
    }

    public IterateHandler<long[]> iterateHandler(Session session) {
        return new IterateHandlerImpl(session);
    }

    private class IterateHandlerImpl implements IterateHandler<long[]> {
        private final BitSet groupSeen = new BitSet();
        private boolean started = false;
        private int lastGroup = 0;

        private final long[] groupCounts;
        private final Session session;

        private IterateHandlerImpl(Session session) {
            this.groupCounts = new long[session.numGroups];
            this.session = session;
        }

        @Override
        public Set<String> scope() {
            return scope;
        }

        public Set<QualifiedPush> requires() {
            if (filter.isPresent()) {
                return filter.get().requires();
            } else {
                return Collections.emptySet();
            }
        }

        public void register(Map<QualifiedPush, Integer> metricIndexes, GroupKeySet groupKeySet) {
            if (filter.isPresent()) {
                filter.get().register(metricIndexes, groupKeySet);
            }
        }

        @Override
        public Session.IntIterateCallback intIterateCallback() {
            return new IntIterateCallback();
        }

        @Override
        public Session.StringIterateCallback stringIterateCallback() {
            return new StringIterateCallback();
        }

        @Override
        public long[] finish() throws ImhotepOutOfMemoryException {
            if (windowSize > 1) {
                updateAllSeenGroups();
            }
            return groupCounts;
        }

        private class IntIterateCallback implements Session.IntIterateCallback {
            private long currentTerm = 0;

            @Override
            public void term(final long term, final long[] stats, final int group) {
                if (started && currentTerm != term) {
                    updateAllSeenGroups();
                    groupSeen.clear();
                } else if (started) {
                    updateSeenGroupsUntil(group);
                }
                currentTerm = term;
                started = true;
                lastGroup = group;
                if (!filter.isPresent() || filter.get().allow(term, stats, group)) {
                    updateGroups(group, groupSeen);
                }
                if (groupSeen.get(group)) {
                    groupCounts[group - 1]++;
                }
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
        }

        private class StringIterateCallback implements Session.StringIterateCallback {
            private String currentTerm;

            @Override
            public void term(final String term, final long[] stats, final int group) {
                if (started && !currentTerm.equals(term)) {
                    updateAllSeenGroups();
                    groupSeen.clear();
                } else if (started) {
                    updateSeenGroupsUntil(group);
                }
                currentTerm = term;
                started = true;
                lastGroup = group;
                if (!filter.isPresent() || filter.get().allow(term, stats, group)) {
                    updateGroups(group, groupSeen);
                }
                if (groupSeen.get(group)) {
                    groupCounts[group - 1]++;
                }
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
        }

        private void updateAllSeenGroups() {
            while ((lastGroup = groupSeen.nextSetBit(lastGroup + 1)) != -1) {
                groupCounts[lastGroup - 1]++;
            }
        }

        private void updateSeenGroupsUntil(int group) {
            while ((lastGroup = groupSeen.nextSetBit(lastGroup + 1)) != -1 && lastGroup < group) {
                groupCounts[lastGroup - 1]++;
            }
        }

        private void updateGroups(final int group, final BitSet groupSeen) {
            if (windowSize == 1) {
                // DISTINCT(...) == DISTINCT_WINDOW(1,...)
                groupSeen.set(group);
            } else {
                // DISTINCT_WINDOW
                final int parent = session.groupKeySet.parentGroup(group);
                final int numGroups = session.groupKeySet.numGroups();
                for (int offset = 0; offset < windowSize; offset++) {
                    if (group + offset <= numGroups && session.groupKeySet.parentGroup(group + offset) == parent) {
                        groupSeen.set(group + offset);
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return "GetGroupDistincts{" +
                "scope=" + scope +
                ", field='" + field + '\'' +
                ", filter=" + filter +
                ", windowSize=" + windowSize +
                '}';
    }
}
