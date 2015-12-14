package com.indeed.squall.iql2.execution.commands;

import com.google.common.base.Optional;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.AggregateFilter;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.commands.misc.IterateHandler;
import com.indeed.squall.iql2.execution.commands.misc.IterateHandlerable;
import com.indeed.squall.iql2.execution.commands.misc.IterateHandlers;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;

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
            return groupCounts;
        }

        private class IntIterateCallback implements Session.IntIterateCallback {
            private final BitSet groupSeen = new BitSet();
            private boolean started = false;
            private int lastGroup = 0;
            private long currentTerm = 0;

            @Override
            public void term(long term, long[] stats, int group) {
                if (started && currentTerm != term) {
                    while ((lastGroup = groupSeen.nextSetBit(lastGroup + 1)) != -1) {
                        groupCounts[lastGroup - 1]++;
                    }
                    groupSeen.clear();
                } else if (started) {
                    while ((lastGroup = groupSeen.nextSetBit(lastGroup + 1)) != -1 && lastGroup < group) {
                        groupCounts[lastGroup - 1]++;
                    }
                }
                currentTerm = term;
                started = true;
                lastGroup = group;
                final int parent = session.groupKeySet.parentGroup(group);
                if (!filter.isPresent() || filter.get().allow(term, stats, group)) {
                    for (int offset = 0; offset < windowSize; offset++) {
                        if (group + offset < session.groupKeySet.numGroups() && session.groupKeySet.parentGroup(group + offset) == parent) {
                            groupSeen.set(group + offset);
                        }
                    }
                }
                if (groupSeen.get(group)) {
                    groupCounts[group - 1]++;
                }
            }
        }

        private class StringIterateCallback implements Session.StringIterateCallback {
            private final BitSet groupSeen = new BitSet();
            private boolean started = false;
            private int lastGroup = 0;
            private String currentTerm;

            @Override
            public void term(String term, long[] stats, int group) {
                if (started && !currentTerm.equals(term)) {
                    while ((lastGroup = groupSeen.nextSetBit(lastGroup + 1)) != -1) {
                        groupCounts[lastGroup - 1]++;
                    }
                    groupSeen.clear();
                } else if (started) {
                    while ((lastGroup = groupSeen.nextSetBit(lastGroup + 1)) != -1 && lastGroup < group) {
                        groupCounts[lastGroup - 1]++;
                    }
                }
                currentTerm = term;
                started = true;
                lastGroup = group;
                final int parent = session.groupKeySet.parentGroup(group);
                if (!filter.isPresent() || filter.get().allow(term, stats, group)) {
                    for (int offset = 0; offset < windowSize; offset++) {
                        if (group + offset < session.groupKeySet.numGroups() && session.groupKeySet.parentGroup(group + offset) == parent) {
                            groupSeen.set(group + offset);
                        }
                    }
                }
                if (groupSeen.get(group)) {
                    groupCounts[group - 1]++;
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
