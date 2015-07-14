package com.indeed.squall.jql.commands;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.jql.AggregateFilter;
import com.indeed.squall.jql.GroupLookupMergeType;
import com.indeed.squall.jql.Session;
import com.indeed.squall.jql.compat.Consumer;
import com.indeed.squall.jql.metrics.aggregate.AggregateMetric;
import com.indeed.squall.jql.metrics.aggregate.Constant;
import com.indeed.squall.jql.metrics.aggregate.IfThenElse;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Arrays;
import java.util.List;

public class RegroupIntoLastSiblingWhere implements Command {
    private final AggregateFilter filter;
    private final GroupLookupMergeType mergeType;

    public RegroupIntoLastSiblingWhere(AggregateFilter filter, GroupLookupMergeType mergeType) {
        this.filter = filter;
        this.mergeType = mergeType;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, JsonProcessingException {
        // TODO: This could be made way more efficient, but I think this should way.
        final GetGroupStats getGroupStats = new GetGroupStats(Arrays.<AggregateMetric>asList(new IfThenElse(filter, new Constant(1), new Constant(0))), false);
        final List<Session.GroupStats> theStats = getGroupStats.evaluate(session);
        final boolean[] remerge = new boolean[session.numGroups + 1];
        for (int i = 0; i < session.numGroups; i++) {
            remerge[i + 1] = theStats.get(i).stats[0] > 0.5;
        }

        final List<Session.GroupKey> groupKeys = session.groupKeys;

        final Int2IntOpenHashMap parentIndexToLastChildIndex = new Int2IntOpenHashMap();
        for (final Session.GroupKey groupKey : groupKeys) {
            if (groupKey != null) {
                parentIndexToLastChildIndex.put(groupKey.parent.index, groupKey.index);
            }
        }

        // Cascade until the end of that parent.
        for (int i = 1; i <= session.numGroups; i++) {
            if (remerge[i]) {
                final Session.GroupKey groupKey = groupKeys.get(i);
                final int end = parentIndexToLastChildIndex.get(groupKey.parent.index);
                for (int j = i; j < end; j++) {
                    remerge[j] = true;
                }
            }
        }

        for (final int lastChildIndex : parentIndexToLastChildIndex.values()) {
            remerge[lastChildIndex] = false;
        }

        boolean anyStatsAtDepth = false;
        for (final Session.SavedGroupStats s : session.savedGroupStats.values()) {
            if (s.depth == session.currentDepth) {
                anyStatsAtDepth = true;
                break;
            }
        }

        final RegroupCondition theCondition = new RegroupCondition("foo", true, 0, null, false);
        final GroupRemapRule[] rules = new GroupRemapRule[session.numGroups];
        int numRemerged = 0;
        for (int i = 1; i <= session.numGroups; i++) {
            final int newGroup;
            if (remerge[i]) {
                newGroup = parentIndexToLastChildIndex.get(groupKeys.get(i).parent.index);
                if (anyStatsAtDepth) {
                    switch (mergeType) {
                        case SumAll: {
                            throw new UnsupportedOperationException("SumAll not supported in RegroupIntoLastSiblingWhere yet");
                        }
                        case TakeTheOneUniqueValue: {
                            throw new UnsupportedOperationException("TakeTheOneUniqueValue not supported in RegroupIntoLastSiblingWhere yet");
                        }
                        case FailIfPresent: {
                            throw new IllegalStateException("Reached a merge when FailIfPresent");
                        }
                    }
                }
                numRemerged += 1;
            } else {
                newGroup = i;
            }
            rules[i - 1] = new GroupRemapRule(i, theCondition, newGroup, newGroup);
        }
        if (numRemerged > 0) {
            System.out.println("numRemerged = " + numRemerged);
            for (final Session.ImhotepSessionInfo sessionInfo : session.sessions.values()) {
                sessionInfo.session.regroup(rules);
            }
        }

        // TODO: Use a bitset?
        // TODO: Don't copy subrange
        final boolean[] merged = Arrays.copyOfRange(remerge, 1, remerge.length);
        out.accept(Session.MAPPER.writeValueAsString(merged));
    }
}
