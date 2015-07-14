package com.indeed.squall.iql2.execution.commands;

import com.google.common.collect.Maps;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.GroupLookupMergeType;
import com.indeed.squall.iql2.execution.Session;

import java.util.Arrays;
import java.util.Map;

public class RegroupIntoParent implements Command {
    private final GroupLookupMergeType mergeType;

    public RegroupIntoParent(GroupLookupMergeType mergeType) {
        this.mergeType = mergeType;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException {
        int maxIndex = 0;
        for (int i = 1; i < session.groupKeys.size(); i++) {
            maxIndex = Math.max(maxIndex, session.groupKeys.get(i).parent.index);
        }
        final Map<String, Session.SavedGroupStats> newSavedGroupStatsEntries = Maps.newHashMap();
        for (final Map.Entry<String, Session.SavedGroupStats> entry : session.savedGroupStats.entrySet()) {
            final String k = entry.getKey();
            final Session.SavedGroupStats v = entry.getValue();
            if (v.depth == session.currentDepth) {
                final double[] mergedStats = new double[maxIndex + 1];
                final double[] oldStats = v.stats;
                final boolean[] anyFound = new boolean[maxIndex + 1];
                for (int i = 1; i < oldStats.length; i++) {
                    final Session.GroupKey groupKey = session.groupKeys.get(i);
                    final int index = groupKey.parent.index;
                    switch (mergeType) {
                        case SumAll: {
                            mergedStats[index] += oldStats[i];
                            break;
                        }
                        case TakeTheOneUniqueValue: {
                            if (anyFound[index]) {
                                if (oldStats[index] != mergedStats[index]) {
                                    throw new IllegalStateException(
                                            "Found multiple values when executing TakeTheOneUniqueValue for group "
                                                    + index + ": values are " + oldStats[index]
                                                    + " and " + mergedStats[index]
                                    );
                                }
                            } else {
                                mergedStats[index] = oldStats[index];
                                anyFound[index] = true;
                            }
                            break;
                        }
                        case FailIfPresent: {
                            throw new IllegalStateException("Should not be merging saved group stats with FailIfPresent!");
                        }
                    }
                }
                newSavedGroupStatsEntries.put(k, new Session.SavedGroupStats(v.depth - 1, mergedStats));
            }
        }
        session.savedGroupStats.putAll(newSavedGroupStatsEntries);
        final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[session.numGroups];
        final RegroupCondition[] fakeConditions = new RegroupCondition[]{new RegroupCondition("fakeField", true, 1, null, false)};
        final Session.GroupKey[] newGroupKeys = new Session.GroupKey[maxIndex + 1];
        for (int group = 1; group <= session.numGroups; group++) {
            final Session.GroupKey groupKey = session.groupKeys.get(group);
            final int newGroup = groupKey.parent.index;
            rules[group - 1] = new GroupMultiRemapRule(group, group, new int[]{newGroup}, fakeConditions);
            newGroupKeys[groupKey.parent.index] = groupKey.parent;
        }
        for (final Session.ImhotepSessionInfo imhotepSessionInfo : session.sessions.values()) {
            imhotepSessionInfo.session.regroup(rules);
        }
        session.currentDepth -= 1;
        session.numGroups = maxIndex;
        session.groupKeys = Arrays.asList(newGroupKeys);

        out.accept("RegroupedIntoParent");
    }
}
