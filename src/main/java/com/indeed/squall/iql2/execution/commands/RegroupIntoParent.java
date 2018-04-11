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

package com.indeed.squall.iql2.execution.commands;

import com.google.common.collect.Maps;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.squall.iql2.execution.GroupLookupMergeType;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;

import java.util.Map;

public class RegroupIntoParent implements Command {
    public final GroupLookupMergeType mergeType;

    public RegroupIntoParent(GroupLookupMergeType mergeType) {
        this.mergeType = mergeType;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException {
        session.timer.push("compute remapping");
        final int maxIndex = session.groupKeySet.previous().numGroups() + 1;
        final Map<String, Session.SavedGroupStats> newSavedGroupStatsEntries = Maps.newHashMap();
        for (final Map.Entry<String, Session.SavedGroupStats> entry : session.savedGroupStats.entrySet()) {
            final String k = entry.getKey();
            final Session.SavedGroupStats v = entry.getValue();
            if (v.depth == session.currentDepth) {
                final double[] mergedStats = new double[maxIndex + 1];
                final double[] oldStats = v.stats;
                final boolean[] anyFound = new boolean[maxIndex + 1];
                for (int i = 1; i < oldStats.length; i++) {
                    final int index = session.groupKeySet.parentGroup(i);
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
        session.timer.pop();

        session.timer.push("create rules");
        final GroupMultiRemapMessage[] messages = new GroupMultiRemapMessage[session.numGroups];
        final RegroupConditionMessage fakeCondition = RegroupConditionMessage.newBuilder()
                .setField("fakeField")
                .setIntType(true)
                .setIntTerm(1)
                .setInequality(false)
                .build();
        for (int group = 1; group <= session.numGroups; group++) {
            final int newGroup = session.groupKeySet.parentGroup(group);
            messages[group - 1] = GroupMultiRemapMessage.newBuilder()
                    .setTargetGroup(group)
                    .setNegativeGroup(newGroup)
                    .addCondition(fakeCondition)
                    .addPositiveGroup(newGroup)
                    .build();
        }
        session.timer.pop();
        session.regroupWithProtos(messages, false);
        session.currentDepth -= 1;
        session.numGroups = maxIndex;
        session.groupKeySet = session.groupKeySet.previous();

        out.accept("RegroupedIntoParent");
    }
}
