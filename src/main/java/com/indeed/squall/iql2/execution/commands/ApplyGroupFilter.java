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

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.squall.iql2.execution.AggregateFilter;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.SessionCallback;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.sets.DumbGroupKeySet;
import com.indeed.util.core.TreeTimer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ApplyGroupFilter implements Command {
    private static final Logger log = Logger.getLogger(ApplyGroupFilter.class);

    private final AggregateFilter filter;

    public ApplyGroupFilter(final AggregateFilter filter) {
        this.filter = filter;
    }

    @Override
    public void execute(final Session session, final Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final Set<QualifiedPush> requires = filter.requires();
        final Map<QualifiedPush, Integer> metricIndexes = new HashMap<>();
        final Map<String, IntList> sessionMetricIndexes = new HashMap<>();
        session.pushMetrics(requires, metricIndexes, sessionMetricIndexes, true);
        filter.register(metricIndexes, session.groupKeySet);
        final long[][] stats = new long[metricIndexes.size()][];
        session.process(new SessionCallback() {
            @Override
            public void handle(final TreeTimer timer, final String name, final ImhotepSession session) throws ImhotepOutOfMemoryException {
                for (final Map.Entry<QualifiedPush, Integer> entry : metricIndexes.entrySet()) {
                    if (!entry.getKey().sessionName.equals(name)) {
                        continue;
                    }
                    final List<String> pushes = entry.getKey().pushes;
                    session.pushStats(pushes);
                    final long[] groupStats = session.getGroupStats(0);
                    session.popStat();
                    synchronized (stats) {
                        stats[entry.getValue()] = groupStats;
                    }
                }
            }
        });
        final boolean[] keep = filter.getGroupStats(stats, session.numGroups);
        int keepCount = 0;
        for (final boolean b : keep) {
            if (b) {
                keepCount++;
            }
        }
        final GroupMultiRemapMessage[] rules = new GroupMultiRemapMessage[keepCount];
        final List<GroupKey> newGroupKeys = new ArrayList<>();
        newGroupKeys.add(null);
        final IntList newGroupParents = new IntArrayList();
        newGroupParents.add(-1);
        int ruleIndex = 0;
        for (int i = 1; i < keep.length; i++) {
            if (keep[i]) {
                final int newGroup = newGroupParents.size();
                rules[ruleIndex] = GroupMultiRemapMessage.newBuilder()
                        .setTargetGroup(i)
                        .setNegativeGroup(newGroup)
                        .build();
                ruleIndex++;
                newGroupKeys.add(session.groupKeySet.groupKey(i));
                newGroupParents.add(session.groupKeySet.parentGroup(i));
            }
        }
        session.process(new SessionCallback() {
            @Override
            public void handle(final TreeTimer timer, final String name, final ImhotepSession session) throws ImhotepOutOfMemoryException {
                session.regroupWithProtos(rules, true);
            }
        });
        session.groupKeySet = DumbGroupKeySet.create(session.groupKeySet.previous(), newGroupParents.toIntArray(), newGroupKeys);
        session.numGroups = session.groupKeySet.numGroups();
        out.accept("done");
    }
}
