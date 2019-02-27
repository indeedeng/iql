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

import com.google.common.collect.Maps;
import com.google.common.primitives.Booleans;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.AggregateFilter;
import com.indeed.iql2.execution.ImhotepSessionHolder;
import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.SessionCallback;
import com.indeed.iql2.execution.groupkeys.GroupKey;
import com.indeed.iql2.execution.groupkeys.sets.DumbGroupKeySet;
import com.indeed.util.logging.TracingTreeTimer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.log4j.Logger;

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
    public void execute(final Session session) throws ImhotepOutOfMemoryException {
        final Set<QualifiedPush> requires = filter.requires();
        final Map<QualifiedPush, Integer> metricIndexes = new HashMap<>();
        final Map<String, IntList> sessionMetricIndexes = new HashMap<>();
        final Map<String, List<List<String>>> sessionStats = Maps.newHashMap();
        session.pushMetrics(requires, metricIndexes, sessionMetricIndexes, sessionStats);
        filter.register(metricIndexes, session.groupKeySet);
        final long[][] stats = new long[metricIndexes.size()][];
        session.process(new SessionCallback() {
            @Override
            public void handle(final TracingTreeTimer timer, final String name, final ImhotepSessionHolder session) throws ImhotepOutOfMemoryException {
                for (final Map.Entry<QualifiedPush, Integer> entry : metricIndexes.entrySet()) {
                    if (!entry.getKey().sessionName.equals(name)) {
                        continue;
                    }
                    final long[] groupStats = session.getGroupStats(entry.getKey().pushes);
                    synchronized (stats) {
                        stats[entry.getValue()] = groupStats;
                    }
                }
            }
        });
        final boolean[] keep = filter.getGroupStats(stats, session.numGroups);
        final int keepCount = Booleans.countTrue(keep);
        final int[] fromGroups = new int[keepCount];
        final int[] toGroups = new int[keepCount];
        final List<GroupKey> newGroupKeys = new ArrayList<>();
        newGroupKeys.add(null);
        final IntList newGroupParents = new IntArrayList();
        newGroupParents.add(-1);
        int ruleIndex = 0;
        for (int i = 1; i < keep.length; i++) {
            if (keep[i]) {
                final int newGroup = newGroupParents.size();
                fromGroups[ruleIndex] = i;
                toGroups[ruleIndex] = newGroup;
                ruleIndex++;
                newGroupKeys.add(session.groupKeySet.groupKey(i));
                newGroupParents.add(session.groupKeySet.parentGroup(i));
            }
        }
        session.remapGroups(fromGroups, toGroups);
        session.groupKeySet = DumbGroupKeySet.create(session.groupKeySet.previous(), newGroupParents.toIntArray(), newGroupKeys);
        session.numGroups = session.groupKeySet.numGroups();
    }
}
