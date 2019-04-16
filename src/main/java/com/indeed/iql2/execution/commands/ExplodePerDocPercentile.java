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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.SingleFieldRegroupTools;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.groupkeys.GroupKey;
import com.indeed.iql2.execution.groupkeys.StringGroupKey;
import com.indeed.iql2.execution.groupkeys.sets.DumbGroupKeySet;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.commons.lang.ArrayUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExplodePerDocPercentile implements Command {
    public final FieldSet field;
    public final int numBuckets;

    public ExplodePerDocPercentile(FieldSet field, int numBuckets) {
        this.field = field;
        this.numBuckets = numBuckets;
    }

    @Override
    public void execute(final Session session) throws ImhotepOutOfMemoryException, IOException {
        final FieldSet field = this.field;
        final int numBuckets = this.numBuckets;

        session.checkGroupLimit((long) (numBuckets) * session.numGroups);

        session.timer.push("get counts");
        final Map<String, List<List<String>>> sessionStats = new HashMap<>();
        final long[] counts = new long[session.numGroups + 1];
        for (final Session.ImhotepSessionInfo s : session.sessions.values()) {
            final long[] stats = s.session.getGroupStats(Collections.singletonList("hasintfield " + field.datasetFieldName(s.displayName)));
            for (int i = 0; i < stats.length; i++) {
                counts[i] += stats[i];
            }
            sessionStats.put(s.displayName, Collections.singletonList(Collections.singletonList("count()")));
        }
        session.timer.pop();

        final long[] runningCounts = new long[session.numGroups + 1];
        final long[][] cutoffs = new long[session.numGroups + 1][numBuckets];
        final int[] soFar = new int[session.numGroups + 1];
        final Map<String, IntList> metricIndexes = Maps.newHashMap();
        for (final String k : session.sessions.keySet()) {
            final int nextIndex = metricIndexes.size();
            metricIndexes.put(k, new IntArrayList(new int[]{nextIndex}));
        }
        session.timer.push("compute cutoffs (iterateMultiInt)");
        Session.iterateMultiInt(session.getSessionsMapRaw(), metricIndexes, Collections.emptyMap(), field, sessionStats, new Session.IntIterateCallback() {
            @Override
            public void term(final long term, final long[] stats, final int group) {
                for (final long stat : stats) {
                    runningCounts[group] += stat;
                }

                final int fraction;
                if (runningCounts[group] == counts[group]) {
                    // To not to rely on double precision.
                    fraction = numBuckets;
                } else if (runningCounts[group] < counts[group]) {
                    fraction = (int) Math.floor(((double) numBuckets * runningCounts[group]) / counts[group]);
                } else {
                    // per-group hasintfield < sum_term per-term-group hasintfield means it is multi-valued field.
                    throw new UnsupportedOperationException(
                            String.format("Query failed trying to do ExplodePerDocPercentile on a multi-valued field %s.", field)
                    );
                }
                for (int i = soFar[group]; i < fraction; i++) {
                    cutoffs[group][i] = term;
                }
                soFar[group] = Math.max(soFar[group], fraction);
            }

            @Override
            public boolean needSorted() {
                return true;
            }

            @Override
            public boolean needGroup() {
                return true;
            }

            @Override
            public boolean needStats() {
                return true;
            }
        }, session.timer, session.options);
        session.timer.pop();

        for (int group = 1; group <= session.numGroups; group++) {
            Preconditions.checkState(runningCounts[group] == counts[group], "Failed to detect multi-valued field, or missed some values?");
            Preconditions.checkState((soFar[group] == numBuckets) || (counts[group] == 0));
        }

        session.timer.push("compute bucket remaps");
        final SingleFieldRegroupTools.SingleFieldRulesBuilder rulesBuilder = session.createRuleBuilder(field, true, true);
        final List<GroupKey> nextGroupKeys = Lists.newArrayList();
        final IntList groupParents = new IntArrayList();
        nextGroupKeys.add(null);
        groupParents.add(-1);
        for (int group = 1; group <= session.numGroups; group++) {
            final IntArrayList positiveGroups = new IntArrayList();
            final LongArrayList thresholds = new LongArrayList();
            for (int bucket = 0; bucket < numBuckets; bucket++) {
                if ((bucket > 0) && (cutoffs[group][bucket] == cutoffs[group][bucket - 1])) {
                    continue;
                }
                final int end = ArrayUtils.lastIndexOf(cutoffs[group], cutoffs[group][bucket]);
                final String keyTerm = "[" + (double) bucket / numBuckets + ", " + (double) (end + 1) / numBuckets + ")";
                final int newGroup = nextGroupKeys.size();
                // TODO: Not use StringGroupKey this.
                nextGroupKeys.add(StringGroupKey.fromTerm(keyTerm, session.formatter));
                groupParents.add(group);
                positiveGroups.add(newGroup);
                thresholds.add(cutoffs[group][bucket]);
            }
            rulesBuilder.addIntRule(
                    group,
                    0,
                    positiveGroups.toIntArray(),
                    thresholds.toLongArray());
        }
        session.timer.pop();

        session.regroupWithSingleFieldRules(rulesBuilder, field, true, true, true);

        session.assumeDense(DumbGroupKeySet.create(session.groupKeySet, groupParents.toIntArray(), nextGroupKeys));
    }
}
