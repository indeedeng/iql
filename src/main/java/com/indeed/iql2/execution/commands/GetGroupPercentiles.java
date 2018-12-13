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

import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.ImhotepSessionHolder;
import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.commands.misc.IterateHandler;
import com.indeed.iql2.execution.commands.misc.IterateHandlerable;
import com.indeed.iql2.execution.commands.misc.IterateHandlers;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class GetGroupPercentiles implements IterateHandlerable<long[][]>, Command {
    public final FieldSet field;
    public final double[] percentiles;

    public GetGroupPercentiles(FieldSet field, double[] percentiles) {
        this.field = field;
        this.percentiles = percentiles;
    }

    @Override
    public void execute(final Session session) {
        // this Command needs special processing since it returns some data.
        throw new IllegalStateException("Call evaluate() method instead");
    }

    public long[][] evaluate(final Session session) throws ImhotepOutOfMemoryException, IOException {
        return IterateHandlers.executeSingle(session, field, iterateHandler(session));
    }

    @Override
    public IterateHandler<long[][]> iterateHandler(Session session) throws ImhotepOutOfMemoryException {
        session.timer.push("compute counts");
        final double[] percentiles = this.percentiles;
        final long[] counts = new long[session.numGroups + 1];
        for (final String dataset : field.datasets()) {
            final ImhotepSessionHolder s = session.sessions.get(dataset).session;
            s.pushStat("hasintfield " + field.datasetFieldName(dataset));
            final long[] stats = s.getGroupStats(0);
            for (int i = 0; i < stats.length; i++) {
                counts[i] += stats[i];
            }
            s.popStat();
        }
        session.timer.pop();

        session.timer.push("compute boundaries");
        final double[][] requiredCounts = new double[counts.length][];
        for (int i = 1; i < counts.length; i++) {
            requiredCounts[i] = new double[percentiles.length];
            for (int j = 0; j < percentiles.length; j++) {
                requiredCounts[i][j] = (percentiles[j] / 100.0) * (double)counts[i];
            }
        }
        session.timer.pop();
        return new IterateHandlerImpl(session.numGroups, requiredCounts);
    }

    private class IterateHandlerImpl implements IterateHandler<long[][]> {
        private final IntSet relevantIndexes = new IntArraySet();
        private final long[][] results;
        private final long[] runningCounts;
        private final double[][] requiredCounts;

        public IterateHandlerImpl(int numGroups, double[][] requiredCounts) {
            this.requiredCounts = requiredCounts;
            this.results = new long[percentiles.length][numGroups + 1];
            this.runningCounts = new long[numGroups + 1];
        }

        @Override
        public Set<String> scope() {
            return field.datasets();
        }

        @Override
        public Set<QualifiedPush> requires() {
            final Set<String> scope = field.datasets();
            final Set<QualifiedPush> pushes = Sets.newHashSetWithExpectedSize(scope.size());
            for (final String name : scope) {
                pushes.add(new QualifiedPush(name, Collections.singletonList("count()")));
            }
            return pushes;
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, GroupKeySet groupKeySet) {
            for (final String name : field.datasets()) {
                relevantIndexes.add(metricIndexes.get(new QualifiedPush(name, Collections.singletonList("count()"))));
            }
        }

        @Override
        public Session.IntIterateCallback intIterateCallback() {
            return new IntIterateCallback();
        }

        @Override
        public Session.StringIterateCallback stringIterateCallback() {
            throw new IllegalArgumentException("Cannot do GetGroupPercentiles over a string field");
        }

        @Override
        public long[][] finish() {
            return results;
        }

        private class IntIterateCallback implements Session.IntIterateCallback {
            @Override
            public void term(final long term, final long[] stats, final int group) {
                final long oldCount = runningCounts[group];
                long termCount = 0L;
                for (final int index : relevantIndexes) {
                    termCount += stats[index];
                }
                final long newCount = oldCount + termCount;

                final double[] groupRequiredCountsArray = requiredCounts[group];
                for (int i = 0; i < percentiles.length; i++) {
                    final double minRequired = groupRequiredCountsArray[i];
                    if (newCount >= minRequired && oldCount < minRequired) {
                        results[i][group] = term;
                    }
                }

                runningCounts[group] = newCount;
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
        }
    }
}
