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
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.commands.misc.IterateHandler;
import com.indeed.iql2.execution.commands.misc.IterateHandlers;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class GetGroupPercentiles implements Command {
    public final FieldSet field;
    public final double percentile;

    public GetGroupPercentiles(final FieldSet field, final double percentile) {
        this.field = field;
        this.percentile = percentile;
    }

    @Override
    public void execute(final Session session) {
        // this Command needs special processing since it returns some data.
        throw new IllegalStateException("Call evaluate() method instead");
    }

    public long[] evaluate(final Session session) throws ImhotepOutOfMemoryException, IOException {
        return IterateHandlers.executeSingle(session, field, iterateHandler(session));
    }

    public IterateHandler<long[]> iterateHandler(final Session session) throws ImhotepOutOfMemoryException {
        session.timer.push("compute counts");
        final long[] counts = new long[session.getNumGroups() + 1];
        for (final String dataset : field.datasets()) {
            final ImhotepSession s = session.sessions.get(dataset).session;
            final long[] stats = s.getGroupStats(Collections.singletonList("hasintfield " + field.datasetFieldName(dataset)));
            for (int i = 0; i < stats.length; i++) {
                counts[i] += stats[i];
            }
        }
        session.timer.pop();

        session.timer.push("compute boundaries");
        final double[] requiredCounts = new double[counts.length];
        for (int i = 1; i < counts.length; i++) {
            requiredCounts[i] = (percentile / 100.0) * (double)counts[i];
        }
        session.timer.pop();
        return new IterateHandlerImpl(session.getNumGroups(), requiredCounts);
    }

    private class IterateHandlerImpl implements IterateHandler<long[]> {
        private final IntSet relevantIndexes = new IntArraySet();
        private final long[] results;
        private final long[] runningCounts;
        private final double[] requiredCounts;

        IterateHandlerImpl(final int numGroups, final double[] requiredCounts) {
            this.requiredCounts = requiredCounts;
            results = new long[numGroups + 1];
            runningCounts = new long[numGroups + 1];
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
        public void register(final Map<QualifiedPush, Integer> metricIndexes, final GroupKeySet groupKeySet) {
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
        public long[] finish() {
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

                final double minRequired = requiredCounts[group];
                if ((newCount >= minRequired) && (oldCount < minRequired)) {
                    results[group] = term;
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
