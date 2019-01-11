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
import com.indeed.iql2.execution.Pushable;
import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.commands.misc.IterateHandler;
import com.indeed.iql2.execution.commands.misc.IterateHandlerable;
import com.indeed.iql2.execution.commands.misc.IterateHandlers;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class GetGroupDistincts implements IterateHandlerable<long[]>, Command {
    public final FieldSet field;
    public final Optional<AggregateFilter> filter;

    public GetGroupDistincts(FieldSet field, Optional<AggregateFilter> filter) {
        this.field = field;
        this.filter = filter;
    }

    @Override
    public void execute(final Session session) {
        // this Command needs special processing since it returns some data.
        throw new IllegalStateException("Call evaluate() method instead");
    }

    public long[] evaluate(final Session session) throws ImhotepOutOfMemoryException, IOException {
        return IterateHandlers.executeSingle(session, field, iterateHandler(session));
    }

    public IterateHandler<long[]> iterateHandler(Session session) {
        return new IterateHandlerImpl(session);
    }

    // This could be accomplished with a circular buffer of size numStats*windowSize, saving a ton of RAM,
    // but this leads to substantial code complexity and isn't actually worth it.
    // We're likely spending megabytes on the heap on this, but gigabytes on tmpfs allocations of the FTGS stream anyway.
    // We don't need to save those megabytes.
    private class IterateHandlerImpl implements IterateHandler<long[]> {
        private final long[] groupCounts;

        private IterateHandlerImpl(final Session session) {
            this.groupCounts = new long[session.numGroups + 1];
        }

        @Override
        public Set<QualifiedPush> requires() {
            return filter.transform(Pushable::requires).or(Collections.emptySet());
        }

        @Override
        public void register(final Map<QualifiedPush, Integer> metricIndexes, final GroupKeySet groupKeySet) {
            if (filter.isPresent()) {
                filter.get().register(metricIndexes, groupKeySet);
            }
        }

        @Override
        public Set<String> scope() {
            return field.datasets();
        }

        @Override
        public long[] finish() {
            return groupCounts;
        }

        @Override
        public Session.IntIterateCallback intIterateCallback() {
            return new IntIterateCallback();
        }

        @Override
        public Session.StringIterateCallback stringIterateCallback() {
            return new StringIterateCallback();
        }


        private class IntIterateCallback implements Session.IntIterateCallback {
            @Override
            public void term(final long term, final long[] stats, final int group) {
                final boolean countIt;
                if (filter.isPresent()) {
                    countIt = filter.get().allow(term, stats, group);
                } else {
                    countIt = true;
                }
                if (countIt) {
                    groupCounts[group] += 1;
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
            @Override
            public void term(final String term, final long[] stats, final int group) {
                final boolean countIt;
                if (filter.isPresent()) {
                    countIt = filter.get().allow(term, stats, group);
                } else {
                    countIt = true;
                }
                if (countIt) {
                    groupCounts[group] += 1;
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
    }

    @Override
    public String toString() {
        return "GetGroupDistincts{" +
                "field=" + field +
                ", filter=" + filter +
                '}';
    }
}
