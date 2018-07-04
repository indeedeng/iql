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

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.AggregateFilter;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.commands.misc.IterateHandler;
import com.indeed.squall.iql2.execution.commands.misc.IterateHandlerable;
import com.indeed.squall.iql2.execution.commands.misc.IterateHandlers;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class SumAcross implements IterateHandlerable<double[]>, Command {
    public final Set<String> scope;
    public final String field;
    public final AggregateMetric metric;
    public final Optional<AggregateFilter> filter;

    public SumAcross(Set<String> scope, String field, AggregateMetric metric, Optional<AggregateFilter> filter) {
        this.scope = scope;
        this.field = field;
        this.metric = metric;
        this.filter = filter;
    }

//    public static Closeable pushAndRegister(Session session, Map<QualifiedPush, Integer> metricIndexes, Map<String, IntList> sessionMetricIndexes, List<Session.GroupKey> groupKeys, Iterable<? extends Pushable> pushables) throws ImhotepOutOfMemoryException {
//        final Set<QualifiedPush> allPushes = Sets.newHashSet();
//        pushables.forEach(x -> allPushes.addAll(x.requires()));
//        session.pushMetrics(allPushes, metricIndexes, sessionMetricIndexes);
//        pushables.forEach(x -> x.register(metricIndexes, groupKeys));
//        return new Closeable() {
//            @Override
//            public void close() {
//                session.getSessionsMapRaw().values().forEach(s -> {
//                    while (s.getNumStats() > 0) {
//                        s.popStat();
//                    }
//                });
//            }
//        };
//    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        out.accept(Session.MAPPER.writeValueAsString(IterateHandlers.executeSingle(session, field, iterateHandler(session))));
    }

    @Override
    public IterateHandler<double[]> iterateHandler(Session session) {
        return new IterateHandlerImpl(session.numGroups);
    }

    private class IterateHandlerImpl implements IterateHandler<double[]> {
        private final double[] groupSums;

        public IterateHandlerImpl(int numGroups) {
            this.groupSums = new double[numGroups];
        }

        @Override
        public Set<String> scope() {
            return scope;
        }

        @Override
        public Set<QualifiedPush> requires() {
            final Set<QualifiedPush> pushes = Sets.newHashSet();
            pushes.addAll(metric.requires());
            if (filter.isPresent()) {
                pushes.addAll(filter.get().requires());
            }
            return pushes;
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, GroupKeySet groupKeySet) {
            metric.register(metricIndexes, groupKeySet);
            if (filter.isPresent()) {
                filter.get().register(metricIndexes, groupKeySet);
            }
        }

        @Override
        public Session.IntIterateCallback intIterateCallback() {
            return new IterateCallback();
        }

        @Override
        public Session.StringIterateCallback stringIterateCallback() {
            return new IterateCallback();
        }

        @Override
        public double[] finish() throws ImhotepOutOfMemoryException {
            return groupSums;
        }

        private class IterateCallback implements Session.IntIterateCallback, Session.StringIterateCallback {
            @Override
            public void term(final long term, final long[] stats, final int group) {
                final double v = metric.apply(term, stats, group);
                if (filter.isPresent()) {
                    if (filter.get().allow(term, stats, group)) {
                        groupSums[group - 1] += v;
                    }
                } else {
                    groupSums[group - 1] += v;
                }
            }

            @Override
            public void term(final String term, final long[] stats, final int group) {
                final double v = metric.apply(term, stats, group);
                if (filter.isPresent()) {
                    if (filter.get().allow(term, stats, group)) {
                        groupSums[group - 1] += v;
                    }
                } else {
                    groupSums[group - 1] += v;
                }
            }

            @Override
            public boolean needSorted() {
                return metric.needSorted() || (filter.isPresent() && filter.get().needSorted());
            }

            @Override
            public boolean needGroup() {
                return true;
            }

            @Override
            public boolean needStats() {
                return metric.needStats()
                        || (filter.isPresent() && filter.get().needStats());
            }
        }
    }
}
