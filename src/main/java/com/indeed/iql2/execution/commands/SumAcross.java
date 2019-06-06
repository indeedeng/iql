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
import com.indeed.iql2.execution.AggregateFilter;
import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.commands.misc.IterateHandler;
import com.indeed.iql2.execution.commands.misc.IterateHandlers;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class SumAcross implements Command {
    public final FieldSet field;
    public final AggregateMetric metric;
    public final Optional<AggregateFilter> filter;

    public SumAcross(FieldSet field, AggregateMetric metric, Optional<AggregateFilter> filter) {
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
    public void execute(final Session session) {
        // this Command needs special processing since it returns some data.
        throw new IllegalStateException("Call evaluate() method instead");
    }

    public double[] evaluate(final Session session) throws ImhotepOutOfMemoryException, IOException {
        return IterateHandlers.executeSingle(session, field, iterateHandler(session));
    }

    public IterateHandler<double[]> iterateHandler(final Session session) {
        return new IterateHandlerImpl(session.getNumGroups());
    }

    private class IterateHandlerImpl implements IterateHandler<double[]> {
        private final double[] groupSums;

        IterateHandlerImpl(int numGroups) {
            this.groupSums = new double[numGroups+1];
        }

        @Override
        public Set<String> scope() {
            return field.datasets();
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
        public double[] finish() {
            return groupSums;
        }

        private class IterateCallback implements Session.IntIterateCallback, Session.StringIterateCallback {
            @Override
            public void term(final long term, final long[] stats, final int group) {
                final double v = metric.apply(term, stats, group);
                if (filter.isPresent()) {
                    if (filter.get().allow(term, stats, group)) {
                        groupSums[group] += v;
                    }
                } else {
                    groupSums[group] += v;
                }
            }

            @Override
            public void term(final String term, final long[] stats, final int group) {
                final double v = metric.apply(term, stats, group);
                if (filter.isPresent()) {
                    if (filter.get().allow(term, stats, group)) {
                        groupSums[group] += v;
                    }
                } else {
                    groupSums[group] += v;
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
