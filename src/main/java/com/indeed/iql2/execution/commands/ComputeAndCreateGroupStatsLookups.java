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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.commands.misc.IterateHandler;
import com.indeed.iql2.execution.commands.misc.IterateHandlers;
import java.util.function.Consumer;;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.util.core.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class ComputeAndCreateGroupStatsLookups implements Command {
    private final List<Pair<Command, String>> namedComputations;

    public ComputeAndCreateGroupStatsLookups(List<Pair<Command, String>> namedComputations) {
        this.namedComputations = namedComputations;
    }

    @Override
    public void execute(Session session, Consumer<String> ignored) throws ImhotepOutOfMemoryException, IOException {
        final List<IterateHandler<Void>> handlerables = Lists.newArrayListWithCapacity(namedComputations.size());
        final Set<String> fields = Sets.newHashSet();
        session.timer.push("get IterateHandlers");
        for (final Pair<Command, String> namedComputation : namedComputations) {
            final Object computation = namedComputation.getFirst();
            final String name = namedComputation.getSecond();
            if (computation instanceof GetGroupDistincts) {
                final GetGroupDistincts getGroupDistincts = (GetGroupDistincts) computation;
                fields.add(getGroupDistincts.field);
                handlerables.add(new NameIt<>(session, new Function<long[], double[]>() {
                    public double[] apply(long[] longs) {
                        return longToDouble(longs);
                    }
                }, getGroupDistincts.iterateHandler(session), name));
            } else if (computation instanceof GetSimpleGroupDistincts) {
                final AtomicReference<String> reference = new AtomicReference<>();
                ((Command) computation).execute(session, reference::set);
                final long[] groupStats = Session.MAPPER.readValue(reference.get(), new TypeReference<long[]>() {});
                final double[] results = longToDouble(groupStats);
                new CreateGroupStatsLookup(Session.prependZero(results), Optional.of(name)).execute(session, s -> {});
            } else if (computation instanceof SumAcross) {
                final SumAcross sumAcross = (SumAcross) computation;
                fields.add(sumAcross.field);
                handlerables.add(new NameIt<>(session, Functions.<double[]>identity(), sumAcross.iterateHandler(session), name));
            } else if (computation instanceof GetGroupPercentiles) {
                final GetGroupPercentiles getGroupPercentiles = (GetGroupPercentiles) computation;
                fields.add(getGroupPercentiles.field);
                handlerables.add(new NameIt<>(session, new Function<long[][], double[]>() {
                    public double[] apply(long[][] input) {
                        return longToDouble(input[0]);
                    }
                }, getGroupPercentiles.iterateHandler(session), name));
            } else if (computation instanceof GetGroupStats) {
                final AtomicReference<String> reference = new AtomicReference<>();
                ((Command) computation).execute(session, new Consumer<String>() {
                    public void accept(String s) {
                        reference.set(s);
                    }
                });
                final List<Session.GroupStats> groupStats = Session.MAPPER.readValue(reference.get(), new TypeReference<List<Session.GroupStats>>() {});
                final double[] results = new double[groupStats.size()];
                for (int i = 0; i < groupStats.size(); i++) {
                    results[i] = groupStats.get(i).stats[0];
                }
                new CreateGroupStatsLookup(Session.prependZero(results), Optional.of(name)).execute(session, new Consumer<String>() {
                    public void accept(String s) {
                    }
                });
            } else if (computation instanceof GetFieldMax) {
                final GetFieldMax getFieldMax = (GetFieldMax) computation;
                fields.add(getFieldMax.field);
                handlerables.add(new NameIt<>(session, new Function<long[], double[]>() {
                    public double[] apply(long[] input) {
                        return longToDouble(input);
                    }
                }, getFieldMax.iterateHandler(session), name));
            } else if (computation instanceof GetFieldMin) {
                final GetFieldMin getFieldMin = (GetFieldMin) computation;
                fields.add(getFieldMin.field);
                handlerables.add(new NameIt<>(session, new Function<long[], double[]>() {
                    public double[] apply(long[] input) {
                        return longToDouble(input);
                    }
                }, getFieldMin.iterateHandler(session), name));
            } else if (computation instanceof ComputeBootstrap) {
                final ComputeBootstrap computeBootstrap = (ComputeBootstrap) computation;
                fields.add(computeBootstrap.field);
                handlerables.add(computeBootstrap.iterateHandler(session));
            } else {
                throw new IllegalArgumentException("Shouldn't be able to reach here. Bug in ComputeAndCreateGroupStatsLookups parser.");
            }
        }
        session.timer.pop();
        if (!handlerables.isEmpty()) {
            if (fields.size() != 1) {
                throw new IllegalStateException("Invalid number of fields seen: " + fields.size());
            }
            String theField = null;
            for (final String field : fields) {
                theField = field;
                break;
            }
            session.timer.push("IterateHandlers.executeMulti");
            IterateHandlers.executeMulti(session, theField, handlerables);
            session.timer.pop();
        }
    }

    public static double[] longToDouble(long[] v) {
        final double[] result = new double[v.length];
        for (int i = 0; i < v.length; i++) {
            result[i] = v[i];
        }
        return result;
    }

    private static class NameIt<T> implements IterateHandler<Void> {
        private final Session session;
        private final Function<T, double[]> transform;
        private final IterateHandler<T> inner;
        private final String name;

        private NameIt(Session session, Function<T, double[]> transform, IterateHandler<T> inner, String name) {
            this.session = session;
            this.transform = transform;
            this.inner = inner;
            this.name = name;
        }

        private void nameIt(Session session, double[] value) throws ImhotepOutOfMemoryException, IOException {
            new CreateGroupStatsLookup(Session.prependZero(value), Optional.of(name)).execute(session, new Consumer<String>() {
                public void accept(String s) {
                }
            });
        }

        @Override
        public Set<String> scope() {
            return inner.scope();
        }

        @Override
        public Session.IntIterateCallback intIterateCallback() {
            return inner.intIterateCallback();
        }

        @Override
        public Session.StringIterateCallback stringIterateCallback() {
            return inner.stringIterateCallback();
        }

        @Override
        public Void finish() throws ImhotepOutOfMemoryException, IOException {
            final T result = inner.finish();
            nameIt(session, transform.apply(result));
            return null;
        }

        @Override
        public Set<QualifiedPush> requires() {
            return inner.requires();
        }

        @Override
        public void register(Map<QualifiedPush, Integer> metricIndexes, GroupKeySet groupKeySet) {
            inner.register(metricIndexes, groupKeySet);
        }
    }
}
