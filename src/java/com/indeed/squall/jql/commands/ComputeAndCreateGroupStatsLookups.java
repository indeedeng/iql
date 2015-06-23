package com.indeed.squall.jql.commands;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.common.util.Pair;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.jql.QualifiedPush;
import com.indeed.squall.jql.Session;
import com.indeed.squall.jql.compat.Consumer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class ComputeAndCreateGroupStatsLookups {
    private final List<Pair<Object, String>> namedComputations;

    public ComputeAndCreateGroupStatsLookups(List<Pair<Object, String>> namedComputations) {
        this.namedComputations = namedComputations;
    }

    public void execute(Session session) throws ImhotepOutOfMemoryException, IOException {
        final List<IterateHandler<Void>> handlerables = Lists.newArrayListWithCapacity(namedComputations.size());
        final Set<String> fields = Sets.newHashSet();
        for (final Pair<Object, String> namedComputation : namedComputations) {
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
                final List<Session.GroupStats> groupStats = Session.MAPPER.readValue(reference.get(), new TypeReference<List<Session.GroupStats>>() {});
                session.evaluateCommandInternal(null, new Consumer<String>() {
                    public void accept(String s) {
                        reference.set(s);
                    }
                }, computation);
                final double[] results = new double[groupStats.size()];
                for (int i = 0; i < groupStats.size(); i++) {
                    results[i] = groupStats.get(i).stats[0];
                }
                session.evaluateCommandInternal(null, new Consumer<String>() {
                    public void accept(String s) {
                    }
                }, new CreateGroupStatsLookup(Session.prependZero(results), Optional.of(name)));
            } else {
                throw new IllegalArgumentException("Shouldn't be able to reach here. Bug in ComputeAndCreateGroupStatsLookups parser.");
            }
        }
        if (!handlerables.isEmpty()) {
            if (fields.size() != 1) {
                throw new IllegalStateException("Invalid number of fields seen: " + fields.size());
            }
            String theField = null;
            for (final String field : fields) {
                theField = field;
                break;
            }
            IterateHandlers.executeMulti(session, theField, handlerables);
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
            session.evaluateCommandInternal(null, new Consumer<String>() {
                public void accept(String s) {
                }
            }, new CreateGroupStatsLookup(Session.prependZero(value), Optional.of(name)));
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
        public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
            inner.register(metricIndexes, groupKeys);
        }
    }
}
