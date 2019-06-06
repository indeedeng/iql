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
import com.google.common.collect.Sets;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.metrics.aggregate.AggregateStatTree;
import com.indeed.iql2.execution.AggregateFilter;
import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.commands.misc.IterateHandler;
import com.indeed.iql2.execution.commands.misc.IterateHandlers;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.util.core.Pair;
import lombok.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ComputeAndCreateGroupStatsLookups implements Command {
    private final List<Pair<Command, String>> namedComputations;

    public ComputeAndCreateGroupStatsLookups(List<Pair<Command, String>> namedComputations) {
        this.namedComputations = namedComputations;
    }

    @Override
    public void execute(final Session session) throws ImhotepOutOfMemoryException, IOException {
        if (tryMultiDistinct(session, namedComputations)) {
            return;
        }

        final List<IterateHandler<Void>> handlerables = Lists.newArrayListWithCapacity(namedComputations.size());
        final Set<FieldSet> fields = Sets.newHashSet();
        session.timer.push("get IterateHandlers");
        for (final Pair<Command, String> namedComputation : namedComputations) {
            final Command computation = namedComputation.getFirst();
            final String name = namedComputation.getSecond();
            if (computation instanceof GetGroupDistincts) {
                final GetGroupDistincts getGroupDistincts = (GetGroupDistincts) computation;
                fields.add(getGroupDistincts.field);
                handlerables.add(new NameIt<>(session, ComputeAndCreateGroupStatsLookups::longToDouble, getGroupDistincts.iterateHandler(session), name));
            } else if (computation instanceof GetGroupDistinctsWindowed) {
                final GetGroupDistinctsWindowed getGroupDistinctsWindowed = (GetGroupDistinctsWindowed) computation;
                fields.add(getGroupDistinctsWindowed.field);
                handlerables.add(new NameIt<>(session, ComputeAndCreateGroupStatsLookups::longToDouble, getGroupDistinctsWindowed.iterateHandler(session), name));
            } else if (computation instanceof GetSimpleGroupDistincts) {
                final long[] groupStats = ((GetSimpleGroupDistincts)computation).evaluate(session);
                final double[] results = longToDouble(groupStats);
                new CreateGroupStatsLookup(results, name).execute(session);
            } else if (computation instanceof SumAcross) {
                final SumAcross sumAcross = (SumAcross) computation;
                fields.add(sumAcross.field);
                handlerables.add(new NameIt<>(session, Function.identity(), sumAcross.iterateHandler(session), name));
            } else if (computation instanceof GetGroupPercentiles) {
                final GetGroupPercentiles getGroupPercentiles = (GetGroupPercentiles) computation;
                fields.add(getGroupPercentiles.field);
                handlerables.add(new NameIt<>(session, p -> longToDouble(p), getGroupPercentiles.iterateHandler(session), name));
            } else if (computation instanceof GetGroupStats) {
                final double[][] groupStats = ((GetGroupStats)computation).evaluate(session);
                final double[] results = Arrays.copyOf(groupStats[0], session.getNumGroups() + 1);
                new CreateGroupStatsLookup(results, name).execute(session);
            } else if (computation instanceof ComputeFieldExtremeValue) {
                final double[] results = ((ComputeFieldExtremeValue)computation).evaluate(session);
                new CreateGroupStatsLookup(results, name).execute(session);
            } else {
                throw new IllegalArgumentException("Shouldn't be able to reach here. Bug in ComputeAndCreateGroupStatsLookups parser.");
            }
        }
        session.timer.pop();
        if (!handlerables.isEmpty()) {
            if (fields.size() != 1) {
                throw new IllegalStateException("Invalid number of fields seen: " + fields.size());
            }
            final FieldSet theField = fields.stream().findFirst().orElse(null);
            session.timer.push("IterateHandlers.executeMulti");
            IterateHandlers.executeMulti(session, theField, handlerables);
            session.timer.pop();
        }
    }

    @Value
    private static class FilterInfo {
        private final AggregateFilter filter;
        private final int windowSize;
    }

    static boolean tryMultiDistinct(Session session, List<Pair<Command, String>> namedComputations) throws IOException, ImhotepOutOfMemoryException {
        session.timer.push("checking aggregate distinct eligibility");
        final Map<String, FilterInfo> namedFilters = new TreeMap<>();
        boolean allDistinct = true;
        boolean allNonOrdered = true;
        boolean anyWindowed = false;
        final Set<FieldSet> fields = new HashSet<>();
        for (final Pair<Command, String> computation : namedComputations) {
            final Command command = computation.getFirst();
            if (command instanceof GetGroupDistincts) {
                final GetGroupDistincts distinct = (GetGroupDistincts) command;
                fields.add(distinct.field);
                final AggregateFilter filter = distinct.filter.orElse(new AggregateFilter.Constant(true));
                allNonOrdered &= !filter.needSorted();
                namedFilters.put(computation.getSecond(), new FilterInfo(filter, 1));
            } else if (command instanceof GetSimpleGroupDistincts) {
                final GetSimpleGroupDistincts distinct = (GetSimpleGroupDistincts) command;
                fields.add(distinct.field);
                namedFilters.put(computation.getSecond(), new FilterInfo(new AggregateFilter.Constant(true), 1));
            } else if (command instanceof GetGroupDistinctsWindowed) {
                final GetGroupDistinctsWindowed distinct = (GetGroupDistinctsWindowed) command;
                fields.add(distinct.field);
                final AggregateFilter filter = distinct.filter.orElse(new AggregateFilter.Constant(true));
                allNonOrdered &= !filter.needSorted();
                anyWindowed = true;
                namedFilters.put(computation.getSecond(), new FilterInfo(filter, distinct.windowSize));
            } else {
                allDistinct = false;
            }
        }
        session.timer.pop();

        if (!(allDistinct && allNonOrdered)) {
            return false;
        }

        session.timer.push("preparing for aggregate distinct");
        // We should not be able to have an instance of this class with different desired fields
        Preconditions.checkState((long) fields.size() == 1);

        // safe because of checkState above
        final FieldSet field = fields.stream().findFirst().get();

        final List<Map.Entry<String, FilterInfo>> filters = new ArrayList<>(namedFilters.entrySet());

        final Set<QualifiedPush> allPushes = filters.stream()
                .flatMap(x -> x.getValue().getFilter().requires().stream())
                .collect(Collectors.toSet());
        final Map<String, List<List<String>>> sessionStats = new HashMap<>();
        final Map<QualifiedPush, AggregateStatTree> atomicStats = session.pushMetrics(allPushes, sessionStats);
        final List<AggregateStatTree> filterTrees = filters.stream()
                .map(x -> x.getValue().getFilter().toImhotep(atomicStats))
                .collect(Collectors.toList());
        final List<Integer> windowSizes = filters.stream()
                .map(x -> x.getValue().getWindowSize())
                .collect(Collectors.toList());


        final List<RemoteImhotepMultiSession.SessionField> sessionFields = field.datasets().stream()
                .map(x -> new RemoteImhotepMultiSession.SessionField(session.sessions.get(x).session, field.datasetFieldName(x), sessionStats.getOrDefault(x, Collections.emptyList())))
                .collect(Collectors.toList());

        final int numFilters = filters.size();
        final double[][] results = new double[numFilters][];
        for (int i = 0; i < results.length; i++) {
            results[i] = new double[session.getNumGroups() + 1];
        }
        session.timer.pop();

        session.timer.push("requesting aggregate distinct");
        try (final GroupStatsIterator groupStatsIterator = RemoteImhotepMultiSession.aggregateDistinct(
                sessionFields,
                filterTrees,
                windowSizes,
                session.isIntField(field),
                anyWindowed ? session.groupKeySet.getParents() : null
        )) {
            session.timer.pop();
            session.timer.push("consuming aggregate distinct");
            int i = 0;
            while (groupStatsIterator.hasNext()) {
                final int group = i / numFilters;
                final int filter = i % numFilters;
                results[filter][group] = (double) groupStatsIterator.nextLong();
                i += 1;
            }
        }

        for (int i = 0; i < numFilters; i++) {
            new CreateGroupStatsLookup(results[i], filters.get(i).getKey()).execute(session);
        }
        session.timer.pop();

        return true;
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

        private void nameIt(Session session, double[] value) {
            new CreateGroupStatsLookup(value, name).execute(session);
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
        public Void finish() {
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
