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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.common.datastruct.BoundedPriorityQueue;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.api.FTGAIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.metrics.aggregate.AggregateStatTree;
import com.indeed.iql2.execution.AggregateFilter;
import com.indeed.iql2.execution.DenseInt2ObjectMap;
import com.indeed.iql2.execution.ImhotepSessionHolder;
import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.QueryOptions;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.TermSelects;
import com.indeed.iql2.execution.commands.misc.FieldIterateOpts;
import com.indeed.iql2.execution.commands.misc.TopK;

import java.util.ArrayList;
import java.util.function.Consumer;;
import com.indeed.iql2.execution.groupkeys.GroupKeySets;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.iql2.execution.metrics.aggregate.DocumentLevelMetric;
import it.unimi.dsi.fastutil.ints.IntList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

public class SimpleIterate implements Command {
    public final String field;
    public final FieldIterateOpts opts;
    public final List<AggregateMetric> selecting;
    public final List<Optional<String>> formatStrings;
    public final boolean streamResult;
    @Nullable
    public final Set<String> scope;

    private int createdGroupCount = 0;

    public SimpleIterate(String field, FieldIterateOpts opts, List<AggregateMetric> selecting, List<Optional<String>> formatStrings, boolean streamResult, Set<String> scope) {
        this.field = field;
        this.opts = opts;
        this.selecting = selecting;
        this.formatStrings = formatStrings;
        this.streamResult = streamResult;
        this.scope = scope == null ? null : ImmutableSet.copyOf(scope);
        if (this.streamResult && opts.topK.isPresent()) {
            throw new IllegalArgumentException("Can't stream results while doing top-k!");
        }
    }

    @Override
    public void execute(Session session, @Nonnull Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final List<List<List<TermSelects>>> result = this.evaluate(session, out);
        final StringBuilder sb = new StringBuilder();
        Session.writeTermSelectsJson(session.groupKeySet, result, sb);
        out.accept(Session.MAPPER.writeValueAsString(Collections.singletonList(sb.toString())));
    }

    /**
     * Used for determining whether we have any LAG, WINDOW, or RUNNING functions
     * and thus need to process the entire raw FTGS stream for every group within a
     * single server.
     * In practical terms, that means we need to process the ENTIRE FTGS stream for all
     * terms within the IQL server. That is, this process.
     */
    private boolean requiresSortedRawFtgs() {
        final boolean filterSorted = opts.filter.transform(AggregateFilter::needSorted).or(false);
        final boolean metricsSorted = selecting.stream().anyMatch(AggregateMetric::needSorted);
        return filterSorted || metricsSorted;
    }

    public List<List<List<TermSelects>>> evaluate(final Session session, @Nullable Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        session.timer.push("request metrics");
        final Set<QualifiedPush> allPushes = Sets.newHashSet();
        final List<AggregateMetric> metrics = Lists.newArrayList();
        final AggregateMetric topKMetricOrNull;
        if (opts.topK.isPresent()) {
            final TopK topK = opts.topK.get();
            if (topK.metric.isPresent()) {
                topKMetricOrNull = topK.metric.get();
                metrics.add(topKMetricOrNull);
            } else {
                topKMetricOrNull = null;
            }
        } else {
            topKMetricOrNull = null;
        }
        metrics.addAll(this.selecting);
        for (final AggregateMetric metric : metrics) {
            allPushes.addAll(metric.requires());
        }
        if (opts.filter.isPresent()) {
            allPushes.addAll(opts.filter.get().requires());
        }
        session.timer.pop();

        // TODO: Add a feature flag
        if (session.options.contains(QueryOptions.Experimental.USE_MULTI_FTGS) && !requiresSortedRawFtgs() && !opts.sortedIntTermSubset.isPresent() && !opts.sortedStringTermSubset.isPresent()) {
            return evaluateMultiFtgs(session, out, allPushes, topKMetricOrNull);
        }

        session.timer.push("push and register metrics");
        final Map<QualifiedPush, Integer> metricIndexes = Maps.newHashMap();
        final Map<String, IntList> sessionMetricIndexes = Maps.newHashMap();
        session.pushMetrics(allPushes, metricIndexes, sessionMetricIndexes);
        session.registerMetrics(metricIndexes, metrics, Arrays.<AggregateFilter>asList());
        if (opts.filter.isPresent()) {
            opts.filter.get().register(metricIndexes, session.groupKeySet);
        }
        session.timer.pop();

        session.timer.push("prepare for iteration");
        final DenseInt2ObjectMap<Queue<TermSelects>> pqs = new DenseInt2ObjectMap<>();
        if (opts.topK.isPresent()) {
            // TODO: Share this with Iterate. Or delete Iterate.
            final Comparator<TermSelects> comparator = TermSelects.COMPARATOR;
            // TODO: If these queue types change, then a line below with an instanceof check will break.
            if (opts.topK.get().limit.isPresent()) {
                final int limit = opts.topK.get().limit.get();
                for (int i = 1; i <= session.numGroups; i++) {
                    pqs.put(i, BoundedPriorityQueue.newInstance(limit, comparator));
                }
            } else {
                // TODO: HOW MUCH CAPACITY?
                for (int i = 1; i <= session.numGroups; i++) {
                    pqs.put(i, new PriorityQueue<>(100, comparator));
                }
            }
        } else {
            for (int i = 1; i <= session.numGroups; i++) {
                pqs.put(i, new ArrayDeque<TermSelects>());
            }
        }
        final Optional<Integer> ftgsRowLimit;
        if (opts.topK.isPresent()) {
            ftgsRowLimit = Optional.absent();
        } else {
            if (!opts.filter.isPresent()) {
                ftgsRowLimit = opts.limit;
            } else {
                ftgsRowLimit = Optional.absent();
            }
        }
        final AggregateFilter filterOrNull = opts.filter.orNull();

        final Map<String, ImhotepSessionHolder> sessionsMapRaw = session.getSessionsMapRaw();
        final Map<String, ImhotepSessionHolder> sessionsToUse;
        if (scope == null) {
            sessionsToUse = sessionsMapRaw;
        } else {
            sessionsToUse = Maps.newHashMap();
            for (final String dataset : scope) {
                sessionsToUse.put(dataset, sessionsMapRaw.get(dataset));
            }
        }

        final Optional<Session.RemoteTopKParams> topKParams;
        if (sessionsToUse.size() > 1) {
            // if there are multiple datasets, topK must be calculated on a client side.
            topKParams = Optional.absent();
        } else {
            topKParams = getTopKParamsOptional(opts);
        }

        session.timer.pop();


        if (session.isIntField(field)) {
            final Session.IntIterateCallback callback;
            if (streamResult) {
                callback = streamingIntCallback(session, filterOrNull, out);
            } else {
                callback = nonStreamingIntCallback(session, pqs, topKMetricOrNull, filterOrNull);
            }
            session.timer.push("iterateMultiInt");
            Session.iterateMultiInt(sessionsToUse, sessionMetricIndexes, Collections.<String, Integer>emptyMap(), field, topKParams, ftgsRowLimit, opts.sortedIntTermSubset, callback, session.timer, session.options);
            session.timer.pop();
        } else if (session.isStringField(field)) {
            final Session.StringIterateCallback callback;
            if (streamResult) {
                callback = streamingStringCallback(session, filterOrNull, out);
            } else {
                callback = nonStreamingStringCallback(session, pqs, topKMetricOrNull, filterOrNull);
            }
            session.timer.push("iterateMultiString");
            Session.iterateMultiString(sessionsToUse, sessionMetricIndexes, Collections.<String, Integer>emptyMap(), field, topKParams, ftgsRowLimit, opts.sortedStringTermSubset, callback, session.timer, session.options);
            session.timer.pop();
        } else {
            throw new IllegalArgumentException("Field is neither all int nor all string field: " + field);
        }

        session.popStats();

        if (streamResult) {
            return Collections.emptyList();
        } else {
            session.timer.push("convert results");
            final List<List<List<TermSelects>>> allTermSelects = Lists.newArrayList();
            for (int group = 1; group <= session.numGroups; group++) {
                final List<List<TermSelects>> groupTermSelects = Lists.newArrayList();
                final Queue<TermSelects> pq = pqs.get(group);
                final List<TermSelects> listTermSelects = Lists.newArrayList();
                while (!pq.isEmpty()) {
                    listTermSelects.add(pq.poll());
                }
                // TODO: This line is very fragile
                if (pq instanceof BoundedPriorityQueue || pq instanceof PriorityQueue) {
                    groupTermSelects.add(Lists.reverse(listTermSelects));
                } else {
                    groupTermSelects.add(listTermSelects);
                }
                allTermSelects.add(groupTermSelects);
            }
            session.timer.pop();
            return allTermSelects;
        }
    }

    @Nonnull
    private List<List<List<TermSelects>>> evaluateMultiFtgs(final Session session, final @Nullable Consumer<String> out, final Set<QualifiedPush> allPushes, final AggregateMetric topKMetricOrNull) throws ImhotepOutOfMemoryException {
        session.timer.push("prepare for iteration");
        final Map<QualifiedPush, AggregateStatTree> atomicStats = session.pushMetrics(allPushes);
        final List<AggregateStatTree> selects = selecting.stream().map(x -> x.toImhotep(atomicStats)).collect(Collectors.toList());
        final List<AggregateStatTree> filters = opts.filter.transform(x -> Collections.singletonList(x.toImhotep(atomicStats))).or(Collections.emptyList());
        final boolean isIntField = session.isIntField(field);
        int termLimit = opts.limit.or(Integer.MAX_VALUE);
        final int sortStat;

        if (topKMetricOrNull != null) {
            termLimit = Math.min(termLimit, opts.topK.get().limit.or(Integer.MAX_VALUE));
            final AggregateStatTree topKStatTree = topKMetricOrNull.toImhotep(atomicStats);
            final int existingSortStatIndex = selects.indexOf(topKStatTree);
            if (existingSortStatIndex != -1) {
                sortStat = existingSortStatIndex;
            } else {
                sortStat = selects.size();
                selects.add(topKMetricOrNull.toImhotep(atomicStats));
            }
        } else {
            sortStat = -1;
        }
        if (termLimit == Integer.MAX_VALUE) {
            termLimit = 0;
        }

        final List<RemoteImhotepMultiSession.SessionField> sessionFields = new ArrayList<>();
        final Set<String> scope = this.scope == null ? session.sessions.keySet() : this.scope;
        for (final String dataset : scope) {
            final ImhotepSessionHolder sessionHolder = session.sessions.get(dataset).session;
            sessionFields.add(sessionHolder.buildSessionField(field));
        }

        // If we do TopK, we will automatically sort in a new way
        final boolean sorted = topKMetricOrNull == null;

        session.timer.pop();

        session.timer.push("multiFTGS");
        try (FTGAIterator iterator = RemoteImhotepMultiSession.multiFtgs(
                sessionFields,
                selects,
                filters,
                isIntField,
                termLimit,
                sortStat,
                sorted
        )) {
            session.timer.pop();

            session.timer.push("convert results");
            final int numSelects = selecting.size();

            final double[] statsBuf = new double[selects.size()];
            final double[] outputStatsBuf = numSelects == selects.size() ? statsBuf : new double[numSelects];

            final List<List<List<TermSelects>>> result = new ArrayList<>();


            if (!streamResult) {
                for (int group = 1; group <= session.numGroups; group++) {
                    result.add(Collections.singletonList(new ArrayList<>()));
                }
            }

            final String[] formatStrings = formFormatStrings();

            Preconditions.checkState(iterator.nextField());
            while (iterator.nextTerm()) {
                while (iterator.nextGroup()) {
                    iterator.groupStats(statsBuf);

                    if (streamResult) {
                        if (statsBuf != outputStatsBuf) {
                            System.arraycopy(statsBuf, 0, outputStatsBuf, 0, outputStatsBuf.length);
                        }
                        if (iterator.fieldIsIntType()) {
                            out.accept(createRow(session.groupKeySet, iterator.group(), iterator.termIntVal(), outputStatsBuf, formatStrings));
                        } else {
                            out.accept(createRow(session.groupKeySet, iterator.group(), iterator.termStringVal(), outputStatsBuf, formatStrings));
                        }
                    } else {
                        result.get(iterator.group() - 1).get(0).add(new TermSelects(
                                field,
                                iterator.fieldIsIntType(),
                                iterator.termStringVal(),
                                iterator.termIntVal(),
                                Arrays.copyOf(statsBuf, numSelects),
                                topKMetricOrNull == null ? 0.0 : statsBuf[sortStat],
                                iterator.group()
                        ));
                    }

                }
            }
            Preconditions.checkState(!iterator.nextField());
            session.timer.pop();

            if (!streamResult && topKMetricOrNull != null) {
                session.timer.push("Sorting results");
                for (final List<List<TermSelects>> groupResult : result) {
                    groupResult.get(0).sort(TermSelects.COMPARATOR.reversed());
                }
                session.timer.pop();
            }

            return result;
        }
    }

    private static Optional<Session.RemoteTopKParams> getTopKParamsOptional(final FieldIterateOpts opts) {
        Optional<Session.RemoteTopKParams> topKParams = Optional.absent();
        if (!opts.filter.isPresent() && opts.topK.isPresent()
                && opts.topK.get().metric.isPresent() && opts.topK.get().limit.isPresent()) {
            final TopK topK = opts.topK.get();
            final AggregateMetric topKMetric = topK.metric.get();
            if (topKMetric instanceof DocumentLevelMetric) {
                final int limitNum;
                if (opts.limit.isPresent()) {
                    limitNum = Math.min(opts.limit.get(), topK.limit.get());
                } else {
                    limitNum = topK.limit.get();
                }
                topKParams = Optional.of(new Session.RemoteTopKParams(limitNum, ((DocumentLevelMetric) topKMetric).getIndex()));
            }
        }
        return topKParams;
    }

    // TODO: Move this
    public static String createRow(GroupKeySet groupKeySet, int groupKey, String term, double[] selectBuffer, String[] formatStrings) {
        final StringBuilder sb = new StringBuilder();
        final List<String> keyColumns = GroupKeySets.asList(groupKeySet, groupKey);
        for (final String k : keyColumns) {
            Session.appendGroupString(k, sb);
            sb.append('\t');
        }
        Session.appendGroupString(term, sb);
        sb.append('\t');
        Session.writeDoubleStatsWithFormatString(selectBuffer, formatStrings, sb);
        if (keyColumns.size() + selectBuffer.length > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    // TODO: Move this
    public static String createRow(GroupKeySet groupKeySet, int groupKey, long term, double[] selectBuffer, String[] formatStrings) {
        final StringBuilder sb = new StringBuilder();
        final List<String> keyColumns = GroupKeySets.asList(groupKeySet, groupKey);
        for (final String k : keyColumns) {
            Session.appendGroupString(k, sb);
            sb.append('\t');
        }
        sb.append(term).append('\t');
        Session.writeDoubleStatsWithFormatString(selectBuffer, formatStrings, sb);
        if (keyColumns.size() + selectBuffer.length > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    private Session.StringIterateCallback streamingStringCallback(final Session session, final AggregateFilter filterOrNull, final Consumer<String> out) {
        final String[] formatStrings = formFormatStrings();

        return new Session.StringIterateCallback() {
            @Override
            public void term(final String term, final long[] stats, final int group) {
                if (filterOrNull != null && !filterOrNull.allow(term, stats, group)) {
                    return;
                }
                final double[] selectBuffer = new double[selecting.size()];
                for (int i = 0; i < selecting.size(); i++) {
                    selectBuffer[i] = selecting.get(i).apply(term, stats, group);
                }
                out.accept(createRow(session.groupKeySet, group, term, selectBuffer, formatStrings));
            }

            @Override
            public boolean needSorted() {
                // TODO: investigate what is better here
                // We have several options what to do:
                // 1. Leave as is (sorted) for backward compatibility.
                // 2. Request unsorted and sort after processing
                // 3. Return to cliend in unsorted order.
                // 4. Add param so client can claim sorted or unsorted result.
                return true;
            }

            @Override
            public boolean needGroup() {
                return true;
            }

            @Override
            public boolean needStats() {
                return ((filterOrNull != null) && filterOrNull.needStats())
                        || selecting.stream().anyMatch(AggregateMetric::needStats);

            }
        };
    }

    @Nonnull
    private String[] formFormatStrings() {
        final String[] formatStrings = new String[selecting.size()];
        for (int i = 0; i < formatStrings.length; i++) {
            final Optional<String> opt = this.formatStrings.get(i);
            formatStrings[i] = opt.isPresent() ? opt.get() : null;
        }
        return formatStrings;
    }

    private Session.StringIterateCallback nonStreamingStringCallback(final Session session, final DenseInt2ObjectMap<Queue<TermSelects>> pqs, final AggregateMetric topKMetricOrNull, final AggregateFilter filterOrNull) {
        return new Session.StringIterateCallback() {
            @Override
            public void term(final String term, final long[] stats, final int group) {
                if (filterOrNull != null && !filterOrNull.allow(term, stats, group)) {
                    return;
                }
                final double[] selectBuffer = new double[selecting.size()];
                final double value;
                if (topKMetricOrNull != null) {
                    value = topKMetricOrNull.apply(term, stats, group);
                } else {
                    value = 0.0;
                }
                for (int i = 0; i < selecting.size(); i++) {
                    selectBuffer[i] = selecting.get(i).apply(term, stats, group);
                }
                final Queue<TermSelects> pq = pqs.get(group);
                if (pq instanceof BoundedPriorityQueue)  {
                    if (((BoundedPriorityQueue<TermSelects>) pq).isFull()) {
                        pq.offer(new TermSelects(field, false, term, 0L, selectBuffer, value, group));
                        return ;
                    }
                }
                if (!pq.offer(new TermSelects(field, false, term, 0L, selectBuffer, value, group))) {
                    return ;
                }
                ++createdGroupCount;
                session.checkGroupLimitWithoutLog(createdGroupCount);
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
                return ((filterOrNull != null) && filterOrNull.needStats())
                        || ((topKMetricOrNull != null) && topKMetricOrNull.needStats())
                        || selecting.stream().anyMatch(AggregateMetric::needStats);
            }
        };
    }

    private Session.IntIterateCallback streamingIntCallback(final Session session, final AggregateFilter filterOrNull, final Consumer<String> out) {
        final String[] formatStrings = formFormatStrings();
        return new Session.IntIterateCallback() {
            @Override
            public void term(final long term, final long[] stats, final int group) {
                if (filterOrNull != null && !filterOrNull.allow(term, stats, group)) {
                    return;
                }
                final double[] selectBuffer = new double[selecting.size()];
                for (int i = 0; i < selecting.size(); i++) {
                    selectBuffer[i] = selecting.get(i).apply(term, stats, group);
                }
                out.accept(createRow(session.groupKeySet, group, term, selectBuffer, formatStrings));
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
                return ((filterOrNull != null) && filterOrNull.needStats())
                        || selecting.stream().anyMatch(AggregateMetric::needStats);
            }
        };
    }

    private Session.IntIterateCallback nonStreamingIntCallback(final Session session, final DenseInt2ObjectMap<Queue<TermSelects>> pqs, final AggregateMetric topKMetricOrNull, final AggregateFilter filterOrNull) {
        return new Session.IntIterateCallback() {
            @Override
            public void term(final long term, final long[] stats, final int group) {
                if (filterOrNull != null && !filterOrNull.allow(term, stats, group)) {
                    return;
                }
                final double[] selectBuffer = new double[selecting.size()];
                final double value;
                if (topKMetricOrNull != null) {
                    value = topKMetricOrNull.apply(term, stats, group);
                } else {
                    value = 0.0;
                }
                for (int i = 0; i < selecting.size(); i++) {
                    selectBuffer[i] = selecting.get(i).apply(term, stats, group);
                }
                final Queue<TermSelects> pq = pqs.get(group);
                if (pq instanceof BoundedPriorityQueue)  {
                    if (((BoundedPriorityQueue<TermSelects>) pq).isFull()) {
                        pq.offer(new TermSelects(field, true, null, term, selectBuffer, value, group));
                        return ;
                    }
                }
                if (!pq.offer(new TermSelects(field, true, null, term, selectBuffer, value, group))) {
                    return ;
                }
                ++createdGroupCount;
                session.checkGroupLimitWithoutLog(createdGroupCount);
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
                return ((filterOrNull != null) && filterOrNull.needStats())
                        || ((topKMetricOrNull != null) && topKMetricOrNull.needStats())
                        || selecting.stream().anyMatch(AggregateMetric::needStats);

            }
        };
    }
}
