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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.common.datastruct.BoundedPriorityQueue;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.api.FTGAIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.metrics.aggregate.AggregateStatTree;
import com.indeed.iql2.execution.AggregateFilter;
import com.indeed.iql2.execution.ImhotepSessionHolder;
import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.QueryOptions;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.TermSelects;
import com.indeed.iql2.execution.commands.misc.FieldIterateOpts;
import com.indeed.iql2.execution.commands.misc.TopK;
import com.indeed.iql2.execution.groupkeys.GroupKeySets;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.iql2.execution.metrics.aggregate.DocumentLevelMetric;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import it.unimi.dsi.fastutil.ints.IntList;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class SimpleIterate implements Command {
    public final FieldSet field;
    public final FieldIterateOpts opts;
    public final List<AggregateMetric> selecting;
    public final List<Optional<String>> formatStrings;
    public final boolean streamResult;

    private int createdGroupCount = 0;

    public SimpleIterate(
            final FieldSet field,
            final FieldIterateOpts opts,
            final List<AggregateMetric> selecting,
            final List<Optional<String>> formatStrings,
            final boolean streamResult) {
        this.field = field;
        this.opts = opts;
        this.selecting = selecting;
        this.formatStrings = formatStrings;
        this.streamResult = streamResult;
        if (this.streamResult && opts.topK.isPresent()) {
            throw new IllegalArgumentException("Can't stream results while doing top-k!");
        }
    }

    @Override
    public void execute(final Session session) {
        // this Command needs special processing since it returns some data.
        throw new IllegalStateException("Call evaluate() method instead");
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

    // evaluate results to memory
    public static void evaluate(
            final Session session,
            final FieldSet field,
            final List<AggregateMetric> selecting,
            final FieldIterateOpts fieldOpts,
            final ResultCollector out) throws ImhotepOutOfMemoryException, IOException {
        new SimpleIterate(
                field,
                fieldOpts,
                selecting,
                Collections.nCopies(selecting.size(), Optional.absent()),
                !fieldOpts.topK.isPresent())
                .evaluate(session, out);
    }

    public void evaluate(
            final Session session,
            final ResultCollector out) throws ImhotepOutOfMemoryException, IOException {
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
            evaluateMultiFtgs(session, out, allPushes, topKMetricOrNull);
            return;
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
        final ResultCollector collector;
        if (streamResult) {
            collector = out;
        } else if (opts.topK.isPresent()) {
            if (opts.topK.get().limit.isPresent()) {
                final int limit = opts.topK.get().limit.get();
                collector = ResultCollector.topKCollector(out, session.numGroups, limit, TermSelects.COMPARATOR);
            } else {
                final Comparator<TermSelects> comparator = TermSelects.COMPARATOR.reversed();
                collector = ResultCollector.allTermsCollector(out, session.numGroups, comparator);
            }
        } else {
            collector = ResultCollector.allTermsCollector(out, session.numGroups, null);
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

        final Map<String, ImhotepSessionHolder> sessionsToUse = session.getSessionsMapRaw();

        final Optional<Session.RemoteTopKParams> topKParams;
        if (sessionsToUse.size() > 1) {
            // if there are multiple datasets, topK must be calculated on a client side.
            topKParams = Optional.absent();
        } else {
            topKParams = getTopKParamsOptional(opts);
        }

        session.timer.pop();


        if (session.isIntField(field)) {
            final Session.IntIterateCallback callback = intCallback(session, collector, topKMetricOrNull, filterOrNull);
            session.timer.push("iterateMultiInt");
            Session.iterateMultiInt(sessionsToUse, sessionMetricIndexes, Collections.<String, Integer>emptyMap(), field, topKParams, ftgsRowLimit, opts.sortedIntTermSubset, callback, session.timer, session.options);
            session.timer.pop();
        } else if (session.isStringField(field)) {
            final Session.StringIterateCallback callback = stringCallback(session, collector, topKMetricOrNull, filterOrNull);
            session.timer.push("iterateMultiString");
            Session.iterateMultiString(sessionsToUse, sessionMetricIndexes, Collections.<String, Integer>emptyMap(), field, topKParams, ftgsRowLimit, opts.sortedStringTermSubset, callback, session.timer, session.options);
            session.timer.pop();
        } else {
            throw new IllegalArgumentException("Field is neither all int nor all string field: " + field);
        }

        session.timer.push("ResultCollector.finish()");
        collector.finish();
        session.timer.pop();

        session.popStats();
    }

    private void evaluateMultiFtgs(
            final Session session,
            final ResultCollector out,
            final Set<QualifiedPush> allPushes,
            final AggregateMetric topKMetricOrNull) throws ImhotepOutOfMemoryException {
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
        for (final Map.Entry<String, Session.ImhotepSessionInfo> entry : session.sessions.entrySet()) {
            final ImhotepSessionHolder sessionHolder = entry.getValue().session;
            final String dataset = sessionHolder.getDatasetName();
            sessionFields.add(sessionHolder.buildSessionField(field.datasetFieldName(dataset)));
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

            final ResultCollector collector;

            if (streamResult) {
                collector = out;
            } else {
                final TermsAccumulator[] result = new TermsAccumulator[session.numGroups+1];
                final Comparator<TermSelects> comparator = TermSelects.COMPARATOR.reversed();
                for (int group = 0; group <= session.numGroups; group++) {
                    result[group] = new TermsAccumulator.ArrayAccumulator(comparator);
                }
                collector = new ResultCollector.PerGroupCollector(result, out);
            }

            Preconditions.checkState(iterator.nextField());
            while (iterator.nextTerm()) {
                while (iterator.nextGroup()) {
                    iterator.groupStats(statsBuf);

                    if (streamResult) {
                        if (statsBuf != outputStatsBuf) {
                            System.arraycopy(statsBuf, 0, outputStatsBuf, 0, outputStatsBuf.length);
                        }
                        if (isIntField) {
                            collector.offer(iterator.group(), iterator.termIntVal(), outputStatsBuf, 0.0);
                        } else {
                            collector.offer(iterator.group(), iterator.termStringVal(), outputStatsBuf, 0.0);
                        }
                    } else {
                        if (isIntField) {
                            collector.offer(
                                    iterator.group(),
                                    iterator.termIntVal(),
                                    Arrays.copyOf(statsBuf, numSelects),
                                    topKMetricOrNull == null ? 0.0 : statsBuf[sortStat]
                            );
                        } else {
                            collector.offer(
                                    iterator.group(),
                                    iterator.termStringVal(),
                                    Arrays.copyOf(statsBuf, numSelects),
                                    topKMetricOrNull == null ? 0.0 : statsBuf[sortStat]
                            );
                        }

                        createdGroupCount += 1;
                        session.checkGroupLimitWithoutLog(createdGroupCount);
                    }
                }
            }
            Preconditions.checkState(!iterator.nextField());
            session.timer.pop();

            session.timer.push("ResultCollector.finish()");
            collector.finish();
            session.timer.pop();

            session.popStats();
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

    @Nonnull
    // TODO: move this.
    public String[] formFormatStrings() {
        final String[] formatStrings = new String[selecting.size()];
        for (int i = 0; i < formatStrings.length; i++) {
            final Optional<String> opt = this.formatStrings.get(i);
            formatStrings[i] = opt.isPresent() ? opt.get() : null;
        }
        return formatStrings;
    }

    private Session.StringIterateCallback stringCallback(
            final Session session,
            final ResultCollector result,
            final AggregateMetric topKMetricOrNull,
            final AggregateFilter filterOrNull) {
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
                if (result.offer(group, term, selectBuffer, value)) {
                    ++createdGroupCount;
                    session.checkGroupLimitWithoutLog(createdGroupCount);
                }
            }

            @Override
            public boolean needSorted() {
                // TODO: investigate what is better here
                // We have several options what to do:
                // 1. Leave as is (sorted) for backward compatibility.
                // 2. Request unsorted and sort after processing
                // 3. Return to client in unsorted order.
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
                        || ((topKMetricOrNull != null) && topKMetricOrNull.needStats())
                        || selecting.stream().anyMatch(AggregateMetric::needStats);
            }
        };
    }

    private Session.IntIterateCallback intCallback(
            final Session session,
            final ResultCollector result,
            final AggregateMetric topKMetricOrNull,
            final AggregateFilter filterOrNull) {
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
                if (result.offer(group, term, selectBuffer, value)) {
                    ++createdGroupCount;
                    session.checkGroupLimitWithoutLog(createdGroupCount);
                }
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

    // interface for adding new terms to groups.
    public interface ResultCollector {

        // offer methods are expected to be called in sorted order,
        // all TermSelects for group 1, then group 2, etc.
        boolean needSortedByGroup();

        // return true iff new group was added
        boolean offer(final int group, final TermSelects termSelects);

        // finalize work. In topK scenario here all gathered data will be passed further.
        void finish();

        default boolean offer(
                final int group,
                final long intTerm,
                final double[] selects,
                final double topMetric) {
            return offer(group, new TermSelects(intTerm, selects, topMetric));
        }

        default boolean offer(
                final int group,
                final String stringTerm,
                final double[] selects,
                final double topMetric) {
            return offer(group, new TermSelects(stringTerm, selects, topMetric));
        }

        default int offer(final int group, final Collection<TermSelects> termSelects) {
            int result = 0;
            for (final TermSelects term : termSelects) {
                if (offer(group, term)) {
                    result++;
                }
            }
            return result;
        }

        class PerGroupCollector implements ResultCollector {

            final TermsAccumulator[] accumulators; // per group accumulators.
            final ResultCollector out;

            PerGroupCollector(
                    final TermsAccumulator[] accumulators,
                    final ResultCollector out) {
                this.accumulators = accumulators;
                this.out = out;
            }

            @Override
            public boolean needSortedByGroup() {
                return false;
            }

            @Override
            public boolean offer(final int group, final TermSelects termSelects) {
                return accumulators[group].addTerm(termSelects);
            }

            @Override
            public void finish() {
                for (int group = 0; group < accumulators.length; group++) {
                    if (accumulators[group] == null) {
                        continue;
                    }
                    final List<TermSelects> results = accumulators[group].getResult();
                    out.offer(group, results);
                }
                out.finish();
            }
        }

        class Streaming implements ResultCollector {

            final Consumer<String> out;
            final GroupKeySet groupKeySet;
            final String[] formatStrings;

            public Streaming(
                    final Consumer<String> out,
                    final GroupKeySet groupKeySet,
                    final String[] formatStrings) {
                this.out = out;
                this.groupKeySet = groupKeySet;
                this.formatStrings = formatStrings;
            }

            @Override
            public boolean needSortedByGroup() {
                // TODO: might this be 'false' sometimes?
                return true;
            }

            @Override
            public boolean offer(final int group, final TermSelects termSelects) {
                if (groupKeySet.isPresent(group)) {
                    if (termSelects.stringTerm == null) {
                        out.accept(createRow(groupKeySet, group, termSelects.intTerm, termSelects.selects, formatStrings));
                    } else {
                        out.accept(createRow(groupKeySet, group, termSelects.stringTerm, termSelects.selects, formatStrings));
                    }
                }
                // we could stream forever so pretend nothing have changed.
                return false;
            }

            @Override
            public int offer(final int group, final Collection<TermSelects> termSelects) {
                ResultCollector.super.offer(group, termSelects);
                // we could stream forever so pretend nothing have changed.
                return 0;
            }

            @Override
            public void finish() {
            }
        }

        static ResultCollector topKCollector(
                final ResultCollector prev,
                final int numGroups,
                final int limit,
                final Comparator<TermSelects> comparator) {
            final TermsAccumulator[] pqs = new TermsAccumulator[numGroups + 1];
            for (int i = 1; i <= numGroups; i++) {
                pqs[i] = new TermsAccumulator.BoundedPriorityQueueAccumulator(limit, comparator);
            }
            return new ResultCollector.PerGroupCollector(pqs, prev);
        }

        static ResultCollector allTermsCollector(
                final ResultCollector prev,
                final int numGroups,
                final Comparator<TermSelects> comparator) {
            if ((comparator == null) && !prev.needSortedByGroup()) {
                return prev;
            }
            final TermsAccumulator[] pqs = new TermsAccumulator[numGroups + 1];
            for (int i = 1; i <= numGroups; i++) {
                pqs[i] = new TermsAccumulator.ArrayAccumulator(comparator);
            }
            return new ResultCollector.PerGroupCollector(pqs, prev);
        }
    }

    private interface TermsAccumulator {

        // offer a term. Returns true iff new group is created.
        boolean addTerm(final TermSelects termSelects);

        // get all accumulated terms
        List<TermSelects> getResult();

        class BoundedPriorityQueueAccumulator implements TermsAccumulator {

            private final BoundedPriorityQueue<TermSelects> queue;

            public BoundedPriorityQueueAccumulator(final int maxCapacity, final Comparator<TermSelects> comparator) {
                queue = BoundedPriorityQueue.newInstance(maxCapacity, comparator);
            }

            @Override
            public boolean addTerm(final TermSelects termSelects) {
                final boolean wasFool = queue.isFull();
                return queue.offer(termSelects) && !wasFool;
            }

            @Override
            public List<TermSelects> getResult() {
                final List<TermSelects> listTermSelects = new ArrayList<>(queue.size());
                while (!queue.isEmpty()) {
                    listTermSelects.add(queue.poll());
                }
                return Lists.reverse(listTermSelects);
            }
        }

        class ArrayAccumulator implements TermsAccumulator {
            private final ArrayList<TermSelects> queue;
            private final Comparator<TermSelects> comparator;

            public ArrayAccumulator(final Comparator<TermSelects> comparator) {
                queue = new ArrayList<>();
                this.comparator = comparator;
            }

            @Override
            public boolean addTerm(final TermSelects termSelects) {
                queue.add(termSelects);
                return true;
            }

            @Override
            public List<TermSelects> getResult() {
                if (comparator != null) {
                    queue.sort(comparator);
                }
                return queue;
            }
        }
    }
}
