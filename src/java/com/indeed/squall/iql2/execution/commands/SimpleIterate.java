package com.indeed.squall.iql2.execution.commands;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import com.indeed.common.datastruct.BoundedPriorityQueue;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.AggregateFilter;
import com.indeed.squall.iql2.execution.DenseInt2ObjectMap;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.TermSelects;
import com.indeed.squall.iql2.execution.commands.misc.FieldIterateOpts;
import com.indeed.squall.iql2.execution.commands.misc.TopK;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.GroupKeySets;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.squall.iql2.execution.metrics.aggregate.DocumentLevelMetric;
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

public class SimpleIterate implements Command {
    public final String field;
    public final FieldIterateOpts opts;
    public final List<AggregateMetric> selecting;
    public final List<Optional<String>> formatStrings;
    public final boolean streamResult;
    @Nullable
    public final Set<String> scope;

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

    public List<List<List<TermSelects>>> evaluate(final Session session, @Nullable Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        session.timer.push("push and register metrics");
        final Set<QualifiedPush> allPushes = Sets.newHashSet();
        final List<AggregateMetric> metrics = Lists.newArrayList();
        if (opts.topK.isPresent()) {
            final TopK topK = opts.topK.get();
            if (topK.metric.isPresent()) {
                metrics.add(topK.metric.get());
            }
        }
        metrics.addAll(this.selecting);
        for (final AggregateMetric metric : metrics) {
            allPushes.addAll(metric.requires());
        }
        if (opts.filter.isPresent()) {
            allPushes.addAll(opts.filter.get().requires());
        }
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
            final Comparator<TermSelects> comparator = new Comparator<TermSelects>() {
                @Override
                public int compare(TermSelects o1, TermSelects o2) {
                    final double v1 = Double.isNaN(o1.topMetric) ? Double.NEGATIVE_INFINITY : o1.topMetric;
                    final double v2 = Double.isNaN(o2.topMetric) ? Double.NEGATIVE_INFINITY : o2.topMetric;

                    int r = Doubles.compare(v1, v2);
                    if (r != 0) {
                        return r;
                    }

                    if (o1.isIntTerm) {
                        r = Longs.compare(o1.intTerm, o2.intTerm);
                    } else {
                        r = o1.stringTerm.compareTo(o2.stringTerm);
                    }

                    return r;
                }
            };
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
        final Optional<Integer> rowLimit;
        final AggregateMetric topKMetricOrNull;
        if (opts.topK.isPresent()) {
            final TopK topK = opts.topK.get();
            if (topK.metric.isPresent()) {
                topKMetricOrNull = topK.metric.get();
                topKMetricOrNull.register(metricIndexes, session.groupKeySet);
            } else {
                topKMetricOrNull = null;
            }
            rowLimit = Optional.absent();
        } else {
            topKMetricOrNull = null;
            rowLimit = opts.limit;
        }
        final AggregateFilter filterOrNull = opts.filter.orNull();
        final Optional<Session.RemoteTopKParams> topKParams = getTopKParamsOptional();
        session.timer.pop();

        final Map<String, ImhotepSession> sessionsMapRaw = session.getSessionsMapRaw();
        final Map<String, ImhotepSession> sessionsToUse;
        if (scope == null) {
            sessionsToUse = sessionsMapRaw;
        } else {
            sessionsToUse = Maps.newHashMap();
            for (final String dataset : scope) {
                sessionsToUse.put(dataset, sessionsMapRaw.get(dataset));
            }
        }

        if (session.isIntField(field)) {
            final Session.IntIterateCallback callback;
            if (streamResult) {
                callback = streamingIntCallback(session, filterOrNull, out);
            } else {
                callback = nonStreamingIntCallback(session, pqs, topKMetricOrNull, filterOrNull);
            }
            session.timer.push("iterateMultiInt");
            Session.iterateMultiInt(sessionsToUse, sessionMetricIndexes, Collections.<String, Integer>emptyMap(), field, topKParams, rowLimit, callback);
            session.timer.pop();
        } else if (session.isStringField(field)) {
            final Session.StringIterateCallback callback;
            if (streamResult) {
                callback = streamingStringCallback(session, filterOrNull, out);
            } else {
                callback = nonStreamingStringCallback(session, pqs, topKMetricOrNull, filterOrNull);
            }
            session.timer.push("iterateMultiString");
            Session.iterateMultiString(sessionsToUse, sessionMetricIndexes, Collections.<String, Integer>emptyMap(), field, topKParams, rowLimit, callback);
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

    private Optional<Session.RemoteTopKParams> getTopKParamsOptional() {
        Optional<Session.RemoteTopKParams> topKParams = Optional.absent();
        if (opts.topK.isPresent()) {
            final TopK topK = opts.topK.get();
            if (topK.metric.isPresent()) {
                if (topK.limit.isPresent()) {
                    final AggregateMetric topKMetric = opts.topK.get().metric.get();
                    if (topKMetric instanceof DocumentLevelMetric) {
                        topKParams = Optional.of(new Session.RemoteTopKParams(topK.limit.get(), ((DocumentLevelMetric) topKMetric).getIndex()));
                    }
                }
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
        for (int i = 0; i < selectBuffer.length; i++) {
            final double stat = selectBuffer[i];
            if (formatStrings[i] != null) {
                sb.append(String.format(formatStrings[i], stat)).append('\t');
            } else if (DoubleMath.isMathematicalInteger(stat)) {
                sb.append((long) stat).append('\t');
            } else {
                sb.append(stat).append('\t');
            }
        }
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
        for (int i = 0; i < selectBuffer.length; i++) {
            final double stat = selectBuffer[i];
            if (i < formatStrings.length && formatStrings[i] != null) {
                sb.append(String.format(formatStrings[i], stat)).append('\t');
            } else if (DoubleMath.isMathematicalInteger(stat)) {
                sb.append((long) stat).append('\t');
            } else {
                sb.append(stat).append('\t');
            }
        }
        if (keyColumns.size() + selectBuffer.length > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    private Session.StringIterateCallback streamingStringCallback(final Session session, final AggregateFilter filterOrNull, final Consumer<String> out) {
        final String[] formatStrings = formFormatStrings();

        return new Session.StringIterateCallback() {
            @Override
            public void term(String term, long[] stats, int group) {
                if (filterOrNull != null && !filterOrNull.allow(term, stats, group)) {
                    return;
                }
                final double[] selectBuffer = new double[selecting.size()];
                for (int i = 0; i < selecting.size(); i++) {
                    selectBuffer[i] = selecting.get(i).apply(term, stats, group);
                }
                out.accept(createRow(session.groupKeySet, group, term, selectBuffer, formatStrings));
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
            public void term(String term, long[] stats, int group) {
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
                pqs.get(group).offer(new TermSelects(field, false, term, 0, selectBuffer, value, group));
            }
        };
    }

    private Session.IntIterateCallback streamingIntCallback(final Session session, final AggregateFilter filterOrNull, final Consumer<String> out) {
        final String[] formatStrings = formFormatStrings();
        return new Session.IntIterateCallback() {
            @Override
            public void term(long term, long[] stats, int group) {
                if (filterOrNull != null && !filterOrNull.allow(term, stats, group)) {
                    return;
                }
                final double[] selectBuffer = new double[selecting.size()];
                for (int i = 0; i < selecting.size(); i++) {
                    selectBuffer[i] = selecting.get(i).apply(term, stats, group);
                }
                out.accept(createRow(session.groupKeySet, group, term, selectBuffer, formatStrings));
            }
        };
    }

    private Session.IntIterateCallback nonStreamingIntCallback(final Session session, final DenseInt2ObjectMap<Queue<TermSelects>> pqs, final AggregateMetric topKMetricOrNull, final AggregateFilter filterOrNull) {
        return new Session.IntIterateCallback() {
            @Override
            public void term(long term, long[] stats, int group) {
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
                pqs.get(group).offer(new TermSelects(field, true, null, term, selectBuffer, value, group));
            }
        };
    }
}
