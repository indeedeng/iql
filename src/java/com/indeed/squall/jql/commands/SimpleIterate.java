package com.indeed.squall.jql.commands;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Doubles;
import com.indeed.common.datastruct.BoundedPriorityQueue;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.jql.AggregateFilter;
import com.indeed.squall.jql.DenseInt2ObjectMap;
import com.indeed.squall.jql.QualifiedPush;
import com.indeed.squall.jql.Session;
import com.indeed.squall.jql.TermSelects;
import com.indeed.squall.jql.compat.Consumer;
import com.indeed.squall.jql.metrics.aggregate.AggregateMetric;
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
import java.util.Queue;
import java.util.Set;

public class SimpleIterate implements Command {
    public final String field;
    public final FieldIterateOpts opts;
    public final List<AggregateMetric> selecting;
    public final boolean streamResult;

    public SimpleIterate(String field, FieldIterateOpts opts, List<AggregateMetric> selecting, boolean streamResult) {
        this.field = field;
        this.opts = opts;
        this.selecting = selecting;
        this.streamResult = streamResult;
        if (this.streamResult && opts.topK.isPresent()) {
            throw new IllegalArgumentException("Can't stream results while doing top-k!");
        }
    }

    @Override
    public void execute(Session session, @Nonnull Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final List<List<List<TermSelects>>> result = this.evaluate(session, out);
        final StringBuilder sb = new StringBuilder();
        Session.writeTermSelectsJson(result, sb);
        out.accept(Session.MAPPER.writeValueAsString(Collections.singletonList(sb.toString())));
    }

    public List<List<List<TermSelects>>> evaluate(final Session session, @Nullable Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final Set<QualifiedPush> allPushes = Sets.newHashSet();
        final List<AggregateMetric> metrics = Lists.newArrayList();
        if (opts.topK.isPresent()) {
            metrics.add(opts.topK.get().metric);
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
            opts.filter.get().register(metricIndexes, session.groupKeys);
        }

        final DenseInt2ObjectMap<Queue<TermSelects>> pqs = new DenseInt2ObjectMap<>();
        if (opts.topK.isPresent()) {
            // TODO: Share this with Iterate. Or delete Iterate.
            final Comparator<TermSelects> comparator = new Comparator<TermSelects>() {
                @Override
                public int compare(TermSelects o1, TermSelects o2) {
                    final double v1 = Double.isNaN(o1.topMetric) ? Double.NEGATIVE_INFINITY : o1.topMetric;
                    final double v2 = Double.isNaN(o2.topMetric) ? Double.NEGATIVE_INFINITY : o2.topMetric;
                    return Doubles.compare(v1, v2);
                }
            };
            for (int i = 1; i <= session.numGroups; i++) {
                // TODO: If this type changes, then a line below with an instanceof check will break.
                pqs.put(i, BoundedPriorityQueue.newInstance(opts.topK.get().limit, comparator));
            }
        } else {
            for (int i = 1; i <= session.numGroups; i++) {
                pqs.put(i, new ArrayDeque<TermSelects>());
            }
        }
        final AggregateMetric topKMetricOrNull;
        if (opts.topK.isPresent()) {
            topKMetricOrNull = opts.topK.get().metric;
            topKMetricOrNull.register(metricIndexes, session.groupKeys);
        } else {
            topKMetricOrNull = null;
        }
        final AggregateFilter filterOrNull = opts.filter.orNull();

        if (session.isIntField(field)) {
            final Session.IntIterateCallback callback;
            if (streamResult) {
                callback = streamingIntCallback(session, filterOrNull, out);
            } else {
                callback = nonStreamingIntCallback(session, pqs, topKMetricOrNull, filterOrNull);
            }
            Session.iterateMultiInt(session.getSessionsMapRaw(), sessionMetricIndexes, field, callback);
        } else if (session.isStringField(field)) {
            final Session.StringIterateCallback callback;
            if (streamResult) {
                callback = streamingStringCallback(session, filterOrNull, out);
            } else {
                callback = nonStreamingStringCallback(session, pqs, topKMetricOrNull, filterOrNull);
            }
            Session.iterateMultiString(session.getSessionsMapRaw(), sessionMetricIndexes, field, callback);
        } else {
            throw new IllegalArgumentException("Field is neither all int nor all string field: " + field);
        }

        session.popStats();

        if (streamResult) {
            out.accept("");
            return Collections.emptyList();
        } else {
            final List<List<List<TermSelects>>> allTermSelects = Lists.newArrayList();
            for (int group = 1; group <= session.numGroups; group++) {
                final List<List<TermSelects>> groupTermSelects = Lists.newArrayList();
                final Queue<TermSelects> pq = pqs.get(group);
                final List<TermSelects> listTermSelects = Lists.newArrayList();
                while (!pq.isEmpty()) {
                    listTermSelects.add(pq.poll());
                }
                // TODO: This line is very fragile
                if (pq instanceof BoundedPriorityQueue) {
                    groupTermSelects.add(Lists.reverse(listTermSelects));
                } else {
                    groupTermSelects.add(listTermSelects);
                }
                allTermSelects.add(groupTermSelects);
            }
            return allTermSelects;
        }
    }

    public static String createRow(Session.GroupKey groupKey, String term, double[] selectBuffer) {
        final StringBuilder sb = new StringBuilder();
        final List<String> keyColumns = groupKey.asList(true);
        for (final String k : keyColumns) {
            sb.append(k).append('\t');
        }
        sb.append(term).append('\t');
        for (final double stat : selectBuffer) {
            if (DoubleMath.isMathematicalInteger(stat)) {
                sb.append((long)stat).append('\t');
            } else {
                sb.append(stat).append('\t');
            }
        }
        if (keyColumns.size() + selectBuffer.length > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    public static String createRow(Session.GroupKey groupKey, long term, double[] selectBuffer) {
        final StringBuilder sb = new StringBuilder();
        final List<String> keyColumns = groupKey.asList(true);
        for (final String k : keyColumns) {
            sb.append(k).append('\t');
        }
        sb.append(term).append('\t');
        for (final double stat : selectBuffer) {
            if (DoubleMath.isMathematicalInteger(stat)) {
                sb.append((long)stat).append('\t');
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
                out.accept(createRow(session.groupKeys.get(group), term, selectBuffer));
            }
        };
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
                pqs.get(group).offer(new TermSelects(field, false, term, 0, selectBuffer, value, session.groupKeys.get(group)));
            }
        };
    }

    private Session.IntIterateCallback streamingIntCallback(final Session session, final AggregateFilter filterOrNull, final Consumer<String> out) {
        return new Session.IntIterateCallback() {
            @Override
            public void term(long term, long[] stats, int group) {
                if (filterOrNull != null && !filterOrNull.allow(term, stats, group)) {
                    return;
                }
                final double[] selectBuffer = new double[selecting.size()];
                final double value;
                for (int i = 0; i < selecting.size(); i++) {
                    selectBuffer[i] = selecting.get(i).apply(term, stats, group);
                }
                out.accept(createRow(session.groupKeys.get(group), term, selectBuffer));
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
                pqs.get(group).offer(new TermSelects(field, true, null, term, selectBuffer, value, session.groupKeys.get(group)));
            }
        };
    }
}
