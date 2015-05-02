package com.indeed.squall.jql.commands;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.jql.AggregateFilter;
import com.indeed.squall.jql.Pushable;
import com.indeed.squall.jql.QualifiedPush;
import com.indeed.squall.jql.Session;
import com.indeed.squall.jql.metrics.aggregate.AggregateMetric;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.stream.Collectors.toMap;

public class SumAcross {
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

    public static Closeable pushAndRegister(Session session, Map<QualifiedPush, Integer> metricIndexes, Map<String, IntList> sessionMetricIndexes, List<Session.GroupKey> groupKeys, Iterable<? extends Pushable> pushables) throws ImhotepOutOfMemoryException {
        final Set<QualifiedPush> allPushes = Sets.newHashSet();
        pushables.forEach(x -> allPushes.addAll(x.requires()));
        session.pushMetrics(allPushes, metricIndexes, sessionMetricIndexes);
        pushables.forEach(x -> x.register(metricIndexes, groupKeys));
        return new Closeable() {
            @Override
            public void close() {
                session.getSessionsMapRaw().values().forEach(s -> {
                    while (s.getNumStats() > 0) {
                        s.popStat();
                    }
                });
            }
        };
    }

    public double[] execute(Session session) throws ImhotepOutOfMemoryException, IOException {
        final Map<String, ImhotepSession> sessionsSubset = scope.stream().collect(toMap(s -> s, s -> session.sessions.get(s).session));
        final List<Pushable> pushables = Lists.newArrayList(metric);
        filter.ifPresent(pushables::add);
        final HashMap<QualifiedPush, Integer> metricIndexes = Maps.newHashMap();
        final HashMap<String, IntList> sessionMetricIndexes = Maps.newHashMap();
        final double[] groupSums = new double[session.numGroups];
        try (Closeable ignored = pushAndRegister(session, metricIndexes, sessionMetricIndexes, session.groupKeys, pushables)) {
            if (session.isIntField(field)) {
                final Session.IntIterateCallback callback = new Session.IntIterateCallback() {
                    @Override
                    public void term(long term, long[] stats, int group) {
                        final double v = metric.apply(term, stats, group);
                        if (filter.isPresent()) {
                            if (filter.get().allow(term, stats, group)) {
                                groupSums[group - 1] += v;
                            }
                        } else {
                            groupSums[group - 1] += v;
                        }
                    }
                };
                Session.iterateMultiInt(sessionsSubset, sessionMetricIndexes, field, callback);
            } else if (session.isStringField(field)) {
                final Session.StringIterateCallback callback = new Session.StringIterateCallback() {
                    @Override
                    public void term(String term, long[] stats, int group) {
                        final double v = metric.apply(term, stats, group);
                        if (filter.isPresent()) {
                            if (filter.get().allow(term, stats, group)) {
                                groupSums[group - 1] += v;
                            }
                        } else {
                            groupSums[group - 1] += v;
                        }
                    }
                };
                Session.iterateMultiString(sessionsSubset, sessionMetricIndexes, field, callback);
            } else {
                for (final Map.Entry<String, Session.ImhotepSessionInfo> s : session.sessions.entrySet()) {
                    final String name = s.getKey();
                    final boolean isIntField = s.getValue().intFields.contains(field);
                    final boolean isStringField = s.getValue().stringFields.contains(field);
                    System.out.println("name = " + name + ", isIntField=" + isIntField + ", isStringField=" + isStringField);
                }
                throw new IllegalStateException("Field is neither all int nor all string field: " + field);
            }
        }
        return groupSums;
    }
}
