package com.indeed.squall.jql.commands;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.jql.AggregateFilter;
import com.indeed.squall.jql.QualifiedPush;
import com.indeed.squall.jql.Session;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class GetGroupDistincts {
    public final Set<String> scope;
    public final String field;
    public final Optional<AggregateFilter> filter;
    public final int windowSize;

    public GetGroupDistincts(Set<String> scope, String field, Optional<AggregateFilter> filter, int windowSize) {
        this.scope = scope;
        this.field = field;
        this.filter = filter;
        this.windowSize = windowSize;
    }

    public static long[] getGroupDistincts(final GetGroupDistincts getGroupDistincts, Session session) throws ImhotepOutOfMemoryException, IOException {
        final String field = getGroupDistincts.field;
        final Map<String, ImhotepSession> sessionsSubset = Maps.newHashMap();
        getGroupDistincts.scope.forEach(s -> sessionsSubset.put(s, session.sessions.get(s).session));
        final List<AggregateFilter> filters = Lists.newArrayList();
        getGroupDistincts.filter.ifPresent(filters::add);
        final Set<QualifiedPush> pushes = Sets.newHashSet();
        filters.forEach(f -> pushes.addAll(f.requires()));
        final Map<QualifiedPush, Integer> metricIndexes = Maps.newHashMap();
        final Map<String, IntList> sessionMetricIndexes = Maps.newHashMap();
        session.pushMetrics(pushes, metricIndexes, sessionMetricIndexes);
        session.registerMetrics(metricIndexes, Arrays.asList(), filters);
        final long[] groupCounts = new long[session.numGroups];
        if (session.isIntField(field)) {
            final Session.IntIterateCallback callback = new Session.IntIterateCallback() {
                private final BitSet groupSeen = new BitSet();
                private boolean started = false;
                private int lastGroup = 0;
                private long currentTerm = 0;

                @Override
                public void term(long term, long[] stats, int group) {
                    if (started && currentTerm != term) {
                        while ((lastGroup = groupSeen.nextSetBit(lastGroup + 1)) != -1) {
                            groupCounts[lastGroup - 1]++;
                        }
                        groupSeen.clear();
                    }
                    currentTerm = term;
                    started = true;
                    lastGroup = group;
                    final Session.GroupKey parent = session.groupKeys.get(group).parent;
                    if (getGroupDistincts.filter.isPresent()) {
                        if (getGroupDistincts.filter.get().allow(term, stats, group)) {
                            for (int offset = 0; offset < getGroupDistincts.windowSize; offset++) {
                                if (group + offset < session.groupKeys.size() && session.groupKeys.get(group + offset).parent == parent) {
                                    groupSeen.set(group + offset);
                                }
                            }
                        }
                    } else {
                        for (int offset = 0; offset < getGroupDistincts.windowSize; offset++) {
                            if (group + offset < session.groupKeys.size() && session.groupKeys.get(group + offset).parent == parent) {
                                groupSeen.set(group + offset);
                            }
                        }
                    }
                    if (groupSeen.get(group)) {
                        groupCounts[group - 1]++;
                    }
                }
            };
            Session.iterateMultiInt(sessionsSubset, sessionMetricIndexes, field, callback);
        } else if (session.isStringField(field)) {
            final Session.StringIterateCallback callback = new Session.StringIterateCallback() {
                private final BitSet groupSeen = new BitSet();
                private boolean started = false;
                private int lastGroup = 0;
                private String currentTerm;

                @Override
                public void term(String term, long[] stats, int group) {
                    if (started && !currentTerm.equals(term)) {
                        while ((lastGroup = groupSeen.nextSetBit(lastGroup + 1)) != -1) {
                            groupCounts[lastGroup - 1]++;
                        }
                        groupSeen.clear();
                    }
                    currentTerm = term;
                    started = true;
                    lastGroup = group;
                    final Session.GroupKey parent = session.groupKeys.get(group).parent;
                    if (getGroupDistincts.filter.isPresent()) {
                        if (getGroupDistincts.filter.get().allow(term, stats, group)) {
                            for (int offset = 0; offset < getGroupDistincts.windowSize; offset++) {
                                if (group + offset < session.groupKeys.size() && session.groupKeys.get(group + offset).parent == parent) {
                                    groupSeen.set(group + offset);
                                }
                            }
                        }
                    } else {
                        for (int offset = 0; offset < getGroupDistincts.windowSize; offset++) {
                            if (group + offset < session.groupKeys.size() && session.groupKeys.get(group + offset).parent == parent) {
                                groupSeen.set(group + offset);
                            }
                        }
                    }
                    if (groupSeen.get(group)) {
                        groupCounts[group - 1]++;
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
        session.sessions.values().forEach(s -> {
            while (s.session.getNumStats() > 0) {
                s.session.popStat();
            }
        });
        return groupCounts;
    }
}
