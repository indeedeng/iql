package com.indeed.squall.jql.commands;

import com.google.common.collect.Lists;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.jql.AggregateMetric;
import com.indeed.squall.jql.QualifiedPush;
import com.indeed.squall.jql.Session;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExplodeByAggregatePercentile {
    public final String field;
    public final AggregateMetric metric;
    public final int numBuckets;

    public ExplodeByAggregatePercentile(String field, AggregateMetric metric, int numBuckets) {
        this.field = field;
        this.metric = metric;
        this.numBuckets = numBuckets;
    }

    public static void explodeByAggregatePercentile(ExplodeByAggregatePercentile explodeCommand, Session session) throws ImhotepOutOfMemoryException, IOException {
        final String field = explodeCommand.field;
        final AggregateMetric metric = explodeCommand.metric;
        final int numBuckets = explodeCommand.numBuckets;
        final HashMap<QualifiedPush, Integer> metricIndexes = new HashMap<>();
        final HashMap<String, IntList> sessionMetricIndexes = new HashMap<>();
        session.pushMetrics(metric.requires(), metricIndexes, sessionMetricIndexes);
        metric.register(metricIndexes, session.groupKeys);

        final List<Session.GroupKey> nextGroupKeys = Lists.newArrayListWithCapacity(1 + session.numGroups * numBuckets);
        nextGroupKeys.add(null);

        if (session.isIntField(field)) {
            final Int2ObjectOpenHashMap<Long2DoubleOpenHashMap> perGroupTermToValue = new Int2ObjectOpenHashMap<>();
            Session.iterateMultiInt(session.getSessionsMapRaw(), sessionMetricIndexes, field,
                    (term, stats, group) -> perGroupTermToValue.computeIfAbsent(group, g -> new Long2DoubleOpenHashMap()).put(term, metric.apply(term, stats, group))
            );
            final List<GroupMultiRemapRule> rules = Lists.newArrayListWithCapacity(session.numGroups);
            for (int group = 1; group <= session.numGroups; group++) {
                final int groupBase = 1 + (group - 1) * numBuckets;
                final Long2DoubleOpenHashMap termToValue = perGroupTermToValue.get(group);
                final double[] percentiles = Session.getPercentiles(termToValue.values(), numBuckets);
                final Int2ObjectOpenHashMap<LongArrayList> groupOffsetToTerms = new Int2ObjectOpenHashMap<>();
                for (final Long2DoubleMap.Entry entry : termToValue.long2DoubleEntrySet()) {
                    final int groupOffset = session.findPercentile(entry.getDoubleValue(), percentiles);
                    groupOffsetToTerms.computeIfAbsent(groupOffset, k -> new LongArrayList()).add(entry.getLongKey());
                }

                final int arraySize = groupOffsetToTerms.values().stream().mapToInt(LongArrayList::size).sum();
                final int[] positiveGroups = new int[arraySize];
                final RegroupCondition[] conditions = new RegroupCondition[arraySize];
                int arrayIndex = 0;
                for (int i = 0; i < numBuckets; i++) {
                    final LongArrayList terms = groupOffsetToTerms.get(i);
                    if (terms != null) {
                        for (final long term : terms) {
                            positiveGroups[arrayIndex] = groupBase + i;
                            conditions[arrayIndex] = new RegroupCondition(field, true, term, null, false);
                            arrayIndex++;
                        }
                    }
                    final String keyTerm = "[" + (double) i / numBuckets + ", " + (double) (i + 1) / numBuckets + ")";
                    nextGroupKeys.add(new Session.GroupKey(keyTerm, nextGroupKeys.size(), session.groupKeys.get(group)));
                }
                rules.add(new GroupMultiRemapRule(group, 0, positiveGroups, conditions));
            }
            final GroupMultiRemapRule[] rulesArr = rules.toArray(new GroupMultiRemapRule[rules.size()]);
            session.sessions.values().forEach(s -> Session.unchecked(() -> s.session.regroup(rulesArr)));
        } else if (session.isStringField(field)) {
            final Int2ObjectOpenHashMap<Object2DoubleOpenHashMap<String>> perGroupTermToValue = new Int2ObjectOpenHashMap<>();
            Session.iterateMultiString(session.getSessionsMapRaw(), sessionMetricIndexes, field,
                    (term, stats, group) -> perGroupTermToValue.computeIfAbsent(group, g -> new Object2DoubleOpenHashMap<>()).put(term, metric.apply(term, stats, group))
            );
            final List<GroupMultiRemapRule> rules = Lists.newArrayListWithCapacity(session.numGroups);
            for (int group = 1; group <= session.numGroups; group++) {
                final int groupBase = 1 + (group - 1) * numBuckets;
                final Object2DoubleOpenHashMap<String> termToValue = perGroupTermToValue.get(group);
                final double[] percentiles = Session.getPercentiles(termToValue.values(), numBuckets);
                final Int2ObjectOpenHashMap<ArrayList<String>> groupOffsetToTerms = new Int2ObjectOpenHashMap<>();
                for (final Map.Entry<String, Double> entry : termToValue.entrySet()) {
                    final int groupOffset = session.findPercentile(entry.getValue(), percentiles);
                    groupOffsetToTerms.computeIfAbsent(groupOffset, k -> new ArrayList<>()).add(entry.getKey());
                }

                final int arraySize = groupOffsetToTerms.values().stream().mapToInt(ArrayList::size).sum();
                final int[] positiveGroups = new int[arraySize];
                final RegroupCondition[] conditions = new RegroupCondition[arraySize];
                int arrayIndex = 0;
                for (int i = 0; i < numBuckets; i++) {
                    final ArrayList<String> terms = groupOffsetToTerms.get(i);
                    if (terms != null) {
                        for (final String term : terms) {
                            positiveGroups[arrayIndex] = groupBase + i;
                            conditions[arrayIndex] = new RegroupCondition(field, false, 0, term, false);
                            arrayIndex++;
                        }
                    }
                    final String keyTerm = "[" + (double) i / numBuckets + ", " + (double) (i + 1) / numBuckets + ")";
                    nextGroupKeys.add(new Session.GroupKey(keyTerm, nextGroupKeys.size(), session.groupKeys.get(group)));
                }
                rules.add(new GroupMultiRemapRule(group, 0, positiveGroups, conditions));
            }
            final GroupMultiRemapRule[] rulesArr = rules.toArray(new GroupMultiRemapRule[rules.size()]);
            session.sessions.values().forEach(s -> Session.unchecked(() -> s.session.regroup(rulesArr)));
        } else {
            throw new IllegalArgumentException("Field is neither int field nor string field: " + field);
        }
        session.sessions.values().forEach(v -> Session.unchecked(() -> {
            while (v.session.getNumStats() > 0) {
                v.session.popStat();
            }
        }));

        session.numGroups = nextGroupKeys.size() - 1;
        session.groupKeys = nextGroupKeys;
        session.currentDepth += 1;
    }
}
