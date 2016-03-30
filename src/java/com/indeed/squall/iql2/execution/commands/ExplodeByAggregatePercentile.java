package com.indeed.squall.iql2.execution.commands;

import com.google.common.collect.Lists;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.groupkeys.GroupKey;
import com.indeed.squall.iql2.execution.groupkeys.StringGroupKey;
import com.indeed.squall.iql2.execution.groupkeys.sets.DumbGroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExplodeByAggregatePercentile implements Command {
    public final String field;
    public final AggregateMetric metric;
    public final int numBuckets;

    public ExplodeByAggregatePercentile(String field, AggregateMetric metric, int numBuckets) {
        this.field = field;
        this.metric = metric;
        this.numBuckets = numBuckets;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final String field = this.field;
        final AggregateMetric metric = this.metric;
        final int numBuckets = this.numBuckets;
        final HashMap<QualifiedPush, Integer> metricIndexes = new HashMap<>();
        final HashMap<String, IntList> sessionMetricIndexes = new HashMap<>();
        session.pushMetrics(metric.requires(), metricIndexes, sessionMetricIndexes);
        metric.register(metricIndexes, session.groupKeySet);

        final List<GroupKey> nextGroupKeys = Lists.newArrayListWithCapacity(1 + session.numGroups * numBuckets);
        nextGroupKeys.add(null);
        final IntList nextGroupParents = new IntArrayList();
        nextGroupParents.add(-1);

        session.checkGroupLimit(this.numBuckets * session.numGroups);

        if (session.isIntField(field)) {
            final Int2ObjectOpenHashMap<Long2DoubleOpenHashMap> perGroupTermToValue = new Int2ObjectOpenHashMap<>();
            Session.iterateMultiInt(session.getSessionsMapRaw(), sessionMetricIndexes, Collections.<String, Integer>emptyMap(), field,
                    new Session.IntIterateCallback() {
                        @Override
                        public void term(long term, long[] stats, int group) {
                            Long2DoubleOpenHashMap termToValue = perGroupTermToValue.get(group);
                            if (termToValue == null) {
                                termToValue = new Long2DoubleOpenHashMap();
                                perGroupTermToValue.put(group, termToValue);
                            }
                            termToValue.put(term, metric.apply(term, stats, group));
                        }
                    }
            );
            final List<GroupMultiRemapRule> rules = Lists.newArrayListWithCapacity(session.numGroups);
            for (int group = 1; group <= session.numGroups; group++) {
                final int groupBase = 1 + (group - 1) * numBuckets;
                final Long2DoubleOpenHashMap termToValue = perGroupTermToValue.get(group);
                final double[] percentiles = Session.getPercentiles(termToValue.values(), numBuckets);
                final Int2ObjectOpenHashMap<LongArrayList> groupOffsetToTerms = new Int2ObjectOpenHashMap<>();
                for (final Long2DoubleMap.Entry entry : termToValue.long2DoubleEntrySet()) {
                    final int groupOffset = session.findPercentile(entry.getDoubleValue(), percentiles);
                    LongArrayList terms = groupOffsetToTerms.get(groupOffset);
                    if (terms == null) {
                        terms = new LongArrayList();
                        groupOffsetToTerms.put(groupOffset, terms);
                    }
                    terms.add(entry.getLongKey());
                }

                int arraySize = 0;
                for (final LongArrayList terms : groupOffsetToTerms.values()) {
                    arraySize += terms.size();
                }
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
                    // TODO: Not use StringGroupKey
                    nextGroupKeys.add(new StringGroupKey(keyTerm));
                    nextGroupParents.add(group);
                }
                rules.add(new GroupMultiRemapRule(group, 0, positiveGroups, conditions));
            }
            final GroupMultiRemapRule[] rulesArr = rules.toArray(new GroupMultiRemapRule[rules.size()]);
            session.regroup(rulesArr, true);
        } else if (session.isStringField(field)) {
            final Int2ObjectOpenHashMap<Object2DoubleOpenHashMap<String>> perGroupTermToValue = new Int2ObjectOpenHashMap<>();
            Session.iterateMultiString(session.getSessionsMapRaw(), sessionMetricIndexes, Collections.<String, Integer>emptyMap(), field, new Session.StringIterateCallback() {
                @Override
                public void term(String term, long[] stats, int group) {
                    Object2DoubleOpenHashMap<String> termToValue = perGroupTermToValue.get(group);
                    if (termToValue == null) {
                        termToValue = new Object2DoubleOpenHashMap<>();
                        perGroupTermToValue.put(group, termToValue);
                    }
                    termToValue.put(term, metric.apply(term, stats, group));
                }
            });
            final List<GroupMultiRemapRule> rules = Lists.newArrayListWithCapacity(session.numGroups);
            for (int group = 1; group <= session.numGroups; group++) {
                final int groupBase = 1 + (group - 1) * numBuckets;
                final Object2DoubleOpenHashMap<String> termToValue = perGroupTermToValue.get(group);
                final double[] percentiles = Session.getPercentiles(termToValue.values(), numBuckets);
                final Int2ObjectOpenHashMap<ArrayList<String>> groupOffsetToTerms = new Int2ObjectOpenHashMap<>();
                for (final Map.Entry<String, Double> entry : termToValue.entrySet()) {
                    final int groupOffset = session.findPercentile(entry.getValue(), percentiles);
                    ArrayList<String> terms = groupOffsetToTerms.get(groupOffset);
                    if (terms == null) {
                        terms = new ArrayList<>();
                        groupOffsetToTerms.put(groupOffset, terms);
                    }
                    terms.add(entry.getKey());
                }

                int arraySize = 0;
                for (final ArrayList<String> terms : groupOffsetToTerms.values()) {
                    arraySize += terms.size();
                }
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
                    // TODO: Not use StringGroupKey
                    nextGroupKeys.add(new StringGroupKey(keyTerm));
                    nextGroupParents.add(group);
                }
                rules.add(new GroupMultiRemapRule(group, 0, positiveGroups, conditions));
            }
            final GroupMultiRemapRule[] rulesArr = rules.toArray(new GroupMultiRemapRule[rules.size()]);
            session.regroup(rulesArr, true);
        } else {
            throw new IllegalArgumentException("Field is neither int field nor string field: " + field);
        }

        session.popStats();

        session.numGroups = nextGroupKeys.size() - 1;
        session.groupKeySet = DumbGroupKeySet.create(session.groupKeySet, nextGroupParents.toIntArray(), nextGroupKeys);
        session.currentDepth += 1;

        out.accept("ExplodedByAggregatePercentile");
    }
}
