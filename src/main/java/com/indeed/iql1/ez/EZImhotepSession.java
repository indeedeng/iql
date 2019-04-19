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
 package com.indeed.iql1.ez;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.io.RequestTools;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.web.Limits;
import com.indeed.iql1.iql.ScoredLong;
import com.indeed.iql1.iql.ScoredObject;
import com.indeed.iql2.execution.ImhotepSessionHolder;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.serialization.Stringifier;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterators;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static com.indeed.iql1.ez.Field.IntField;
import static com.indeed.iql1.ez.Field.StringField;

/**
 * @author jwolfe
 */
public class EZImhotepSession implements Closeable {
    private static final Logger log = Logger.getLogger(EZImhotepSession.class);

    private final ImhotepSessionHolder session;
    private final Deque<StatReference> statStack = new ArrayDeque<>();
    private final Limits limits;
    private int stackDepth = 0;
    private int numGroups = 2;
    private boolean closed = false;

    public EZImhotepSession(final ImhotepSessionHolder session, final Limits limits) {
        this.session = session;
        this.limits = limits;
    }

    public StatReference pushStatGeneric(Stats.Stat stat) throws ImhotepOutOfMemoryException {
        if(stat instanceof Stats.AggregateBinOpStat) {
            return pushStatComposite((Stats.AggregateBinOpStat) stat);
        } else {
            return pushStatInternal(stat, true);
        }
    }

    // Use this method when you don't need index of stat in imhotep session
    public StatReference pushStat(final Stats.Stat stat) throws ImhotepOutOfMemoryException {
        return pushStatInternal(stat, true);
    }

    // Use this method when you do need index of stat in imhotep session
    // i.e. for metricRegroup
    public SingleStatReference pushSingleStat(final Stats.Stat stat) throws ImhotepOutOfMemoryException {
        return pushStatInternal(stat, false);
    }

    private SingleStatReference pushStatInternal(
            final Stats.Stat stat,
            final boolean allowAggregateStats) throws ImhotepOutOfMemoryException {
        if(stat instanceof Stats.AggregateBinOpStat) {
            throw new IllegalArgumentException("Aggregate operations have to be pushed with pushStatGeneric");
        }
        if (!allowAggregateStats && (stat instanceof Stats.AggregateBinOpConstStat)) {
            throw new IqlKnownException.ParseErrorException(
                    "'/' (aggregate division) not supported here in IQL1. Did you mean '\\' for per-document instead? If not, for deeper nested arithmetic, use IQL2.");
        }
        final int initialDepth = stackDepth;
        stackDepth = session.pushStats(stat.pushes());
        if (initialDepth + 1 != stackDepth) {
            throw new RuntimeException("Bug! Did not change stack depth by exactly 1.");
        }
        SingleStatReference statReference = new SingleStatReference(initialDepth, stat.toString(), this);
        if(stat instanceof Stats.AggregateBinOpConstStat) { // hacks for handling division by a constant
            final Stats.AggregateBinOpConstStat statAsConstAggregate = (Stats.AggregateBinOpConstStat) stat;
            if(!"/".equals(statAsConstAggregate.getOp())) {
                throw new IllegalArgumentException("Only aggregate division is currently supported");
            }
            statReference = new ConstantDivideSingleStatReference(statReference, statAsConstAggregate.getValue(), this);
        }

        statStack.push(statReference);
        return statReference;
    }

    private CompositeStatReference pushStatComposite(Stats.AggregateBinOpStat stat) throws ImhotepOutOfMemoryException {
        final int initialDepth = stackDepth;
        stackDepth = session.pushStats(stat.pushes());
        if (initialDepth + 2 != stackDepth) {
            throw new RuntimeException("Bug! Did not change stack depth by exactly 2.");
        }
        final SingleStatReference stat1 = new SingleStatReference(initialDepth, stat.toString(), this);
        final SingleStatReference stat2 = new SingleStatReference(initialDepth + 1, stat.toString(), this);
        final CompositeStatReference statReference = new CompositeStatReference(stat1, stat2);
        statStack.push(statReference);
        return statReference;
    }

    public void popStat() {
        stackDepth = session.popStat();
        final StatReference poppedStat = statStack.pop();
        poppedStat.invalidate();
    }

    public int getStackDepth() {
        return stackDepth;
    }

    public int getNumGroups() {
        return numGroups;
    }

    public double[] getGroupStats(StatReference statReference) throws ImhotepOutOfMemoryException {
        return statReference.getGroupStats();
    }

    long[] getGroupStats(int depth) throws ImhotepOutOfMemoryException {
        return session.getGroupStats(ImhotepSession.stackStat(depth));
    }

    public long[] getDistinct(final Field field) {
        try (final GroupStatsIterator distinct = session.getDistinct(field.getFieldName(), field.isIntField())) {
            return LongIterators.unwrap(distinct, distinct.getNumGroups());
        } catch(final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the number of bytes written to the temp files for this session locally.
     * Returns -1 if tempFileSizeBytesLeft was set to null or if the session is not a RemoteImhotepMultiSession.
     */
    public long getTempFilesBytesWritten() {
        return session.getTempFilesBytesWritten();
    }

    public void ftgsIterate(final Field field, final FTGSCallback callback) throws ImhotepOutOfMemoryException {
        ftgsIterate(field, callback, 0);
    }

    public void ftgsIterate(final Field field, final FTGSCallback callback, final int termLimit) throws ImhotepOutOfMemoryException {
        final FTGSIterator ftgsIterator = getFtgsIterator(field, termLimit);
        performIteration(callback, ftgsIterator);
    }

    private static void performIteration(final FTGSCallback callback, final FTGSIterator ftgsIterator) {
        try {
            while (ftgsIterator.nextField()) {
                if (ftgsIterator.fieldIsIntType()) {
                    while (ftgsIterator.nextTerm()) {
                        final long term = ftgsIterator.termIntVal();

                        while (ftgsIterator.nextGroup()) {
                            final int group = ftgsIterator.group();
                            ftgsIterator.groupStats(callback.stats);
                            callback.intTermGroup(term, group);
                        }
                    }
                } else {
                    while (ftgsIterator.nextTerm()) {
                        final String term = ftgsIterator.termStringVal();
                        while (ftgsIterator.nextGroup()) {
                            final int group = ftgsIterator.group();
                            ftgsIterator.groupStats(callback.stats);
                            callback.stringTermGroup(term, group);
                        }
                    }
                }
            }
        } finally {
            Closeables2.closeQuietly(ftgsIterator, log);
        }
    }

    public <E> Iterator<E> ftgsGetSubsetIterator(final Field field, final List<?> termsSubset, final FTGSIteratingCallback<E> callback) throws ImhotepOutOfMemoryException {
        final FTGSIterator ftgsIterator = getFtgsSubsetIterator(field, termsSubset);

        // TODO: make sure ftgsIterator gets closed
        return new FTGSCallbackIterator<>(callback, ftgsIterator);
    }

    private FTGSIterator getFtgsSubsetIterator(final Field field, final List<?> terms) throws ImhotepOutOfMemoryException {
        final Map<FieldSet, long[]> intFields = Maps.newHashMap();
        final Map<FieldSet, String[]> stringFields = Maps.newHashMap();
        if (field.isIntField()) {
            final long[] intTermsSubset = new long[terms.size()];
            for(int i = 0; i < intTermsSubset.length; i++) {
                final Object term = terms.get(i);
                if(term instanceof Long) {
                    intTermsSubset[i] = (Long) term;
                } else if(term instanceof String) {
                    try {
                        intTermsSubset[i] = Long.valueOf((String) term);
                    } catch (NumberFormatException e) {
                        // TODO: move
                        throw new IqlKnownException.ParseErrorException("IN grouping for int field " + field.getFieldName() +
                                " has a non integer argument: " + term);
                    }
                }
            }
            Arrays.sort(intTermsSubset);
            intFields.put(FieldSet.of(session.getDatasetName(), field.fieldName, true), intTermsSubset);
        } else {
            final String[] stringTermsSubset = new String[terms.size()];
            for(int i = 0; i < stringTermsSubset.length; i++) {
                stringTermsSubset[i] = (String)terms.get(i);
            }
            Arrays.sort(stringTermsSubset);
            stringFields.put(FieldSet.of(session.getDatasetName(), field.fieldName, false), stringTermsSubset);
        }

        return session.getSubsetFTGSIterator(intFields, stringFields, null);
    }

    public <E> Iterator<E> ftgsGetIterator(final Field field, final FTGSIteratingCallback<E> callback, final int termLimit) throws ImhotepOutOfMemoryException {
        final FTGSIterator ftgsIterator = getFtgsIterator(field, termLimit);

        // TODO: make sure ftgsIterator gets closed
        return new FTGSCallbackIterator<>(callback, ftgsIterator);
    }

    private FTGSIterator getFtgsIterator(final Field field, int termLimit) throws ImhotepOutOfMemoryException {
        final List<String> intFields = Lists.newArrayList();
        final List<String> stringFields = Lists.newArrayList();
        if (field.isIntField()) {
            intFields.add(field.fieldName);
        } else {
            stringFields.add(field.fieldName);
        }

        if(termLimit == Integer.MAX_VALUE) {
            termLimit = 0;  // slight optimization by disabling the limit completely
        }

        return session.getFTGSIterator(
                intFields.toArray(new String[intFields.size()]),
                stringFields.toArray(new String[stringFields.size()]),
                termLimit, null
        );
    }

    public void filter(IntField field, long[] terms) throws ImhotepOutOfMemoryException {
        if (numGroups > 2) {
            System.err.println("WARNING: performing a term filter with more than one group. Consider filtering before regrouping.");
        }
        for (int group = 1; group < numGroups; group++) {
            session.intOrRegroup(field.getFieldName(), terms, group, 0, group);
        }
    }

    public void filterNegation(IntField field, long[] terms) throws ImhotepOutOfMemoryException {
        if (numGroups > 2) {
            System.err.println("WARNING: performing a term filter with more than one group. Consider filtering before regrouping.");
        }
        for (int group = 1; group < numGroups; group++) {
            session.intOrRegroup(field.getFieldName(), terms, group, group, 0);
        }
    }

    public void filter(StringField field, String[] terms) throws ImhotepOutOfMemoryException {
        if (numGroups > 2) {
            System.err.println("WARNING: performing a term filter with more than one group. Consider filtering before regrouping.");
        }
        for (int group = 1; group < numGroups; group++) {
            session.stringOrRegroup(field.getFieldName(), terms, group, 0, group);
        }
    }
    public void filterNegation(StringField field, String[] terms) throws ImhotepOutOfMemoryException {
        if (numGroups > 2) {
            System.err.println("WARNING: performing a term filter with more than one group. Consider filtering before regrouping.");
        }
        for (int group = 1; group < numGroups; group++) {
            session.stringOrRegroup(field.getFieldName(), terms, group, group, 0);
        }
    }

    public void filter(SingleStatReference stat, long min, long max) throws ImhotepOutOfMemoryException {
        if (numGroups > 2) {
            System.err.println("WARNING: performing a metric filter with more than one group. Consider filtering before regrouping.");
        }
        Stats.requireValid(stat);
        session.metricFilter(ImhotepSession.stackStat(stat.depth), min, max, false);
    }

    public void filterNegation(SingleStatReference stat, long min, long max) throws ImhotepOutOfMemoryException {
        if (numGroups > 2) {
            System.err.println("WARNING: performing a metric filter with more than one group. Consider filtering before regrouping.");
        }
        Stats.requireValid(stat);
        session.metricFilter(ImhotepSession.stackStat(stat.depth), min, max, true);
    }


    public void filter(Query query) throws ImhotepOutOfMemoryException {
        if (numGroups > 2) {
            System.err.println("WARNING: performing a query filter with more than one group. Consider filtering before regrouping.");
        }
        for (int group = 1; group < numGroups; group++) {
            session.regroup(new QueryRemapRule(group, query, 0, group));
        }
    }

    public void filterNegation(Query query) throws ImhotepOutOfMemoryException {
        if (numGroups > 2) {
            System.err.println("WARNING: performing a query filter with more than one group. Consider filtering before regrouping.");
        }
        for (int group = 1; group < numGroups; group++) {
            session.regroup(new QueryRemapRule(group, query, group, 0));
        }
    }

    /**
     * @param field field to sample by
     * @param p ratio of terms to remove. In the range [0,1]
     * @param salt the salt to use for hashing. Providing a constant salt will lead to a reproducible result.
     */
    public void filterSample(Field field, double p, String salt) throws ImhotepOutOfMemoryException {
        if (numGroups > 2) {
            System.err.println("WARNING: performing a term filter with more than one group. Consider filtering before regrouping.");
        }
        for (int group = 1; group < numGroups; group++) {
            session.randomRegroup(field.getFieldName(), field.isIntField(), salt, p, group, 0, group);
        }
    }

    /**
     * @param field field to filter on
     * @param regex regex to test with
     */
    public void filterRegex(Field field, String regex) throws ImhotepOutOfMemoryException {
        if (numGroups > 2) {
            System.err.println("WARNING: performing a filter with more than one group. Consider filtering before regrouping.");
        }
        for (int group = 1; group < numGroups; group++) {
            session.regexRegroup(field.getFieldName(), regex, group, 0, group);
        }
    }

    /**
     * @param field field to filter on
     * @param regex regex to test with
     */
    public void filterRegexNegation(Field field, String regex) throws ImhotepOutOfMemoryException {
        if (numGroups > 2) {
            System.err.println("WARNING: performing a filter with more than one group. Consider filtering before regrouping.");
        }
        for (int group = 1; group < numGroups; group++) {
            session.regexRegroup(field.getFieldName(), regex, group, group, 0);
        }
    }

    public static Int2ObjectMap<GroupKey> newGroupKeys() {
        final Int2ObjectMap<GroupKey> ret = new Int2ObjectOpenHashMap<>();
        ret.put(1, GroupKey.empty());
        return ret;
    }

    // TODO: replace with message builder
    private int regroupWithProtos(final GroupMultiRemapMessage[] rawRuleMessages) throws ImhotepOutOfMemoryException {
        final RequestTools.GroupMultiRemapRuleSender ruleSender =
                RequestTools.GroupMultiRemapRuleSender.createFromMessages(Arrays.asList(rawRuleMessages).iterator(), true);
        return session.regroupWithSender(ruleSender, true);
    }

    // @deprecated due to inefficiency. use splitAll()
    @Nullable
    @Deprecated
    public Int2ObjectMap<GroupKey> explodeEachGroup(IntField field, long[] terms, @Nullable Int2ObjectMap<GroupKey> groupKeys) throws ImhotepOutOfMemoryException {
        if(terms.length == 0) {
            return new Int2ObjectOpenHashMap<>();
        }
        checkGroupLimitWithFactor(terms.length);
        final GroupMultiRemapMessage[] rules = new GroupMultiRemapMessage[numGroups-1];
        final Int2ObjectMap<GroupKey> ret = groupKeys == null ? null : new Int2ObjectOpenHashMap<>();
        int positiveGroup = 1;
        final GroupMultiRemapMessage.Builder regroupMessageBuilder =  GroupMultiRemapMessage.newBuilder();
        final RegroupConditionMessage.Builder conditionMessageBuilder = RegroupConditionMessage.newBuilder();
        for (int group = 1; group < numGroups; group++) {
            regroupMessageBuilder.clear();
            for (final long term : terms) {
                final int newGroup = positiveGroup++;
                regroupMessageBuilder.addPositiveGroup(newGroup);
                if (groupKeys != null) {
                    ret.put(newGroup, groupKeys.get(group).add(term));
                }
                conditionMessageBuilder.clear();
                conditionMessageBuilder.setField(field.getFieldName());
                conditionMessageBuilder.setIntType(true);
                conditionMessageBuilder.setIntTerm(term);
                conditionMessageBuilder.setInequality(false);
                regroupMessageBuilder.addCondition(conditionMessageBuilder.build());
            }
            regroupMessageBuilder.setTargetGroup(group);
            regroupMessageBuilder.setNegativeGroup(0);

            rules[group - 1] = regroupMessageBuilder.build();
        }
        numGroups = regroupWithProtos(rules);
        return ret;
    }

    // @deprecated due to inefficiency. use splitAll()
    @Nullable
    @Deprecated
    public Int2ObjectMap<GroupKey> explodeEachGroup(StringField field, String[] terms, @Nullable Int2ObjectMap<GroupKey> groupKeys) throws ImhotepOutOfMemoryException {
        if(terms.length == 0) {
            return new Int2ObjectOpenHashMap<>();
        }
        checkGroupLimitWithFactor(terms.length);
        final GroupMultiRemapMessage[] rules = new GroupMultiRemapMessage[numGroups-1];
        final Int2ObjectMap<GroupKey> ret = groupKeys == null ? null : new Int2ObjectOpenHashMap<>();
        int positiveGroup = 1;
        final GroupMultiRemapMessage.Builder regroupMessageBuilder =  GroupMultiRemapMessage.newBuilder();
        final RegroupConditionMessage.Builder conditionMessageBuilder = RegroupConditionMessage.newBuilder();
        for (int group = 1; group < numGroups; group++) {
            regroupMessageBuilder.clear();
            for (final String term : terms) {
                final int newGroup = positiveGroup++;
                regroupMessageBuilder.addPositiveGroup(newGroup);
                if (groupKeys != null) {
                    ret.put(newGroup, groupKeys.get(group).add(term));
                }
                conditionMessageBuilder.clear();
                conditionMessageBuilder.setField(field.getFieldName());
                conditionMessageBuilder.setIntType(false);
                conditionMessageBuilder.setStringTerm(term);
                conditionMessageBuilder.setInequality(false);
                regroupMessageBuilder.addCondition(conditionMessageBuilder.build());
            }
            regroupMessageBuilder.setTargetGroup(group);
            regroupMessageBuilder.setNegativeGroup(0);

            rules[group - 1] = regroupMessageBuilder.build();
        }
        numGroups = regroupWithProtos(rules);
        return ret;
    }

    private void checkGroupLimitWithFactor(final int factor) {
        final long newNumGroups = ((long)(numGroups-1)) * factor;
        limits.assertQueryInMemoryRowsLimit(newNumGroups);
    }

    @Nullable
    public Int2ObjectMap<GroupKey> splitAll(Field field, @Nullable Int2ObjectMap<GroupKey> groupKeys, int termLimit) throws ImhotepOutOfMemoryException {
        final Int2ObjectMap<GroupKey> ret = groupKeys == null ? null : new Int2ObjectOpenHashMap<>();
        if (field.isIntField()) {
            final IntField intField = (IntField) field;
            // TODO: avoid row number explosion when current groupCount already = termLimit
            final Int2ObjectMap<LongList> termListsMap = getIntGroupTerms(intField, termLimit);

            final int newGroupCount = checkGroupLimitForTerms(termListsMap);

            final GroupMultiRemapMessage[] rules = new GroupMultiRemapMessage[termListsMap.size()];
            int ruleIndex = 0;
            int positiveGroup = 1;
            for (int group = 1; group < numGroups; group++) {
                final LongList termList = termListsMap.get(group);
                if (termList != null) {
                    final long[] termArray = termList.toLongArray();
                    positiveGroup = getIntRemapRules(field, groupKeys, ret, rules, ruleIndex, positiveGroup, group, termArray);
                    ruleIndex++;
                }
            }
            if(newGroupCount > 0) {
                numGroups = regroupWithProtos(rules);
            }
        } else {
            final StringField stringField = (StringField) field;
            final Int2ObjectMap<List<String>> termListsMap = getStringGroupTerms(stringField, termLimit);

            final int newGroupCount = checkGroupLimitForTerms(termListsMap);

            final GroupMultiRemapMessage[] rules = new GroupMultiRemapMessage[termListsMap.size()];
            int ruleIndex = 0;
            int positiveGroup = 1;
            for (int group = 1; group < numGroups; group++) {
                final List<String> termList = termListsMap.get(group);
                if (termList != null) {
                    positiveGroup = getStringRemapRules(field, groupKeys, ret, rules, ruleIndex, positiveGroup, group, termList);
                    ruleIndex++;
                }
            }
            if(newGroupCount > 0) {
                numGroups = regroupWithProtos(rules);
            }
        }
        return ret;
    }

    private int checkGroupLimitForTerms(Int2ObjectMap<? extends Collection> groupToTerms) {
        int newNumGroups = 0;
        for (final Collection termsForGroup : groupToTerms.values()) {
            newNumGroups += termsForGroup.size();
        }
        limits.assertQueryInMemoryRowsLimit(newNumGroups);
        return newNumGroups;
    }

    @Nullable
    public Int2ObjectMap<GroupKey> splitAllTopK(Field field, @Nullable Int2ObjectMap<GroupKey> groupKeys, int topK, Stats.Stat stat, boolean bottom) throws ImhotepOutOfMemoryException {
        final Int2ObjectMap<GroupKey> ret = groupKeys == null ? null : new Int2ObjectOpenHashMap<>();
        if (field.isIntField()) {
            final IntField intField = (IntField) field;
            final Int2ObjectMap<PriorityQueue<ScoredLong>> termListsMap = getIntGroupTermsTopK(intField, topK, stat, bottom);
            checkGroupLimitForTerms(termListsMap);
            final GroupMultiRemapMessage[] rules = new GroupMultiRemapMessage[termListsMap.size()];
            int ruleIndex = 0;
            int positiveGroup = 1;
            for (int group = 1; group < numGroups; group++) {
                final PriorityQueue<ScoredLong> termList = termListsMap.get(group);
                if (termList != null) {
                    final long[] nativeArray = new long[termList.size()];
                    int index = nativeArray.length-1;
                    while (!termList.isEmpty()) {
                        nativeArray[index--] = termList.remove().getValue();
                    }
                    positiveGroup = getIntRemapRules(field, groupKeys, ret, rules, ruleIndex, positiveGroup, group, nativeArray);
                    ruleIndex++;
                }
            }
            numGroups = regroupWithProtos(rules);
        } else {
            final StringField stringField = (StringField) field;
            final Int2ObjectMap<PriorityQueue<ScoredObject<String>>> termListsMap = getStringGroupTermsTopK(stringField, topK, stat, bottom);
            checkGroupLimitForTerms(termListsMap);
            final GroupMultiRemapMessage[] rules = new GroupMultiRemapMessage[termListsMap.size()];
            int ruleIndex = 0;
            int positiveGroup = 1;
            for (int group = 1; group < numGroups; group++) {
                final PriorityQueue<ScoredObject<String>> terms = termListsMap.get(group);
                if (terms != null) {
                    final String[] termsArray = new String[terms.size()];
                    int index = termsArray.length-1;
                    while (!terms.isEmpty()) {
                        termsArray[index--] = terms.remove().getObject();
                    }
                    positiveGroup = getStringRemapRules(field, groupKeys, ret, rules, ruleIndex, positiveGroup, group, Arrays.asList(termsArray));
                    ruleIndex++;
                }
            }
            numGroups = regroupWithProtos(rules);
        }
        return ret;
    }

    private int getStringRemapRules(final Field field, @Nullable final Int2ObjectMap<GroupKey> groupKeys, Int2ObjectMap<GroupKey> newGroupKeys, final GroupMultiRemapMessage[] rules, final int ruleIndex, int positiveGroup, final int group, final List<String> termList) {
        final GroupMultiRemapMessage.Builder remapMessageBuilder = GroupMultiRemapMessage.newBuilder();
        remapMessageBuilder.setTargetGroup(group);
        remapMessageBuilder.setNegativeGroup(0);
        positiveGroup = getStringRegroupConditions(field, groupKeys, newGroupKeys, positiveGroup, group, termList, remapMessageBuilder);
        rules[ruleIndex] = remapMessageBuilder.build();
        return positiveGroup;
    }

    private int getIntRemapRules(final Field field, @Nullable final Int2ObjectMap<GroupKey> groupKeys, final Int2ObjectMap<GroupKey> newGroupKeys, final GroupMultiRemapMessage[] rules, final int ruleIndex, int positiveGroup, final int group, final long[] nativeArray) {
        final GroupMultiRemapMessage.Builder remapMessageBuilder = GroupMultiRemapMessage.newBuilder();
        remapMessageBuilder.setTargetGroup(group);
        remapMessageBuilder.setNegativeGroup(0);
        positiveGroup = getIntRegroupConditions(field, groupKeys, newGroupKeys, positiveGroup, group, nativeArray, remapMessageBuilder);
        rules[ruleIndex] = remapMessageBuilder.build();
        return positiveGroup;
    }

    private int getStringRegroupConditions(
            final Field field,
            @Nullable final Int2ObjectMap<GroupKey> groupKeys,
            final Int2ObjectMap<GroupKey> newGroupKeys,
            int positiveGroup,
            final int group,
            final List<String> termList,
            final GroupMultiRemapMessage.Builder remapMessageBuilder
            ) {
        for (final String term : termList) {
            final RegroupConditionMessage.Builder conditionMessageBuilder = RegroupConditionMessage.newBuilder();
            conditionMessageBuilder.setField(field.getFieldName());
            conditionMessageBuilder.setIntType(false);
            conditionMessageBuilder.setStringTerm(term);
            conditionMessageBuilder.setInequality(false);
            remapMessageBuilder.addCondition(conditionMessageBuilder.build());
            final int newGroup = positiveGroup++;
            if (groupKeys != null) {
                newGroupKeys.put(newGroup, groupKeys.get(group).add(term));
            }
            remapMessageBuilder.addPositiveGroup(newGroup);
        }
        return positiveGroup;
    }

    private int getIntRegroupConditions(
            final Field field,
            @Nullable final Int2ObjectMap<GroupKey> groupKeys,
            final Int2ObjectMap<GroupKey> newGroupKeys,
            int positiveGroup,
            final int group,
            final long[] nativeArray,
            final GroupMultiRemapMessage.Builder remapMessageBuilder
    ) {
        for (final long term : nativeArray) {
            final RegroupConditionMessage.Builder conditionMessageBuilder = RegroupConditionMessage.newBuilder();
            conditionMessageBuilder.setField(field.getFieldName());
            conditionMessageBuilder.setIntType(true);
            conditionMessageBuilder.setIntTerm(term);
            conditionMessageBuilder.setInequality(false);
            remapMessageBuilder.addCondition(conditionMessageBuilder.build());
            final int newGroup = positiveGroup++;
            if (groupKeys != null) {
                newGroupKeys.put(newGroup, groupKeys.get(group).add(term));
            }
            remapMessageBuilder.addPositiveGroup(newGroup);
        }
        return positiveGroup;
    }

    public Int2ObjectMap<GroupKey> metricRegroup(SingleStatReference statRef, long min, long max, long intervalSize,
                                                boolean noGutters, Stringifier<Long> stringifier,
                                                @Nullable Int2ObjectMap<GroupKey> groupKeys) throws ImhotepOutOfMemoryException {
        final Int2ObjectMap<GroupKey> ret = new Int2ObjectOpenHashMap<>();
        if ( (max-min)%intervalSize != 0 ) {
            final long bucketRange = max - min;
            throw new IqlKnownException.ParseErrorException("Bucket range should be a multiple of the interval. To correct, decrease the upper bound to " + (max - bucketRange%intervalSize) + " or increase to " + (max + intervalSize - bucketRange%intervalSize));
        }
        final int gutterBuckets = noGutters ? 0 : 2;
        final int numBuckets = (int)((max-min-1)/intervalSize + 1 + gutterBuckets);
        for (int group = 1; group < numGroups; group++) {
            int bucket = 1;
            int newGroupOffset = (group - 1) * numBuckets;
            final GroupKey<String> groupKey = groupKeys != null ? groupKeys.get(group) : GroupKey.empty();
            for (long i = min; i < max; i += intervalSize, bucket++) {
                final String bucketString = String.format("[%s, %s)", stringifier.toString(i), stringifier.toString(i + intervalSize));
                ret.put(newGroupOffset + bucket, groupKey.add(bucketString));
            }

            if(!noGutters) {
                ret.put(newGroupOffset + numBuckets - 1, groupKey.add(String.format("< %s", stringifier.toString(min))));
                ret.put(newGroupOffset + numBuckets, groupKey.add(String.format(">= %s", stringifier.toString(max))));
            }
        }
        final int newExpectedNumberOfGroups = (numGroups-1) * numBuckets;
        session.metricRegroup(ImhotepSession.stackStat(statRef.depth), min, max, intervalSize, noGutters);
        numGroups = session.getNumGroups();
        // Delete the keys for trailing groups that don't exist on the server
        for (int group = numGroups; group <= newExpectedNumberOfGroups; group++) {
            ret.remove(group);
        }
        return ret;
    }

    private static final class GetGroupTermsCallback extends FTGSCallback {

        final Int2ObjectMap<LongList> intTermListsMap = new Int2ObjectOpenHashMap<>();
        final Int2ObjectMap<List<String>> stringTermListsMap = new Int2ObjectOpenHashMap<>();
        private final Limits limits;
        private int rowCount = 0;


        GetGroupTermsCallback(final int numStats, final Limits limits) {
            super(numStats);
            this.limits = limits;
        }

        public void intTermGroup(final long term, int group) {
            limits.assertQueryInMemoryRowsLimit(rowCount++);
            if (!intTermListsMap.containsKey(group)) {
                intTermListsMap.put(group, new LongArrayList());
            }
            intTermListsMap.get(group).add(term);
        }

        public void stringTermGroup(final String term, int group) {
            limits.assertQueryInMemoryRowsLimit(rowCount++);
            if (!stringTermListsMap.containsKey(group)) {
                stringTermListsMap.put(group, Lists.newArrayList());
            }
            stringTermListsMap.get(group).add(term);
        }
    }

    private Int2ObjectMap<List<String>> getStringGroupTerms(StringField field, int termLimit) throws ImhotepOutOfMemoryException {
        final GetGroupTermsCallback callback = new GetGroupTermsCallback(stackDepth, limits);
        ftgsIterate(field, callback, termLimit);
        return callback.stringTermListsMap;
    }

    private Int2ObjectMap<LongList> getIntGroupTerms(IntField field, int termLimit) throws ImhotepOutOfMemoryException {
        final GetGroupTermsCallback callback = new GetGroupTermsCallback(stackDepth, limits);
        ftgsIterate(field, callback, termLimit);
        return callback.intTermListsMap;
    }

    private static final class GetGroupTermsCallbackTopK extends FTGSCallback {

        final Int2ObjectMap<PriorityQueue<ScoredLong>> intTermListsMap = new Int2ObjectOpenHashMap<>();
        final Int2ObjectMap<PriorityQueue<ScoredObject<String>>> stringTermListsMap = new Int2ObjectOpenHashMap<>();
        final Comparator<ScoredObject<String>> scoredObjectComparator;
        final Comparator<ScoredLong> scoredLongComparator;
        private final StatReference count;
        private final int k;
        private final boolean isBottom;

        GetGroupTermsCallbackTopK(final int numStats, StatReference count, int k, boolean isBottom) {
            super(numStats);
            this.count = count;
            this.k = k;
            this.isBottom = isBottom;
            scoredObjectComparator = isBottom ? ScoredObject.bottomScoredObjectComparator(Comparator.<String>reverseOrder()) : ScoredObject.topScoredObjectComparator(Comparator.<String>reverseOrder());
            scoredLongComparator = isBottom ? ScoredLong.BOTTOM_SCORE_COMPARATOR : ScoredLong.TOP_SCORE_COMPARATOR;
        }

        public void intTermGroup(final long term, int group) {
            PriorityQueue<ScoredLong> terms = intTermListsMap.get(group);
            if (terms == null) {
                terms = new PriorityQueue<>(10, scoredLongComparator);
                intTermListsMap.put(group, terms);
            }
            final double count = getStat(this.count);
            if (terms.size() < k) {
                terms.add(new ScoredLong(count, term));
            } else {
                final ScoredLong headObject = terms.peek();
                final double headCount = headObject.getScore();
                if ((!isBottom && ( count > headCount || ( count == headCount && term < headObject.getValue() ))) ||
                        (isBottom && ( count < headCount || ( count == headCount && term > headObject.getValue() )))) {
                    terms.remove();
                    terms.add(new ScoredLong(count, term));
                }
            }
        }

        public void stringTermGroup(final String term, int group) {
            PriorityQueue<ScoredObject<String>> terms = stringTermListsMap.get(group);
            if (terms == null) {
                terms = new PriorityQueue<>(10, scoredObjectComparator);
                stringTermListsMap.put(group, terms);
            }
            final double count = getStat(this.count);
            if (terms.size() < k) {
                terms.add(new ScoredObject<>(count, term));
            } else {
                final ScoredObject headObject = terms.peek();
                final double headCount = headObject.getScore();
                if ((!isBottom && ( count > headCount || ( count == headCount && term.compareTo((String)headObject.getObject()) < 0 ))) ||
                        (isBottom && ( count < headCount || ( count == headCount && term.compareTo((String)headObject.getObject()) > 0 )))) {
                    terms.remove();
                    terms.add(new ScoredObject<>(count, term));
                }
            }
        }
    }

    private Int2ObjectMap<PriorityQueue<ScoredObject<String>>> getStringGroupTermsTopK(StringField field, int k, Stats.Stat stat, boolean bottom) throws ImhotepOutOfMemoryException {
        final StatReference statRef = pushStat(stat);
        final GetGroupTermsCallbackTopK callback = new GetGroupTermsCallbackTopK(stackDepth, statRef, k, bottom);
        ftgsIterate(field, callback);
        popStat();
        return callback.stringTermListsMap;
    }

    private Int2ObjectMap<PriorityQueue<ScoredLong>> getIntGroupTermsTopK(IntField field, int k, Stats.Stat stat, boolean bottom) throws ImhotepOutOfMemoryException {
        final StatReference statRef = pushStat(stat);
        final GetGroupTermsCallbackTopK callback = new GetGroupTermsCallbackTopK(stackDepth, statRef, k, bottom);
        ftgsIterate(field, callback);
        popStat();
        return callback.intTermListsMap;
    }

    public abstract static class FTGSCallback {

        private final long[] stats;

        public FTGSCallback(int numStats) {
            stats = new long[numStats];
        }

        protected final double getStat(StatReference ref) {
            Stats.requireValid(ref);
            return ref.getValue(stats);
        }

        protected abstract void intTermGroup(long term, int group);
        protected abstract void stringTermGroup(String term, int group);
    }

    public abstract static class FTGSIteratingCallback <E> {

        final long[] stats;

        public FTGSIteratingCallback(int numStats) {
            stats = new long[numStats];
        }

        protected final double getStat(StatReference ref) {
            Stats.requireValid(ref);
            return ref.getValue(stats);
        }

        public abstract E intTermGroup(long term, int group);
        public abstract E stringTermGroup(String term, int group);
    }

    @Nullable
    public PerformanceStats closeAndGetPerformanceStats() {
        if (closed) {
            return null;
        }
        final PerformanceStats performanceStats = session.closeAndGetPerformanceStats();
        closed = true;
        return performanceStats;
    }

    @Override
    public void close() {
        if (!closed) {
            session.close();
            closed = true;
        }
    }

    public static Stats.Stat add(Stats.Stat... stats) {
        return new Stats.BinOpStat("+", stats);
    }
    public static Stats.Stat sub(Stats.Stat... stats) {
        return new Stats.BinOpStat("-", stats);
    }
    public static Stats.Stat mult(Stats.Stat... stats) {
        return new Stats.BinOpStat("*", stats);
    }
    public static Stats.Stat div(Stats.Stat... stats) {
        return new Stats.BinOpStat("/", stats);
    }
    public static Stats.Stat mod(Stats.Stat... stats) {
        return new Stats.BinOpStat("%", stats);
    }
    public static Stats.Stat less(Stats.Stat... stats) {
        return new Stats.BinOpStat("<", stats);
    }
    public static Stats.Stat lessEq(Stats.Stat... stats) {
        return new Stats.BinOpStat("<=", stats);
    }
    public static Stats.Stat isEqual(Stats.Stat... stats) {
        // try to optimize it as a hasint stat
        if(stats.length == 2 && stats[0] instanceof Stats.IntFieldStat && stats[1] instanceof Stats.ConstantStat) {
            return hasInt(((Stats.IntFieldStat)stats[0]).getFieldName(), ((Stats.ConstantStat) stats[1]).getValue());
        }
        return new Stats.BinOpStat("=", stats);
    }
    public static Stats.Stat isNotEqual(Stats.Stat... stats) {
        return new Stats.BinOpStat("!=", stats);
    }
    public static Stats.Stat greater(Stats.Stat... stats) {
        return new Stats.BinOpStat(">", stats);
    }
    public static Stats.Stat greaterEq(Stats.Stat... stats) {
        return new Stats.BinOpStat(">=", stats);
    }
    public static Stats.Stat min(Stats.Stat... stats) {
        return new Stats.BinOpStat("min()", stats);
    }
    public static Stats.Stat max(Stats.Stat... stats) {
        return new Stats.BinOpStat("max()", stats);
    }
    public static Stats.Stat exp(Stats.Stat ref, int scaleFactor) {
        return new Stats.ExpStat(ref, scaleFactor);
    }
    public static Stats.Stat log(Stats.Stat ref, int scaleFactor) {
        return new Stats.LogStat(ref, scaleFactor);
    }
    public static Stats.Stat constant(long value) {
        return new Stats.ConstantStat(value);
    }
    public static Stats.Stat intField(String name) {
        return new Stats.IntFieldStat(name);
    }
    public static Stats.Stat hasInt(String field, long value) {
        return new Stats.HasIntStat(field, value);
    }
    public static Stats.Stat hasString(String field, String value) {
        return new Stats.HasStringStat(field, value);
    }
    public static Stats.Stat hasIntField(String field) {
        return new Stats.HasIntFieldStat(field);
    }
    public static Stats.Stat hasStringField(String field) {
        return new Stats.HasStringFieldStat(field);
    }
    public static Stats.Stat lucene(Query luceneQuery) {
        return new Stats.LuceneQueryStat(luceneQuery);
    }
    public static Stats.Stat counts() {
        return new Stats.CountStat();
    }
    public static Stats.Stat cached(Stats.Stat stat) {
        return new Stats.CachedStat(stat);
    }
    public static Stats.Stat abs(Stats.Stat stat) {
        return new Stats.AbsoluteValueStat(stat);
    }
    public static Stats.Stat floatScale(String intField, long mult, long add) {
        return new Stats.FloatScaleStat(intField, mult, add);
    }
    public static Stats.Stat multiplyShiftRight(int shift, Stats.Stat stat1, Stats.Stat stat2) {
        return new Stats.MultiplyShiftRight(shift, stat1, stat2);
    }
    public static Stats.Stat shiftLeftDivide(int shift, Stats.Stat stat1, Stats.Stat stat2) {
        return new Stats.ShiftLeftDivide(shift, stat1, stat2);
    }

    public static Stats.Stat aggDiv(Stats.Stat stat1, Stats.Stat stat2) {
        return new Stats.AggregateBinOpStat("/", stat1, stat2);
    }

    public static Stats.Stat aggDivConst(Stats.Stat stat1, long value) {
        return new Stats.AggregateBinOpConstStat("/", stat1, value);
    }
}
