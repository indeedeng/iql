/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.imhotep.ez;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.exceptions.GroupLimitExceededException;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.util.core.Pair;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.serialization.Stringifier;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.TermCount;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.iql.ScoredLong;
import com.indeed.imhotep.iql.ScoredObject;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectSortedMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.text.DecimalFormat;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static com.indeed.imhotep.ez.Field.IntField;
import static com.indeed.imhotep.ez.Field.StringField;
import static com.indeed.imhotep.ez.Stats.BinOpStat;
import static com.indeed.imhotep.ez.Stats.CachedStat;
import static com.indeed.imhotep.ez.Stats.ConstantStat;
import static com.indeed.imhotep.ez.Stats.CountStat;
import static com.indeed.imhotep.ez.Stats.DynamicMetricStat;
import static com.indeed.imhotep.ez.Stats.ExpStat;
import static com.indeed.imhotep.ez.Stats.LogStat;
import static com.indeed.imhotep.ez.Stats.HasIntStat;
import static com.indeed.imhotep.ez.Stats.HasStringStat;
import static com.indeed.imhotep.ez.Stats.IntFieldStat;
import static com.indeed.imhotep.ez.Stats.HasStringFieldStat;
import static com.indeed.imhotep.ez.Stats.HasIntFieldStat;
import static com.indeed.imhotep.ez.Stats.Stat;
import static com.indeed.imhotep.ez.Stats.StatRefStat;
import static com.indeed.imhotep.ez.Stats.requireValid;

/**
 * @author jwolfe
 */
public class EZImhotepSession implements Closeable {
    private static final Logger log = Logger.getLogger(EZImhotepSession.class);
    public static int GROUP_LIMIT = 1000000;    // 1 mil

    private final ImhotepSession session;
    private final Deque<StatReference> statStack = new ArrayDeque<StatReference>();
    private final Map<String, DynamicMetric> dynamicMetrics = Maps.newHashMap();
    private int stackDepth = 0;
    private int numGroups = 2;
    private boolean closed = false;

    public EZImhotepSession(ImhotepSession session) {
        this.session = session;
    }

    public StatReference pushStatGeneric(Stat stat) throws ImhotepOutOfMemoryException {
        if(stat instanceof Stats.AggregateBinOpStat) {
            return pushStatComposite((Stats.AggregateBinOpStat) stat);
        } else {
            return pushStat(stat);
        }
    }

    public SingleStatReference pushStat(Stat stat) throws ImhotepOutOfMemoryException {
        if(stat instanceof Stats.AggregateBinOpStat) {
            throw new IllegalArgumentException("Aggregate operations have to be pushed with pushStatGeneric");
        }
        final int initialDepth = stackDepth;
        for (String statToPush : stat.pushes(this)) {
            stackDepth = session.pushStat(statToPush);
        }
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

    public CompositeStatReference pushStatComposite(Stats.AggregateBinOpStat stat) throws ImhotepOutOfMemoryException {
        final int initialDepth = stackDepth;
        for (String statToPush : stat.pushes(this)) {
            stackDepth = session.pushStat(statToPush);
        }
        if (initialDepth + 2 != stackDepth) {
            throw new RuntimeException("Bug! Did not change stack depth by exactly 2.");
        }
        final SingleStatReference stat1 = new SingleStatReference(initialDepth, stat.toString(), this);
        final SingleStatReference stat2 = new SingleStatReference(initialDepth + 1, stat.toString(), this);
        final CompositeStatReference statReference = new CompositeStatReference(stat1, stat2);
        statStack.push(statReference);
        return statReference;
    }

    public StatReference popStat() {
        stackDepth = session.popStat();
        final StatReference poppedStat = statStack.pop();
        poppedStat.invalidate();
        return poppedStat;
    }

    public int getStackDepth() {
        return this.stackDepth;
    }

    public int getNumGroups() {
        return numGroups;
    }

    public double[] getGroupStats(StatReference statReference) {
        return statReference.getGroupStats();
    }

    long[] getGroupStats(int depth) {
        return session.getGroupStats(depth);
    }

    /**
     * Returns the number of bytes written to the temp files for this session locally.
     * Returns -1 if tempFileSizeBytesLeft was set to null or if the session is not a RemoteImhotepMultiSession.
     */
    public long getTempFilesBytesWritten() {
        if(!(session instanceof RemoteImhotepMultiSession)) {
            return -1;
        }
        return ((RemoteImhotepMultiSession) session).getTempFilesBytesWritten();
    }

    public DynamicMetric createDynamicMetric(String name) throws ImhotepOutOfMemoryException {
        if (dynamicMetrics.containsKey(name)) {
            throw new IllegalArgumentException("Dynamic metric with name "+name+" already exists!");
        }
        session.createDynamicMetric(name);
        return new DynamicMetric(name);
    }

    public void deleteDynamicMetric(DynamicMetric metric) {
        metric.valid = false;
        throw new UnsupportedOperationException("Sorry, this isn't actually possible yet");
    }

    public void ftgsSubsetIterate(Map<Field, List<?>> fieldsToTermsSubsets, FTGSCallback callback) {
        final FTGSIterator ftgsIterator = getFtgsSubsetIterator(fieldsToTermsSubsets);
        performIteration(callback, ftgsIterator);
    }

    public void ftgsIterate(List<Field> fields, FTGSCallback callback) {
        ftgsIterate(fields, callback, 0);
    }

    public void ftgsIterate(List<Field> fields, FTGSCallback callback, int termLimit) {
        final FTGSIterator ftgsIterator = getFtgsIterator(fields, termLimit);
        performIteration(callback, ftgsIterator);
    }

    private void performIteration(FTGSCallback callback, FTGSIterator ftgsIterator) {
        try {
            while (ftgsIterator.nextField()) {
                final String field = ftgsIterator.fieldName();
                if (ftgsIterator.fieldIsIntType()) {
                    while (ftgsIterator.nextTerm()) {
                        final long term = ftgsIterator.termIntVal();

                        while (ftgsIterator.nextGroup()) {
                            final int group = ftgsIterator.group();
                            ftgsIterator.groupStats(callback.stats);
                            callback.intTermGroup(field, term, group);
                        }
                    }
                } else {
                    while (ftgsIterator.nextTerm()) {
                        final String term = ftgsIterator.termStringVal();
                        while (ftgsIterator.nextGroup()) {
                            final int group = ftgsIterator.group();
                            ftgsIterator.groupStats(callback.stats);
                            callback.stringTermGroup(field, term, group);
                        }
                    }
                }
            }
        } finally {
            Closeables2.closeQuietly(ftgsIterator, log);
        }
    }

    public <E> Iterator<E> ftgsGetSubsetIterator(Map<Field, List<?>> fieldsToTermsSubsets, final FTGSIteratingCallback<E> callback) {
        final FTGSIterator ftgsIterator = getFtgsSubsetIterator(fieldsToTermsSubsets);

        // TODO: make sure ftgsIterator gets closed
        return new FTGSCallbackIterator<E>(callback, ftgsIterator);
    }

    private FTGSIterator getFtgsSubsetIterator(Map<Field, List<?>> fieldsToTermsSubsets) {
        final Map<String, long[]> intFields = Maps.newHashMap();
        final Map<String, String[]> stringFields = Maps.newHashMap();
        for (Field field : fieldsToTermsSubsets.keySet()) {
            final List<?> terms = fieldsToTermsSubsets.get(field);
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
                            throw new IllegalArgumentException("IN grouping for int field " + field.getFieldName() +
                                    " has a non integer argument: " + term);
                        }
                    }
                }
                Arrays.sort(intTermsSubset);
                intFields.put(field.fieldName, intTermsSubset);
            } else {
                final String[] stringTermsSubset = new String[terms.size()];
                for(int i = 0; i < stringTermsSubset.length; i++) {
                    stringTermsSubset[i] = (String)terms.get(i);
                }
                Arrays.sort(stringTermsSubset);
                stringFields.put(field.fieldName, stringTermsSubset);
            }
        }

        return session.getSubsetFTGSIterator(intFields, stringFields);
    }

    public <E> Iterator<E> ftgsGetIterator(List<Field> fields, final FTGSIteratingCallback<E> callback, int termLimit) {
        final FTGSIterator ftgsIterator = getFtgsIterator(fields, termLimit);

        // TODO: make sure ftgsIterator gets closed
        return new FTGSCallbackIterator<E>(callback, ftgsIterator);
    }

    private FTGSIterator getFtgsIterator(List<Field> fields, int termLimit) {
        final List<String> intFields = Lists.newArrayList();
        final List<String> stringFields = Lists.newArrayList();
        for (Field field : fields) {
            if (field.isIntField()) {
                intFields.add(field.fieldName);
            } else {
                stringFields.add(field.fieldName);
            }
        }

        if(termLimit == Integer.MAX_VALUE) {
            termLimit = 0;  // slight optimization by disabling the limit completely
        }

        return session.getFTGSIterator(
                intFields.toArray(new String[intFields.size()]),
                stringFields.toArray(new String[stringFields.size()]),
                termLimit
        );
    }

    public void filter(IntField field, Predicate<Long> predicate) throws ImhotepOutOfMemoryException {
        if (numGroups > 2) {
            System.err.println("WARNING: performing a term filter with more than one group. Consider filtering before regrouping.");
        }
        final LongList intTerms = intFieldTerms(field, session, predicate, 0);
        final long[] longs = intTerms.toLongArray();
        for (int group = 1; group < numGroups; group++) {
            session.intOrRegroup(field.getFieldName(), longs, group, 0, group);
        }
    }

    public void filterNegation(IntField field, Predicate<Long> predicate) throws ImhotepOutOfMemoryException {
        if (numGroups > 2) {
            System.err.println("WARNING: performing a term filter with more than one group. Consider filtering before regrouping.");
        }
        final LongList intTerms = intFieldTerms(field, session, predicate, 0);
        final long[] longs = intTerms.toLongArray();
        for (int group = 1; group < numGroups; group++) {
            session.intOrRegroup(field.getFieldName(), longs, group, group, 0);
        }
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

    public void filter(StringField field, Predicate<String> predicate) throws ImhotepOutOfMemoryException {
        if (numGroups > 2) {
            System.err.println("WARNING: performing a term filter with more than one group. Consider filtering before regrouping.");
        }
        final List<String> stringTerms = stringFieldTerms(field, session, predicate, 0);
        for (int group = 1; group < numGroups; group++) {
            session.stringOrRegroup(field.getFieldName(), stringTerms.toArray(new String[stringTerms.size()]), group, 0, group);
        }
    }

    public void filterNegation(StringField field, Predicate<String> predicate) throws ImhotepOutOfMemoryException {
        if (numGroups > 2) {
            System.err.println("WARNING: performing a term filter with more than one group. Consider filtering before regrouping.");
        }
        final List<String> stringTerms = stringFieldTerms(field, session, predicate, 0);
        for (int group = 1; group < numGroups; group++) {
            session.stringOrRegroup(field.getFieldName(), stringTerms.toArray(new String[stringTerms.size()]), group, group, 0);
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
        requireValid(stat);
        numGroups = session.metricFilter(stat.depth, min, max, false);
    }

    public void filterNegation(SingleStatReference stat, long min, long max) throws ImhotepOutOfMemoryException {
        requireValid(stat);
        numGroups = session.metricFilter(stat.depth, min, max, true);
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
        final Int2ObjectMap<GroupKey> ret = new Int2ObjectOpenHashMap<GroupKey>();
        ret.put(1, GroupKey.empty());
        return ret;
    }

    // @deprecated due to inefficiency. use splitAll()
    @Deprecated
    public @Nullable Int2ObjectMap<GroupKey> explodeEachGroup(IntField field, long[] terms, @Nullable Int2ObjectMap<GroupKey> groupKeys) throws ImhotepOutOfMemoryException {
        if(terms.length == 0) {
            return new Int2ObjectOpenHashMap<GroupKey>();
        }
        checkGroupLimitWithFactor(terms.length);
        final GroupMultiRemapMessage[] rules = new GroupMultiRemapMessage[numGroups-1];
        final Int2ObjectMap<GroupKey> ret = groupKeys == null ? null : new Int2ObjectOpenHashMap<GroupKey>();
        int positiveGroup = 1;
        final GroupMultiRemapMessage.Builder regroupMessageBuilder =  GroupMultiRemapMessage.newBuilder();
        final RegroupConditionMessage.Builder conditionMessageBuilder = RegroupConditionMessage.newBuilder();
        for (int group = 1; group < numGroups; group++) {
            regroupMessageBuilder.clear();
            for (int i = 0; i < terms.length; i++) {
                final long term = terms[i];
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
        numGroups = session.regroupWithProtos(rules, true);
        return ret;
    }

    // @deprecated due to inefficiency. use splitAll()
    @Deprecated
    public @Nullable Int2ObjectMap<GroupKey> explodeEachGroup(StringField field, String[] terms, @Nullable Int2ObjectMap<GroupKey> groupKeys) throws ImhotepOutOfMemoryException {
        if(terms.length == 0) {
            return new Int2ObjectOpenHashMap<GroupKey>();
        }
        checkGroupLimitWithFactor(terms.length);
        final GroupMultiRemapMessage[] rules = new GroupMultiRemapMessage[numGroups-1];
        final Int2ObjectMap<GroupKey> ret = groupKeys == null ? null : new Int2ObjectOpenHashMap<GroupKey>();
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
        numGroups = session.regroupWithProtos(rules, true);
        return ret;
    }

    private void checkGroupLimitWithFactor(int factor) {
        final double newNumGroups = (double)(numGroups-1) * factor;
        checkGroupLimit(newNumGroups);
    }

    private static void checkGroupLimit(double newNumGroups) {
        if(newNumGroups > GROUP_LIMIT) {
            DecimalFormat df = new DecimalFormat("###,###");
            throw new GroupLimitExceededException("Number of groups " + df.format(newNumGroups) + " exceeds the limit " + df.format(GROUP_LIMIT)+
                    ". Please simplify the query.");
        }
    }

    public @Nullable Int2ObjectMap<GroupKey> splitAll(Field field, @Nullable Int2ObjectMap<GroupKey> groupKeys, int termLimit) throws ImhotepOutOfMemoryException {
        final Int2ObjectMap<GroupKey> ret = groupKeys == null ? null : new Int2ObjectOpenHashMap<GroupKey>();
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
                numGroups = session.regroupWithProtos(rules, true);
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
                numGroups = session.regroupWithProtos(rules, true);
            }
        }
        return ret;
    }

    public @Nullable Int2ObjectMap<GroupKey> splitAllExplode(Field field, @Nullable Int2ObjectMap<GroupKey> groupKeys, int termLimit) throws ImhotepOutOfMemoryException {
        if (field.isIntField()) {
            final IntField intField = (IntField) field;
            final LongList terms = intFieldTerms(intField, session, null, termLimit);
            return explodeEachGroup(intField, terms.toLongArray(), groupKeys);
        } else {
            final StringField stringField = (StringField) field;
            final List<String> terms = stringFieldTerms(stringField, session, null, termLimit);
            return explodeEachGroup(stringField, terms.toArray(new String[terms.size()]), groupKeys);
        }
    }

    private int checkGroupLimitForTerms(Int2ObjectMap<? extends Collection> groupToTerms) {
        int newNumGroups = 0;
        for (final Collection termsForGroup : groupToTerms.values()) {
            newNumGroups += termsForGroup.size();
        }
        checkGroupLimit(newNumGroups);
        return newNumGroups;
    }

    @SuppressWarnings("unchecked")
    public @Nullable Int2ObjectMap<GroupKey> splitAllTopK(Field field, @Nullable Int2ObjectMap<GroupKey> groupKeys, int topK, Stat stat, boolean bottom) throws ImhotepOutOfMemoryException {
        final Int2ObjectMap<GroupKey> ret = groupKeys == null ? null : new Int2ObjectOpenHashMap<GroupKey>();
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
            numGroups = session.regroupWithProtos(rules, true);
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
            numGroups = session.regroupWithProtos(rules, true);
        }
        return ret;
    }

    private int getStringRemapRules(final Field field, final @Nullable Int2ObjectMap<GroupKey> groupKeys, Int2ObjectMap<GroupKey> newGroupKeys, final GroupMultiRemapMessage[] rules, final int ruleIndex, int positiveGroup, final int group, final List<String> termList) {
        final GroupMultiRemapMessage.Builder remapMessageBuilder = GroupMultiRemapMessage.newBuilder();
        remapMessageBuilder.setTargetGroup(group);
        remapMessageBuilder.setNegativeGroup(0);
        positiveGroup = getStringRegroupConditions(field, groupKeys, newGroupKeys, positiveGroup, group, termList, remapMessageBuilder);
        rules[ruleIndex] = remapMessageBuilder.build();
        return positiveGroup;
    }

    private int getIntRemapRules(final Field field, final @Nullable Int2ObjectMap<GroupKey> groupKeys, final Int2ObjectMap<GroupKey> newGroupKeys, final GroupMultiRemapMessage[] rules, final int ruleIndex, int positiveGroup, final int group, final long[] nativeArray) {
        final GroupMultiRemapMessage.Builder remapMessageBuilder = GroupMultiRemapMessage.newBuilder();
        remapMessageBuilder.setTargetGroup(group);
        remapMessageBuilder.setNegativeGroup(0);
        positiveGroup = getIntRegroupConditions(field, groupKeys, newGroupKeys, positiveGroup, group, nativeArray, remapMessageBuilder);
        rules[ruleIndex] = remapMessageBuilder.build();
        return positiveGroup;
    }

    private int getStringRegroupConditions(
            final Field field,
            final @Nullable Int2ObjectMap<GroupKey> groupKeys,
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
            final @Nullable Int2ObjectMap<GroupKey> groupKeys,
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
        numGroups = session.metricRegroup(statRef.depth, min, max, intervalSize, noGutters);
        // Delete the keys for trailing groups that don't exist on the server
        for (int group = numGroups; group <= newExpectedNumberOfGroups; group++) {
            ret.remove(group);
        }
        return ret;
    }

    public Int2ObjectSortedMap<GroupKey> metricRegroup2D(SingleStatReference xStat, long xMin, long xMax, long xIntervalSize,
                                   SingleStatReference yStat, long yMin, long yMax, long yIntervalSize) throws ImhotepOutOfMemoryException {
        final Int2ObjectSortedMap<GroupKey> ret = new Int2ObjectAVLTreeMap<>();
        numGroups = session.metricRegroup2D(xStat.depth, xMin, xMax, xIntervalSize, yStat.depth, yMin, yMax, yIntervalSize);
        final int xBuckets = (int)(((xMax - 1) - xMin) / xIntervalSize + 3);
        final int yBuckets = (int)(((yMax - 1) - yMin) / yIntervalSize + 3);
        final int numBuckets = xBuckets * yBuckets;
        ret.put(1, GroupKey.singleton(String.format("< %d, < %d", xMin, yMin)));
        ret.put(numBuckets, GroupKey.singleton(String.format(">= %d, >= %d", xMax, yMax)));
        ret.put(xBuckets, GroupKey.singleton(String.format(">= %d, < %d", xMax, yMin)));
        ret.put((yBuckets-1)*xBuckets+1, GroupKey.singleton(String.format("< %d, >= %d", xMin, yMax)));
        {
            int index = 2;
            for (long x = xMin; x < xMax; x+=xIntervalSize) {
                ret.put(index, GroupKey.singleton(String.format("[%d, %d), < %d", x, x+xIntervalSize, yMin)));
                ret.put(index+(yBuckets-1)*xBuckets, GroupKey.singleton(String.format("[%d, %d), >= %d", x, x+xIntervalSize, yMax)));
                index++;
            }
        }
        {
            int index = 1;
            for (long y = yMin; y < yMax; y+=yIntervalSize) {
                ret.put(index*xBuckets+1, GroupKey.singleton(String.format("< %d, [%d, %d)", xMin, y, y+yIntervalSize)));
                ret.put((index+1)*xBuckets, GroupKey.singleton(String.format(">= %d, [%d, %d)", xMax, y, y+yIntervalSize)));
                index++;
            }
        }
        {
            for (int xBucket = 2; xBucket < xBuckets; xBucket++) {
                final long xStart = (xBucket-2)*xIntervalSize;
                final long xEnd = xStart+xIntervalSize;
                for (int yBucket = 1; yBucket < yBuckets-1; yBucket++) {
                    final long yStart = (yBucket-1)*yIntervalSize;
                    final long yEnd = yStart+yIntervalSize;
                    ret.put(yBucket*xBuckets+xBucket, GroupKey.singleton(String.format("[%d, %d), [%d, %d)", xStart, xEnd, yStart, yEnd)));
                }
            }
        }
        for (int group = numGroups; group <= numBuckets; group++) {
            ret.remove(group);
        }
        return ret;
    }

    public Object2LongMap<String> topTerms(StringField field, int k) {
        final List<TermCount> termCounts = session.approximateTopTerms(field.getFieldName(), false, k);
        final Object2LongMap<String> ret = new Object2LongOpenHashMap<String>();
        for (TermCount termCount : termCounts) {
            ret.put(termCount.getTerm().getTermStringVal(), termCount.getCount());
        }
        return ret;
    }

    public Long2LongMap topTerms(IntField field, int k) {
        final List<TermCount> termCounts = session.approximateTopTerms(field.getFieldName(), true, k);
        final Long2LongMap ret = new Long2LongOpenHashMap();
        for (TermCount termCount : termCounts) {
            ret.put(termCount.getTerm().getTermIntVal(), termCount.getCount());
        }
        return ret;
    }

    private static final class GetGroupTermsCallback extends FTGSCallback {

        final Int2ObjectMap<LongList> intTermListsMap = new Int2ObjectOpenHashMap<LongList>();
        final Int2ObjectMap<List<String>> stringTermListsMap = new Int2ObjectOpenHashMap<List<String>>();
        private int rowCount = 0;


        public GetGroupTermsCallback(final int numStats) {
            super(numStats);
        }

        public void intTermGroup(final String field, final long term, int group) {
            checkGroupLimit(rowCount++);
            if (!intTermListsMap.containsKey(group)) {
                intTermListsMap.put(group, new LongArrayList());
            }
            intTermListsMap.get(group).add(term);
        }

        public void stringTermGroup(final String field, final String term, int group) {
            checkGroupLimit(rowCount++);
            if (!stringTermListsMap.containsKey(group)) {
                stringTermListsMap.put(group, Lists.<String>newArrayList());
            }
            stringTermListsMap.get(group).add(term);
        }
    }

    private Int2ObjectMap<List<String>> getStringGroupTerms(StringField field, int termLimit) {
        final GetGroupTermsCallback callback = new GetGroupTermsCallback(stackDepth);
        ftgsIterate(Arrays.asList((Field) field), callback, termLimit);
        return callback.stringTermListsMap;
    }

    private Int2ObjectMap<LongList> getIntGroupTerms(IntField field, int termLimit) {
        final GetGroupTermsCallback callback = new GetGroupTermsCallback(stackDepth);
        ftgsIterate(Arrays.asList((Field)field), callback, termLimit);
        return callback.intTermListsMap;
    }

    private static final class GetGroupTermsCallbackTopK extends FTGSCallback {

        final Int2ObjectMap<PriorityQueue<ScoredLong>> intTermListsMap = new Int2ObjectOpenHashMap<PriorityQueue<ScoredLong>>();
        final Int2ObjectMap<PriorityQueue<ScoredObject<String>>> stringTermListsMap = new Int2ObjectOpenHashMap<PriorityQueue<ScoredObject<String>>>();
        final Comparator<ScoredObject> scoredObjectComparator;
        final Comparator<ScoredLong> scoredLongComparator;
        private final StatReference count;
        private final int k;
        private final boolean isBottom;

        public GetGroupTermsCallbackTopK(final int numStats, StatReference count, int k, boolean isBottom) {
            super(numStats);
            this.count = count;
            this.k = k;
            this.isBottom = isBottom;
            scoredObjectComparator = isBottom ? ScoredObject.BOTTOM_SCORE_COMPARATOR : ScoredObject.TOP_SCORE_COMPARATOR;
            scoredLongComparator = isBottom ? ScoredLong.BOTTOM_SCORE_COMPARATOR : ScoredLong.TOP_SCORE_COMPARATOR;
        }

        public void intTermGroup(final String field, final long term, int group) {
            PriorityQueue<ScoredLong> terms = intTermListsMap.get(group);
            if (terms == null) {
                terms = new PriorityQueue<ScoredLong>(10, scoredLongComparator);
                intTermListsMap.put(group, terms);
            }
            final double count = getStat(this.count);
            if (terms.size() < k) {
                terms.add(new ScoredLong(count, term));
            } else {
                final double headCount = terms.peek().getScore();
                if ((!isBottom && count > headCount) ||
                        (isBottom && count < headCount)) {
                    terms.remove();
                    terms.add(new ScoredLong(count, term));
                }
            }
        }

        public void stringTermGroup(final String field, final String term, int group) {
            PriorityQueue<ScoredObject<String>> terms = stringTermListsMap.get(group);
            if (terms == null) {
                terms = new PriorityQueue<ScoredObject<String>>(10, scoredObjectComparator);
                stringTermListsMap.put(group, terms);
            }
            final double count = getStat(this.count);
            if (terms.size() < k) {
                terms.add(new ScoredObject<String>(count, term));
            } else {
                final double headCount = terms.peek().getScore();
                if ((!isBottom && count > headCount) ||
                        (isBottom && count < headCount)) {
                    terms.remove();
                    terms.add(new ScoredObject<String>(count, term));
                }
            }
        }
    }

    private Int2ObjectMap<PriorityQueue<ScoredObject<String>>> getStringGroupTermsTopK(StringField field, int k, Stat stat, boolean bottom) throws ImhotepOutOfMemoryException {
        final StatReference statRef = pushStat(stat);
        final GetGroupTermsCallbackTopK callback = new GetGroupTermsCallbackTopK(stackDepth, statRef, k, bottom);
        ftgsIterate(Arrays.asList((Field)field), callback);
        popStat();
        return callback.stringTermListsMap;
    }

    private Int2ObjectMap<PriorityQueue<ScoredLong>> getIntGroupTermsTopK(IntField field, int k, Stat stat, boolean bottom) throws ImhotepOutOfMemoryException {
        final StatReference statRef = pushStat(stat);
        final GetGroupTermsCallbackTopK callback = new GetGroupTermsCallbackTopK(stackDepth, statRef, k, bottom);
        ftgsIterate(Arrays.asList((Field)field), callback);
        popStat();
        return callback.intTermListsMap;
    }

    public static void filter(StringField field, String[] terms, ImhotepSession session) throws ImhotepOutOfMemoryException {
        new EZImhotepSession(session).filter(field, terms);
    }

    private static final class FieldTermsCallback extends FTGSCallback {

        final LongList intTerms = new LongArrayList();
        final List<String> stringTerms = Lists.newArrayList();
        private final Predicate<Long> predicateInt;
        private final Predicate<String> predicateString;

        private long lastIntTerm = Long.MIN_VALUE;
        private String lastStringTerm = null;
        private boolean firstIteration = true;

        public FieldTermsCallback(final int numStats, @Nullable Predicate<Long> predicateInt, @Nullable Predicate<String> predicateString) {
            super(numStats);

            this.predicateInt = predicateInt;
            this.predicateString = predicateString;
        }

        public void intTermGroup(final String field, final long term, int group) {
            // expecting incoming terms to be in sorted order
            if(firstIteration || term != lastIntTerm) {
                firstIteration = false;
                lastIntTerm = term;
                if(predicateInt == null || predicateInt.apply(term)) {
                    intTerms.add(term);
                }
            }
        }

        public void stringTermGroup(final String field, final String term, int group) {
            // expecting incoming terms to be in sorted order
            if(firstIteration || !term.equals(lastStringTerm)) {
                firstIteration = false;
                lastStringTerm = term;
                if(predicateString == null || predicateString.apply(term)) {
                    stringTerms.add(term);
                }
            }
        }
    }
    public LongList intFieldTerms(IntField field, ImhotepSession session, @Nullable Predicate<Long> filterPredicate, int termLimit) throws ImhotepOutOfMemoryException {
        final FieldTermsCallback callback = new FieldTermsCallback(stackDepth, filterPredicate, null);
        new EZImhotepSession(session).ftgsIterate(Arrays.asList((Field)field), callback, termLimit);
        return callback.intTerms;
    }

    public List<String> stringFieldTerms(StringField field, ImhotepSession session, @Nullable Predicate<String> filterPredicate, int termLimit) throws ImhotepOutOfMemoryException {
        final FieldTermsCallback callback = new FieldTermsCallback(stackDepth, null, filterPredicate);
        new EZImhotepSession(session).ftgsIterate(Arrays.asList((Field)field), callback, termLimit);
        return callback.stringTerms;
    }

    public static abstract class FTGSCallback {

        private final long[] stats;

        public FTGSCallback(int numStats) {
            stats = new long[numStats];
        }

        protected final double getStat(StatReference ref) {
            requireValid(ref);
            return ref.getValue(stats);
        }

        protected abstract void intTermGroup(String field, long term, int group);
        protected abstract void stringTermGroup(String field, String term, int group);
    }

    public static abstract class FTGSIteratingCallback <E> {

        final long[] stats;

        public FTGSIteratingCallback(int numStats) {
            stats = new long[numStats];
        }

        protected final double getStat(StatReference ref) {
            requireValid(ref);
            return ref.getValue(stats);
        }

        public abstract E intTermGroup(String field, long term, int group);
        public abstract E stringTermGroup(String field, String term, int group);
    }

    public class FTGSDoNothingCallback extends FTGSCallback {

        public FTGSDoNothingCallback(final int numStats) {
            super(numStats);
        }

        protected void intTermGroup(final String field, final long term, int group) {}

        protected void stringTermGroup(final String field, final String term, int group) {}
    }

    @Override
    public void close() {
        if (!closed) {
            session.close();
            closed = true;
        }
    }

    public static Stat add(Stat... stats) {
        return new BinOpStat("+", stats);
    }
    public static Stat sub(Stat... stats) {
        return new BinOpStat("-", stats);
    }
    public static Stat mult(Stat... stats) {
        return new BinOpStat("*", stats);
    }
    public static Stat div(Stat... stats) {
        return new BinOpStat("/", stats);
    }
    public static Stat mod(Stat... stats) {
        return new BinOpStat("%", stats);
    }
    public static Stat less(Stat... stats) {
        return new BinOpStat("<", stats);
    }
    public static Stat lessEq(Stat... stats) {
        return new BinOpStat("<=", stats);
    }
    public static Stat isEqual(Stat... stats) {
        // try to optimize it as a hasint stat
        if(stats.length == 2 && stats[0] instanceof IntFieldStat && stats[1] instanceof ConstantStat) {
            return hasInt(((IntFieldStat)stats[0]).getFieldName(), ((ConstantStat) stats[1]).getValue());
        }
        return new BinOpStat("=", stats);
    }
    public static Stat isNotEqual(Stat... stats) {
        return new BinOpStat("!=", stats);
    }
    public static Stat greater(Stat... stats) {
        return new BinOpStat(">", stats);
    }
    public static Stat greaterEq(Stat... stats) {
        return new BinOpStat(">=", stats);
    }
    public static Stat min(Stat... stats) {
        return new BinOpStat("min()", stats);
    }
    public static Stat max(Stat... stats) {
        return new BinOpStat("max()", stats);
    }
    public static Stat exp(Stat ref, int scaleFactor) {
        return new ExpStat(ref, scaleFactor);
    }
    public static Stat log(Stat ref, int scaleFactor) {
        return new LogStat(ref, scaleFactor);
    }
    public static Stat constant(long value) {
        return new ConstantStat(value);
    }
    public static Stat intField(String name) {
        return new IntFieldStat(name);
    }
    public static Stat intField(IntField field) {
        return new IntFieldStat(field.getFieldName());
    }
    public static Stat dynamic(DynamicMetric metric) {
        return new DynamicMetricStat(metric);
    }
    public static Stat hasInt(String field, long value) {
        return new HasIntStat(field, value);
    }
    public static Stat hasString(String field, String value) {
        return new HasStringStat(field, value);
    }
    public static Stat hasIntField(String field) {
        return new HasIntFieldStat(field);
    }
    public static Stat hasStringField(String field) {
        return new HasStringFieldStat(field);
    }
    public static Stat lucene(Query luceneQuery) {
        return new Stats.LuceneQueryStat(luceneQuery);
    }
    public static Stat ref(SingleStatReference ref) {
        return new StatRefStat(ref);
    }
    public static Stat counts() {
        return new CountStat();
    }
    public static Stat cached(Stat stat) {
        return new CachedStat(stat);
    }
    public static Stat abs(Stat stat) {
        return new Stats.AbsoluteValueStat(stat);
    }
    public static Stat floatScale(String intField, long mult, long add) {
        return new Stats.FloatScaleStat(intField, mult, add);
    }
    public static Stat multiplyShiftRight(int shift, Stat stat1, Stat stat2) {
        return new Stats.MultiplyShiftRight(shift, stat1, stat2);
    }
    public static Stat shiftLeftDivide(int shift, Stat stat1, Stat stat2) {
        return new Stats.ShiftLeftDivide(shift, stat1, stat2);
    }

    public static Stat aggDiv(Stat stat1, Stat stat2) {
        return new Stats.AggregateBinOpStat("/", stat1, stat2);
    }

    public static Stat aggDivConst(Stat stat1, long value) {
        return new Stats.AggregateBinOpConstStat("/", stat1, value);
    }

    @Nonnull
    private static long[] intArrayToLongArray(@Nonnull final int[] a) {
        final long[] ret = new long[a.length];
        for (int i = 0; i < a.length; ++i) {
            ret[i] = a[i];
        }
        return ret;
    }
}
