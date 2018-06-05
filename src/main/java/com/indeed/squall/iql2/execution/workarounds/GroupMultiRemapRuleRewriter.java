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

package com.indeed.squall.iql2.execution.workarounds;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.Instrumentation;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.TermCount;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.squall.iql2.execution.WrappingImhotepSession;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GroupMultiRemapRuleRewriter extends WrappingImhotepSession implements ImhotepSession {
    private static RegroupCondition THE_CONDITION = new RegroupCondition("fakeField", true, 0, null, false);
    private static RegroupCondition[] THE_CONDITIONS = new RegroupCondition[]{THE_CONDITION};
    private static RegroupConditionMessage THE_CONDITION_PROTO = RegroupConditionMessage.newBuilder().setField("fakeField").setIntType(true).setIntTerm(0).setInequality(false).build();

    private final ImhotepSession wrapped;

    public GroupMultiRemapRuleRewriter(ImhotepSession wrapped) {
        super(wrapped);
        this.wrapped = wrapped;
    }

    @Override
    public ImhotepSession wrapped() {
        return wrapped;
    }

    private static boolean requiresRewrite(GroupMultiRemapRule rule) {
        return rule.conditions.length == 0;
    }

    private static boolean requiresRewrite(GroupMultiRemapRule[] rules) {
        for (final GroupMultiRemapRule rule : rules) {
            if (requiresRewrite(rule)) {
                return true;
            }
        }
        return false;
    }

    private static boolean requiresRewrite(GroupMultiRemapMessage rule) {
        return rule.getConditionCount() == 0;
    }

    private static boolean requiresRewrite(GroupMultiRemapMessage[] rules) {
        for (final GroupMultiRemapMessage rule : rules) {
            if (requiresRewrite(rule)) {
                return true;
            }
        }
        return false;
    }

    private static boolean requiresRewrite(Iterable<GroupMultiRemapMessage> rules) {
        for (final GroupMultiRemapMessage rule : rules) {
            if (requiresRewrite(rule)) {
                return true;
            }
        }
        return false;
    }

    private static GroupMultiRemapRule makeRule(int targetGroup, int negativeGroup) {
        return new GroupMultiRemapRule(targetGroup, negativeGroup, new int[]{negativeGroup}, THE_CONDITIONS);
    }

    private static GroupMultiRemapMessage makeProtoRule(int targetGroup, int negativeGroup) {
        return GroupMultiRemapMessage.newBuilder().setTargetGroup(targetGroup).setNegativeGroup(negativeGroup).addPositiveGroup(negativeGroup).addCondition(THE_CONDITION_PROTO).build();
    }

    private static GroupMultiRemapRule rewrite(GroupMultiRemapRule rule) {
        if (requiresRewrite(rule)) {
            return makeRule(rule.targetGroup, rule.negativeGroup);
        } else {
            return rule;
        }
    }

    private static GroupMultiRemapRule[] rewrite(GroupMultiRemapRule[] rules) {
        if (requiresRewrite(rules)) {
            final GroupMultiRemapRule[] rewritten = new GroupMultiRemapRule[rules.length];
            for (int i = 0; i < rules.length; i++) {
                rewritten[i] = rewrite(rules[i]);
            }
            return rewritten;
        } else {
            return rules;
        }
    }

    private static GroupMultiRemapMessage rewriteProto(GroupMultiRemapMessage rule) {
        if (requiresRewrite(rule)) {
            return makeProtoRule(rule.getTargetGroup(), rule.getNegativeGroup());
        } else {
            return rule;
        }
    }

    private static List<GroupMultiRemapMessage> rewriteProtos(List<GroupMultiRemapMessage> rules) {
        if (requiresRewrite(rules)) {
            rules.replaceAll(GroupMultiRemapRuleRewriter::rewriteProto);
        }
        return rules;
    }

    private Iterator<GroupMultiRemapRule> rewrite(Iterator<GroupMultiRemapRule> iterator) {
        return Iterators.transform(iterator, new Function<GroupMultiRemapRule, GroupMultiRemapRule>() {
            public GroupMultiRemapRule apply(GroupMultiRemapRule rule) {
                return rewrite(rule);
            }
        });
    }

    @Override
    public int regroup(GroupMultiRemapRule[] groupMultiRemapRules) throws ImhotepOutOfMemoryException {
        return wrapped.regroup(rewrite(groupMultiRemapRules));
    }

    @Override
    public int regroup(int i, Iterator<GroupMultiRemapRule> iterator) throws ImhotepOutOfMemoryException {
        return wrapped.regroup(i, rewrite(iterator));
    }

    @Override
    public int regroup(GroupMultiRemapRule[] groupMultiRemapRules, boolean b) throws ImhotepOutOfMemoryException {
        return wrapped.regroup(rewrite(groupMultiRemapRules), b);
    }

    @Override
    public int regroupWithProtos(GroupMultiRemapMessage[] rawRuleMessages, boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        return regroupWithProtos(Arrays.asList(rawRuleMessages), errorOnCollisions);
    }

    public int regroupWithProtos(List<GroupMultiRemapMessage> rawRuleMessages, boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        return wrapped.regroupWithProtos(Iterables.toArray(rewriteProtos(rawRuleMessages), GroupMultiRemapMessage.class), errorOnCollisions);
    }

    @Override
    public int regroup(int i, Iterator<GroupMultiRemapRule> iterator, boolean b) throws ImhotepOutOfMemoryException {
        return wrapped.regroup(i, rewrite(iterator), b);
    }

    // AUTO-GENERATED DELEGATION FROM HERE ON OUT

    @Override
    public long getTotalDocFreq(String[] strings, String[] strings1) {
        return wrapped.getTotalDocFreq(strings, strings1);
    }

    @Override
    public long[] getGroupStats(int i) {
        return wrapped.getGroupStats(i);
    }

    @Override
    public FTGSIterator getFTGSIterator(String[] strings, String[] strings1) {
        return wrapped.getFTGSIterator(strings, strings1);
    }

    @Override
    public FTGSIterator getFTGSIterator(String[] strings, String[] strings1, long l) {
        return wrapped.getFTGSIterator(strings, strings1, l);
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(Map<String, long[]> map, Map<String, String[]> map1) {
        return wrapped.getSubsetFTGSIterator(map, map1);
    }

    @Override
    public FTGSIterator getFTGSIterator(String[] intFields, String[] stringFields, long termLimit, int sortStat) {
        return wrapped.getFTGSIterator(intFields, stringFields, termLimit, sortStat);
    }

    @Override
    public GroupStatsIterator getDistinct(String field, boolean isIntField) {
        return wrapped.getDistinct(field, isIntField);
    }

    @Override
    public int regroup(GroupRemapRule[] groupRemapRules) throws ImhotepOutOfMemoryException {
        return wrapped.regroup(groupRemapRules);
    }

    @Override
    public int regroup2(int i, Iterator<GroupRemapRule> iterator) throws ImhotepOutOfMemoryException {
        return wrapped.regroup2(i, iterator);
    }

    @Override
    public int regroup(QueryRemapRule queryRemapRule) throws ImhotepOutOfMemoryException {
        return wrapped.regroup(queryRemapRule);
    }

    @Override
    public void intOrRegroup(String s, long[] longs, int i, int i1, int i2) throws ImhotepOutOfMemoryException {
        wrapped.intOrRegroup(s, longs, i, i1, i2);
    }

    @Override
    public void stringOrRegroup(String s, String[] strings, int i, int i1, int i2) throws ImhotepOutOfMemoryException {
        wrapped.stringOrRegroup(s, strings, i, i1, i2);
    }

    @Override
    public void regexRegroup(String s, String s1, int i, int i1, int i2) throws ImhotepOutOfMemoryException {
        wrapped.regexRegroup(s, s1, i, i1, i2);
    }

    @Override
    public void randomRegroup(String s, boolean b, String s1, double v, int i, int i1, int i2) throws ImhotepOutOfMemoryException {
        wrapped.randomRegroup(s, b, s1, v, i, i1, i2);
    }

    @Override
    public void randomMultiRegroup(String s, boolean b, String s1, int i, double[] doubles, int[] ints) throws ImhotepOutOfMemoryException {
        wrapped.randomMultiRegroup(s, b, s1, i, doubles, ints);
    }

    @Override
    public void randomMetricRegroup(final int stat, final String salt, final double p, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        wrapped.randomMetricRegroup(stat, salt, p, targetGroup, negativeGroup, positiveGroup);
    }

    @Override
    public void randomMetricMultiRegroup(final int stat, final String salt, final int targetGroup, final double[] percentages, final int[] resultGroups) throws ImhotepOutOfMemoryException {
        wrapped.randomMetricMultiRegroup(stat, salt, targetGroup, percentages, resultGroups);
    }

    @Override
    public int metricRegroup(int i, long l, long l1, long l2) throws ImhotepOutOfMemoryException {
        return wrapped.metricRegroup(i, l, l1, l2);
    }

    @Override
    public int metricRegroup(int i, long l, long l1, long l2, boolean b) throws ImhotepOutOfMemoryException {
        return wrapped.metricRegroup(i, l, l1, l2, b);
    }

    @Override
    public int metricRegroup2D(int i, long l, long l1, long l2, int i1, long l3, long l4, long l5) throws ImhotepOutOfMemoryException {
        return wrapped.metricRegroup2D(i, l, l1, l2, i1, l3, l4, l5);
    }

    @Override
    public int metricFilter(int i, long l, long l1, boolean b) throws ImhotepOutOfMemoryException {
        return wrapped.metricFilter(i, l, l1, b);
    }

    @Override
    public List<TermCount> approximateTopTerms(String s, boolean b, int i) {
        return wrapped.approximateTopTerms(s, b, i);
    }

    @Override
    public int pushStat(String s) throws ImhotepOutOfMemoryException {
        return wrapped.pushStat(s);
    }

    @Override
    public int pushStats(List<String> list) throws ImhotepOutOfMemoryException {
        return wrapped.pushStats(list);
    }

    @Override
    public int popStat() {
        return wrapped.popStat();
    }

    @Override
    public int getNumStats() {
        return wrapped.getNumStats();
    }

    @Override
    public int getNumGroups() {
        return wrapped.getNumGroups();
    }

    @Override
    public long getLowerBound(int i) {
        return wrapped.getLowerBound(i);
    }

    @Override
    public long getUpperBound(int i) {
        return wrapped.getUpperBound(i);
    }

    @Override
    public void createDynamicMetric(String s) throws ImhotepOutOfMemoryException {
        wrapped.createDynamicMetric(s);
    }

    @Override
    public void updateDynamicMetric(String s, int[] ints) throws ImhotepOutOfMemoryException {
        wrapped.updateDynamicMetric(s, ints);
    }

    @Override
    public void conditionalUpdateDynamicMetric(String s, RegroupCondition[] regroupConditions, int[] ints) {
        wrapped.conditionalUpdateDynamicMetric(s, regroupConditions, ints);
    }

    @Override
    public void groupConditionalUpdateDynamicMetric(String s, int[] ints, RegroupCondition[] regroupConditions, int[] ints1) {
        wrapped.groupConditionalUpdateDynamicMetric(s, ints, regroupConditions, ints1);
    }

    @Override
    public void groupQueryUpdateDynamicMetric(final String name, final int[] groups, final Query[] conditions, final int[] deltas) throws ImhotepOutOfMemoryException {
        wrapped.groupQueryUpdateDynamicMetric(name, groups, conditions, deltas);
    }

    @Override
    public void close() {
        wrapped.close();
    }

    @Override
    public void resetGroups() throws ImhotepOutOfMemoryException {
        wrapped.resetGroups();
    }

    @Override
    public void rebuildAndFilterIndexes(List<String> list, List<String> list1) throws ImhotepOutOfMemoryException {
        wrapped.rebuildAndFilterIndexes(list, list1);
    }

    @Override
    public void addObserver(Instrumentation.Observer observer) {
        wrapped.addObserver(observer);
    }

    @Override
    public void removeObserver(Instrumentation.Observer observer) {
        wrapped.removeObserver(observer);
    }

    @Override
    public long getNumDocs() {
        return wrapped.getNumDocs();
    }

    @Override
    public GroupStatsIterator getGroupStatsIterator(final int stat) {
        return wrapped.getGroupStatsIterator(stat);
    }


    @Override
    public PerformanceStats getPerformanceStats(boolean reset) {
        return wrapped.getPerformanceStats(reset);
    }

    @Override
    public PerformanceStats closeAndGetPerformanceStats() {
        return wrapped.closeAndGetPerformanceStats();
    }
}

