package com.indeed.squall.iql2.execution.workarounds;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.Instrumentation;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.TermCount;
import com.indeed.imhotep.api.DocIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RawFTGSIterator;
import com.indeed.squall.iql2.execution.WrappingImhotepSession;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GroupMultiRemapRuleRewriter implements ImhotepSession, WrappingImhotepSession {
    private static RegroupCondition THE_CONDITION = new RegroupCondition("fakeField", true, 0, null, false);
    private static RegroupCondition[] THE_CONDITIONS = new RegroupCondition[]{THE_CONDITION};

    private final ImhotepSession wrapped;

    public GroupMultiRemapRuleRewriter(ImhotepSession wrapped) {
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

    private static GroupMultiRemapRule makeRule(int targetGroup, int negativeGroup) {
        return new GroupMultiRemapRule(targetGroup, negativeGroup, new int[]{negativeGroup}, THE_CONDITIONS);
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
    public RawFTGSIterator[] getSubsetFTGSIteratorSplits(Map<String, long[]> map, Map<String, String[]> map1) {
        return wrapped.getSubsetFTGSIteratorSplits(map, map1);
    }

    @Override
    public DocIterator getDocIterator(String[] strings, String[] strings1) throws ImhotepOutOfMemoryException {
        return wrapped.getDocIterator(strings, strings1);
    }

    @Override
    public RawFTGSIterator[] getFTGSIteratorSplits(String[] strings, String[] strings1, long l) {
        return wrapped.getFTGSIteratorSplits(strings, strings1, l);
    }

    @Override
    public RawFTGSIterator getFTGSIteratorSplit(String[] strings, String[] strings1, int i, int i1, long l) {
        return wrapped.getFTGSIteratorSplit(strings, strings1, i, i1, l);
    }

    @Override
    public void writeFTGSIteratorSplit(String[] strings, String[] strings1, int i, int i1, long l, Socket socket) throws ImhotepOutOfMemoryException {
        wrapped.writeFTGSIteratorSplit(strings, strings1, i, i1, l, socket);
    }

    @Override
    public RawFTGSIterator getSubsetFTGSIteratorSplit(Map<String, long[]> map, Map<String, String[]> map1, int i, int i1) {
        return wrapped.getSubsetFTGSIteratorSplit(map, map1, i, i1);
    }

    @Override
    public RawFTGSIterator mergeFTGSSplit(String[] strings, String[] strings1, String s, InetSocketAddress[] inetSocketAddresses, int i, long l) {
        return wrapped.mergeFTGSSplit(strings, strings1, s, inetSocketAddresses, i, l);
    }

    @Override
    public RawFTGSIterator mergeSubsetFTGSSplit(Map<String, long[]> map, Map<String, String[]> map1, String s, InetSocketAddress[] inetSocketAddresses, int i) {
        return wrapped.mergeSubsetFTGSSplit(map, map1, s, inetSocketAddresses, i);
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
}

