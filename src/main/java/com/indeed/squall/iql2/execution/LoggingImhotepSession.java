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

package com.indeed.squall.iql2.execution;

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
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LoggingImhotepSession implements ImhotepSession {
    private static final Logger log = Logger.getLogger(LoggingImhotepSession.class);

    private final ImhotepSession wrapped;

    public LoggingImhotepSession(ImhotepSession wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public long getTotalDocFreq(String[] intFields, String[] stringFields) {
        log.info("LoggingImhotepSession.getTotalDocFreq: " + "intFields = [" + Arrays.toString(intFields) + "], stringFields = [" + Arrays.toString(stringFields) + "]");
        return wrapped.getTotalDocFreq(intFields, stringFields);
    }

    @Override
    public long[] getGroupStats(int stat) {
        log.info("LoggingImhotepSession.getGroupStats: " + "stat = [" + stat + "]");
        return wrapped.getGroupStats(stat);
    }

    @Override
    public FTGSIterator getFTGSIterator(String[] intFields, String[] stringFields) {
        log.info("LoggingImhotepSession.getFTGSIterator: " + "intFields = [" + Arrays.toString(intFields) + "], stringFields = [" + Arrays.toString(stringFields) + "]");
        return wrapped.getFTGSIterator(intFields, stringFields);
    }

    @Override
    public FTGSIterator getFTGSIterator(String[] intFields, String[] stringFields, long termLimit) {
        log.info("LoggingImhotepSession.getFTGSIterator: " + "intFields = [" + Arrays.toString(intFields) + "], stringFields = [" + Arrays.toString(stringFields) + "], termLimit = [" + termLimit + "]");
        return wrapped.getFTGSIterator(intFields, stringFields, termLimit);
    }

    @Override
    public FTGSIterator getFTGSIterator(String[] intFields, String[] stringFields, long termLimit, int sortStat) {
        log.info("LoggingImhotepSession.getFTGSIterator: " + "intFields = [" + Arrays.toString(intFields) + "], stringFields = [" + Arrays.toString(stringFields) + "], termLimit = [" + termLimit + "], sortStat = [" + sortStat + "]");
        return wrapped.getFTGSIterator(intFields, stringFields, termLimit, sortStat);
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(Map<String, long[]> intFields, Map<String, String[]> stringFields) {
        log.info("LoggingImhotepSession.getSubsetFTGSIterator: " + "intFields = [" + intFields + "], stringFields = [" + stringFields + "]");
        return wrapped.getSubsetFTGSIterator(intFields, stringFields);
    }

    @Override
    public GroupStatsIterator getDistinct(String field, boolean isIntField) {
        log.info("LoggingImhotepSession.getDistinct: field = [" + field + "], isIntField = [" + isIntField + "]");
        return wrapped.getDistinct(field, isIntField);
    }

    @Override
    public int regroup(GroupMultiRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.regroup: " + "rawRules = [" + Arrays.toString(rawRules) + "]");
        return wrapped.regroup(rawRules);
    }

    @Override
    public int regroup(int numRawRules, Iterator<GroupMultiRemapRule> rawRules) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.regroup: " + "numRawRules = [" + numRawRules + "], rawRules = [" + rawRules + "]");
        return wrapped.regroup(numRawRules, rawRules);
    }

    @Override
    public int regroup(GroupMultiRemapRule[] rawRules, boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.regroup: " + "rawRules = [" + Arrays.toString(rawRules) + "], errorOnCollisions = [" + errorOnCollisions + "]");
        return wrapped.regroup(rawRules, errorOnCollisions);
    }

    @Override
    public int regroupWithProtos(GroupMultiRemapMessage[] rawRuleMessages, boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.rawRuleMessages = [" + rawRuleMessages + "], errorOnCollisions = [" + errorOnCollisions + "]");
        return wrapped.regroupWithProtos(rawRuleMessages, errorOnCollisions);
    }

    @Override
    public int regroup(int numRawRules, Iterator<GroupMultiRemapRule> rawRules, boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.regroup: " + "numRawRules = [" + numRawRules + "], rawRules = [" + rawRules + "], errorOnCollisions = [" + errorOnCollisions + "]");
        return wrapped.regroup(numRawRules, rawRules, errorOnCollisions);
    }

    @Override
    public int regroup(GroupRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.regroup: " + "rawRules = [" + Arrays.toString(rawRules) + "]");
        return wrapped.regroup(rawRules);
    }

    @Override
    public int regroup2(int numRawRules, Iterator<GroupRemapRule> iterator) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.regroup2: " + "numRawRules = [" + numRawRules + "], iterator = [" + iterator + "]");
        return wrapped.regroup2(numRawRules, iterator);
    }

    @Override
    public int regroup(QueryRemapRule rule) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.regroup: " + "rule = [" + rule + "]");
        return wrapped.regroup(rule);
    }

    @Override
    public void intOrRegroup(String field, long[] terms, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.intOrRegroup: " + "field = [" + field + "], terms = [" + Arrays.toString(terms) + "], targetGroup = [" + targetGroup + "], negativeGroup = [" + negativeGroup + "], positiveGroup = [" + positiveGroup + "]");
        wrapped.intOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
    }

    @Override
    public void stringOrRegroup(String field, String[] terms, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.stringOrRegroup: " + "field = [" + field + "], terms = [" + Arrays.toString(terms) + "], targetGroup = [" + targetGroup + "], negativeGroup = [" + negativeGroup + "], positiveGroup = [" + positiveGroup + "]");
        wrapped.stringOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
    }

    @Override
    public void regexRegroup(String field, String regex, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.regexRegroup: " + "field = [" + field + "], regex = [" + regex + "], targetGroup = [" + targetGroup + "], negativeGroup = [" + negativeGroup + "], positiveGroup = [" + positiveGroup + "]");
        wrapped.regexRegroup(field, regex, targetGroup, negativeGroup, positiveGroup);
    }

    @Override
    public void randomRegroup(String field, boolean isIntField, String salt, double p, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.randomRegroup: " + "field = [" + field + "], isIntField = [" + isIntField + "], salt = [" + salt + "], p = [" + p + "], targetGroup = [" + targetGroup + "], negativeGroup = [" + negativeGroup + "], positiveGroup = [" + positiveGroup + "]");
        wrapped.randomRegroup(field, isIntField, salt, p, targetGroup, negativeGroup, positiveGroup);
    }

    @Override
    public void randomMultiRegroup(String field, boolean isIntField, String salt, int targetGroup, double[] percentages, int[] resultGroups) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.randomMultiRegroup: " + "field = [" + field + "], isIntField = [" + isIntField + "], salt = [" + salt + "], targetGroup = [" + targetGroup + "], percentages = [" + Arrays.toString(percentages) + "], resultGroups = [" + Arrays.toString(resultGroups) + "]");
        wrapped.randomMultiRegroup(field, isIntField, salt, targetGroup, percentages, resultGroups);
    }

    @Override
    public void randomMetricRegroup(final int stat, final String salt, final double p, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.randomMetricRegroup: " + "stat = [" + stat + "], salt = [" + salt + "], p = [" + p + "], targetGroup = [" + targetGroup + "], negativeGroup = [" + negativeGroup + "], positiveGroup = [" + positiveGroup + "]");
        wrapped.randomMetricRegroup(stat, salt, p, targetGroup, negativeGroup, positiveGroup);
    }

    @Override
    public void randomMetricMultiRegroup(final int stat, final String salt, final int targetGroup, final double[] percentages, final int[] resultGroups) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.randomMetricMultiRegroup: " + "stat = [" + stat + "], salt = [" + salt + "], targetGroup = [" + targetGroup + "], percentages = [" + Arrays.toString(percentages) + "], resultGroups = [" + Arrays.toString(resultGroups) + "]");
        wrapped.randomMetricMultiRegroup(stat, salt, targetGroup, percentages, resultGroups);
    }

    @Override
    public int metricRegroup(int stat, long min, long max, long intervalSize) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.metricRegroup: " + "stat = [" + stat + "], min = [" + min + "], max = [" + max + "], intervalSize = [" + intervalSize + "]");
        return wrapped.metricRegroup(stat, min, max, intervalSize);
    }

    @Override
    public int metricRegroup(int stat, long min, long max, long intervalSize, boolean noGutters) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.metricRegroup: " + "stat = [" + stat + "], min = [" + min + "], max = [" + max + "], intervalSize = [" + intervalSize + "], noGutters = [" + noGutters + "]");
        return wrapped.metricRegroup(stat, min, max, intervalSize, noGutters);
    }

    @Override
    public int metricRegroup2D(int xStat, long xMin, long xMax, long xIntervalSize, int yStat, long yMin, long yMax, long yIntervalSize) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.metricRegroup2D: " + "xStat = [" + xStat + "], xMin = [" + xMin + "], xMax = [" + xMax + "], xIntervalSize = [" + xIntervalSize + "], yStat = [" + yStat + "], yMin = [" + yMin + "], yMax = [" + yMax + "], yIntervalSize = [" + yIntervalSize + "]");
        return wrapped.metricRegroup2D(xStat, xMin, xMax, xIntervalSize, yStat, yMin, yMax, yIntervalSize);
    }

    @Override
    public int metricFilter(int stat, long min, long max, boolean negate) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.metricFilter: " + "stat = [" + stat + "], min = [" + min + "], max = [" + max + "], negate = [" + negate + "]");
        return wrapped.metricFilter(stat, min, max, negate);
    }

    @Override
    public List<TermCount> approximateTopTerms(String field, boolean isIntField, int k) {
        log.info("LoggingImhotepSession.approximateTopTerms: " + "field = [" + field + "], isIntField = [" + isIntField + "], k = [" + k + "]");
        return wrapped.approximateTopTerms(field, isIntField, k);
    }

    @Override
    public int pushStat(String statName) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.pushStat: " + "statName = [" + statName + "]");
        return wrapped.pushStat(statName);
    }

    @Override
    public int pushStats(List<String> statNames) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.pushStats: " + "statNames = [" + statNames + "]");
        return wrapped.pushStats(statNames);
    }

    @Override
    public int popStat() {
        log.info("LoggingImhotepSession.popStat: " + "");
        return wrapped.popStat();
    }

    @Override
    public int getNumStats() {
        log.info("LoggingImhotepSession.getNumStats: " + "");
        return wrapped.getNumStats();
    }

    @Override
    public int getNumGroups() {
        log.info("LoggingImhotepSession.getNumGroups: " + "");
        return wrapped.getNumGroups();
    }

    @Override
    public long getLowerBound(int stat) {
        log.info("LoggingImhotepSession.getLowerBound: " + "stat = [" + stat + "]");
        return wrapped.getLowerBound(stat);
    }

    @Override
    public long getUpperBound(int stat) {
        log.info("LoggingImhotepSession.getUpperBound: " + "stat = [" + stat + "]");
        return wrapped.getUpperBound(stat);
    }

    @Override
    public void createDynamicMetric(String name) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.createDynamicMetric: " + "name = [" + name + "]");
        wrapped.createDynamicMetric(name);
    }

    @Override
    public void updateDynamicMetric(String name, int[] deltas) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.updateDynamicMetric: " + "name = [" + name + "], deltas = [" + Arrays.toString(deltas) + "]");
        wrapped.updateDynamicMetric(name, deltas);
    }

    @Override
    public void conditionalUpdateDynamicMetric(String name, RegroupCondition[] conditions, int[] deltas) {
        log.info("LoggingImhotepSession.conditionalUpdateDynamicMetric: " + "name = [" + name + "], conditions = [" + Arrays.toString(conditions) + "], deltas = [" + Arrays.toString(deltas) + "]");
        wrapped.conditionalUpdateDynamicMetric(name, conditions, deltas);
    }

    @Override
    public void groupConditionalUpdateDynamicMetric(String name, int[] groups, RegroupCondition[] conditions, int[] deltas) {
        log.info("LoggingImhotepSession.groupConditionalUpdateDynamicMetric: " + "name = [" + name + "], groups = [" + Arrays.toString(groups) + "], conditions = [" + Arrays.toString(conditions) + "], deltas = [" + Arrays.toString(deltas) + "]");
        wrapped.groupConditionalUpdateDynamicMetric(name, groups, conditions, deltas);
    }

    @Override
    public void groupQueryUpdateDynamicMetric(final String name, final int[] groups, final Query[] conditions, final int[] deltas) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.groupQueryUpdateDynamicMetric: " + "name = [" + name + "], groups = [" + Arrays.toString(groups) + "], conditions = [" + Arrays.toString(conditions) + "], deltas = [" + Arrays.toString(deltas) + "]");
        wrapped.groupQueryUpdateDynamicMetric(name, groups, conditions, deltas);
    }

    @Override
    public void close() {
        log.info("LoggingImhotepSession.close: " + "");
        wrapped.close();
    }

    @Override
    public void resetGroups() throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.resetGroups: " + "");
        wrapped.resetGroups();
    }

    @Override
    public void rebuildAndFilterIndexes(List<String> intFields, List<String> stringFields) throws ImhotepOutOfMemoryException {
        log.info("LoggingImhotepSession.rebuildAndFilterIndexes: " + "intFields = [" + intFields + "], stringFields = [" + stringFields + "]");
        wrapped.rebuildAndFilterIndexes(intFields, stringFields);
    }

    @Override
    public long getNumDocs() {
        log.info("LoggingImhotepSession.getNumDocs: " + "");
        return wrapped.getNumDocs();
    }

    @Override
    public void addObserver(Instrumentation.Observer observer) {
        log.info("LoggingImhotepSession.addObserver: " + "observer = [" + observer + "]");
        wrapped.addObserver(observer);
    }

    @Override
    public void removeObserver(Instrumentation.Observer observer) {
        log.info("LoggingImhotepSession.removeObserver: " + "observer = [" + observer + "]");
        wrapped.removeObserver(observer);
    }

    @Override
    public GroupStatsIterator getGroupStatsIterator(final int stat) {
        log.info("LoggingImhotepSession.getGroupStatsIterator: " + "stat = [" + stat + "]");
        return wrapped.getGroupStatsIterator(stat);
    }


    @Override
    public PerformanceStats getPerformanceStats(boolean reset) {
        log.info("LoggingImhotepSession.getPerformanceStats: " + "reset = [" + reset + "]");
        return wrapped.getPerformanceStats(reset);
    }

    @Override
    public PerformanceStats closeAndGetPerformanceStats() {
        log.info("LoggingImhotepSession.closeAndGetPerformanceStats: " + "");
        return wrapped.closeAndGetPerformanceStats();
    }
}
