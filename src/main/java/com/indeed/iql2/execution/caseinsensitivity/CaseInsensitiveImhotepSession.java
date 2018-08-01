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

package com.indeed.iql2.execution.caseinsensitivity;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.Instrumentation;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.TermCount;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.marshal.ImhotepClientMarshaller;
import com.indeed.imhotep.marshal.ImhotepDaemonMarshaller;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.QueryMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.iql2.execution.WrappingImhotepSession;
import org.apache.commons.codec.binary.Base64;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CaseInsensitiveImhotepSession extends WrappingImhotepSession implements ImhotepSession {
    private final ImhotepSession wrapped;
    private final Map<String, String> upperCaseToActual;

    public CaseInsensitiveImhotepSession(ImhotepSession wrapped, Set<String> fieldNames) {
        super(wrapped);
        this.wrapped = wrapped;

        final Map<String, String> upperCaseToActual = new HashMap<>();
        for (final String fieldName : fieldNames) {
            upperCaseToActual.put(fieldName.toUpperCase(), fieldName);
        }
        upperCaseToActual.put("UNIXTIME", "unixtime");
        upperCaseToActual.put("TIME", "time");
        this.upperCaseToActual = upperCaseToActual;
    }

    // Real logic

    private String rewrite(String field) {
        if (upperCaseToActual.containsKey(field.toUpperCase())) {
            return upperCaseToActual.get(field.toUpperCase());
        }
        return field;
    }

    private RegroupCondition rewriteCondition(RegroupCondition condition) {
        return new RegroupCondition(
                rewrite(condition.field),
                condition.intType,
                condition.intTerm,
                condition.stringTerm,
                condition.inequality
        );
    }

    private GroupMultiRemapRule rewriteMulti(GroupMultiRemapRule rule) {
        return new GroupMultiRemapRule(
                rule.targetGroup,
                rule.negativeGroup,
                rule.positiveGroups,
                rewriteCondition(rule.conditions)
        );
    }

    private GroupRemapRule rewriteSingle(GroupRemapRule rule) {
        return new GroupRemapRule(
                rule.targetGroup,
                rewriteCondition(rule.condition),
                rule.negativeGroup,
                rule.positiveGroup
        );
    }

    private QueryRemapRule rewriteQuery(QueryRemapRule rule) {
        return new QueryRemapRule(
                rule.getTargetGroup(),
                rewriteQuery(rule.getQuery()),
                rule.getNegativeGroup(),
                rule.getPositiveGroup()
        );
    }

    private Query rewriteQuery(Query query) {
        switch (query.getQueryType()) {
            case TERM:
                return Query.newTermQuery(rewrite(query.getStartTerm()));
            case BOOLEAN:
                final List<Query> operands = new ArrayList<>(query.getOperands().size());
                for (final Query operand : query.getOperands()) {
                    operands.add(rewriteQuery(operand));
                }
                return Query.newBooleanQuery(query.getOperator(), operands);
            case RANGE:
                return Query.newRangeQuery(rewrite(query.getStartTerm()), rewrite(query.getEndTerm()), query.isMaxInclusive());
            default:
                throw new IllegalArgumentException();
        }
    }

    private Term rewrite(Term term) {
        if (term.isIntField()) {
            return Term.intTerm(rewrite(term.getFieldName()), term.getTermIntVal());
        } else {
            return Term.stringTerm(rewrite(term.getFieldName()), term.getTermStringVal());
        }
    }

    private static final Pattern REGEXMATCH_COMMAND = Pattern.compile("regexmatch\\s+(\\w+)\\s+([0-9]+)\\s(.+)");

    private String rewriteStat(String statName) {
        if (upperCaseToActual.containsKey(statName.toUpperCase())) {
            return rewrite(statName);
        } else if (statName.startsWith("hasstr ")) {
            return handlePrefixSeparated(statName, "hasstr ");
        } else if (statName.startsWith("hasint ")) {
            return handlePrefixSeparated(statName, "hasint ");
        } else if (statName.startsWith("regex ")) {
            return handlePrefixSeparated(statName, "regex ");
        } else if (statName.startsWith("hasintfield ")) {
            return handlePrefix(statName, "hasintfield ");
        } else if (statName.startsWith("hasstrfield ")) {
            return handlePrefix(statName, "hasstrfield ");
        } else if (statName.startsWith("inttermcount ")) {
            return handlePrefix(statName, "inttermcount ");
        } else if (statName.startsWith("strtermcount ")) {
            return handlePrefix(statName, "strtermcount ");
        } else if (statName.startsWith("len ")) {
            return handlePrefix(statName, "len ");
        } else if (statName.startsWith("floatscale ")) {
            final int multIndex = statName.indexOf('*');
            return "floatscale " + rewrite(statName.substring("floatscale ".length(), multIndex)) + statName.substring(multIndex);
        } else if (statName.startsWith("regexmatch ")) {
            final Matcher matcher = REGEXMATCH_COMMAND.matcher(statName);
            if (matcher.matches()) {
                final String field = matcher.group(1);
                return "regexmatch " + rewrite(field) + " " + matcher.group(2) + " " + matcher.group(3);
            } else {
                throw new IllegalArgumentException("Invalid regexmatch command: [" + statName + "]");
            }
        } else if (statName.startsWith("lucene ")) {
            final String queryBase64 = statName.substring(7);
            final byte[] queryBytes = Base64.decodeBase64(queryBase64.getBytes());
            final QueryMessage queryMessage;
            try {
                queryMessage = QueryMessage.parseFrom(queryBytes);
            } catch (InvalidProtocolBufferException e) {
                throw Throwables.propagate(e);
            }
            final Query query = ImhotepDaemonMarshaller.marshal(queryMessage);
            final Query rewritten = rewriteQuery(query);
            final QueryMessage luceneQueryMessage = ImhotepClientMarshaller.marshal(rewritten);
            return "lucene " + Base64.encodeBase64String(luceneQueryMessage.toByteArray());
        } else if (statName.startsWith("fieldequal ")) {
            final String query = statName.substring("fieldequal ".length());
            final String[] fields = query.split("=");
            if (fields.length != 2) {
                throw new IllegalArgumentException("Invalid fieldequal command: [" + statName + "]");
            }
            return "fieldequal " + rewrite(fields[0]) + "="+ rewrite(fields[1]);
        }
        return statName;
    }

    private String handlePrefixSeparated(String statName, String prefix) {
        if (!statName.startsWith(prefix)) {
            throw new IllegalArgumentException("Wrong prefix! statName = [" + statName + "], prefix = [" + prefix + "]");
        }
        final String rest = statName.substring(prefix.length());
        final int colonIndex = rest.indexOf(':');
        if (colonIndex == -1) {
            throw new IllegalArgumentException("Invalid separated field:term: [" + rest + "]");
        }
        final String fieldName = rest.substring(0, colonIndex);
        return prefix + rewrite(fieldName) + rest.substring(colonIndex);
    }

    private String handlePrefix(String statName, String prefix) {
        if (!statName.startsWith(prefix)) {
            throw new IllegalArgumentException("Wrong prefix! statName = [" + statName + "], prefix = [" + prefix + "]");
        }
        final String field = statName.substring(prefix.length());
        return prefix + rewrite(field);
    }

    // Mapping real logic over structures

    private String[] rewriteArray(String[] fields) {
        for (int i = 0; i < fields.length; i++) {
            fields[i] = rewrite(fields[i]);
        }
        return fields;
    }

    private List<String> rewriteList(List<String> fields) {
        fields.replaceAll(this::rewrite);
        return fields;
    }

    private <V> Map<String, V> rewriteMap(Map<String, V> map) {
        final Map<String, V> result = Maps.newHashMapWithExpectedSize(map.size());
        for (final Map.Entry<String, V> entry : map.entrySet()) {
            result.put(rewrite(entry.getKey()), entry.getValue());
        }
        return result;
    }

    private RegroupCondition[] rewriteCondition(RegroupCondition[] conditions) {
        for (int i = 0; i < conditions.length; i++) {
            conditions[i] = rewriteCondition(conditions[i]);
        }
        return conditions;
    }

    private GroupMultiRemapRule[] rewriteMulti(GroupMultiRemapRule[] rawRules) {
        for (int i = 0; i < rawRules.length; i++) {
            rawRules[i] = rewriteMulti(rawRules[i]);
        }
        return rawRules;
    }

    private GroupRemapRule[] rewriteSingle(GroupRemapRule[] rawRules) {
        for (int i = 0; i < rawRules.length; i++) {
            rawRules[i] = rewriteSingle(rawRules[i]);
        }
        return rawRules;
    }


    private GroupMultiRemapMessage rewriteProto(GroupMultiRemapMessage message) {
        final GroupMultiRemapMessage.Builder builder = message.toBuilder();
        for (int i = 0; i < builder.getConditionCount(); i++) {
            final RegroupConditionMessage.Builder conditionBuilder = builder.getCondition(i).toBuilder();
            builder.setCondition(i, conditionBuilder.setField(rewrite(conditionBuilder.getField())));
        }
        return builder.build();
    }

    private List<GroupMultiRemapMessage> rewriteProtos(List<GroupMultiRemapMessage> rawRuleMessages) {
        rawRuleMessages.replaceAll(this::rewriteProto);
        return rawRuleMessages;
    }

    // Delegation with rewriting

    @Override
    public long getTotalDocFreq(String[] intFields, String[] stringFields) {
        return wrapped.getTotalDocFreq(rewriteArray(intFields), rewriteArray(stringFields));
    }

    @Override
    public long[] getGroupStats(int stat) {
        return wrapped.getGroupStats(stat);
    }

    @Override
    public FTGSIterator getFTGSIterator(String[] intFields, String[] stringFields) {
        return wrapped.getFTGSIterator(rewriteArray(intFields), rewriteArray(stringFields));
    }

    @Override
    public FTGSIterator getFTGSIterator(String[] intFields, String[] stringFields, long termLimit) {
        return wrapped.getFTGSIterator(rewriteArray(intFields), rewriteArray(stringFields), termLimit);
    }

    @Override
    public FTGSIterator getFTGSIterator(String[] intFields, String[] stringFields, long termLimit, int sortStat) {
        return wrapped.getFTGSIterator(rewriteArray(intFields), rewriteArray(stringFields), termLimit, sortStat);
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(Map<String, long[]> intFields, Map<String, String[]> stringFields) {
        return wrapped.getSubsetFTGSIterator(rewriteMap(intFields), rewriteMap(stringFields));
    }

    @Override
    public FTGSIterator getFTGSIterator(final FTGSParams params) {
        final FTGSParams fixedParams =
                new FTGSParams(
                        rewriteArray(params.intFields),
                        rewriteArray(params.stringFields),
                        params.termLimit,
                        params.sortStat,
                        params.sorted);
        return wrapped.getFTGSIterator(fixedParams);
    }

    @Override
    public GroupStatsIterator getDistinct(String field, boolean isIntField) {
        return wrapped.getDistinct(rewrite(field), isIntField);
    }

    @Override
    public int regroup(GroupMultiRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        return wrapped.regroup(rewriteMulti(rawRules));
    }

    @Override
    public int regroup(int numRawRules, Iterator<GroupMultiRemapRule> rawRules) throws ImhotepOutOfMemoryException {
        return wrapped.regroup(numRawRules, Iterators.transform(rawRules, new Function<GroupMultiRemapRule, GroupMultiRemapRule>() {
            public GroupMultiRemapRule apply(GroupMultiRemapRule input) {
                return rewriteMulti(input);
            }
        }));
    }

    @Override
    public int regroup(GroupMultiRemapRule[] rawRules, boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        return wrapped.regroup(rewriteMulti(rawRules), errorOnCollisions);
    }

    @Override
    public int regroupWithProtos(GroupMultiRemapMessage[] rawRuleMessages, boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        return regroupWithProtos(Arrays.asList(rawRuleMessages), errorOnCollisions);
    }

    public int regroupWithProtos(List<GroupMultiRemapMessage> rawRuleMessages, boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        return wrapped.regroupWithProtos(Iterables.toArray(rewriteProtos(rawRuleMessages), GroupMultiRemapMessage.class), errorOnCollisions);
    }

    @Override
    public int regroup(int numRawRules, Iterator<GroupMultiRemapRule> rawRules, boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        return wrapped.regroup(numRawRules, Iterators.transform(rawRules, new Function<GroupMultiRemapRule, GroupMultiRemapRule>() {
            public GroupMultiRemapRule apply(@Nullable GroupMultiRemapRule input) {
                return rewriteMulti(input);
            }
        }), errorOnCollisions);
    }

    @Override
    public int regroup(GroupRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        return wrapped.regroup(rewriteSingle(rawRules));
    }

    @Override
    public int regroup2(int numRawRules, Iterator<GroupRemapRule> iterator) throws ImhotepOutOfMemoryException {
        return wrapped.regroup2(numRawRules, Iterators.transform(iterator, new Function<GroupRemapRule, GroupRemapRule>() {
            public GroupRemapRule apply(@Nullable GroupRemapRule input) {
                return rewriteSingle(input);
            }
        }));
    }

    @Override
    public int regroup(QueryRemapRule rule) throws ImhotepOutOfMemoryException {
        return wrapped.regroup(rewriteQuery(rule));
    }

    @Override
    public void intOrRegroup(String field, long[] terms, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException {
        wrapped.intOrRegroup(rewrite(field), terms, targetGroup, negativeGroup, positiveGroup);
    }

    @Override
    public void stringOrRegroup(String field, String[] terms, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException {
        wrapped.stringOrRegroup(rewrite(field), terms, targetGroup, negativeGroup, positiveGroup);
    }

    @Override
    public void regexRegroup(String field, String regex, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException {
        wrapped.regexRegroup(rewrite(field), regex, targetGroup, negativeGroup, positiveGroup);
    }

    @Override
    public void randomRegroup(String field, boolean isIntField, String salt, double p, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException {
        wrapped.randomRegroup(rewrite(field), isIntField, salt, p, targetGroup, negativeGroup, positiveGroup);
    }

    @Override
    public void randomMultiRegroup(String field, boolean isIntField, String salt, int targetGroup, double[] percentages, int[] resultGroups) throws ImhotepOutOfMemoryException {
        wrapped.randomMultiRegroup(rewrite(field), isIntField, salt, targetGroup, percentages, resultGroups);
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
    public int metricRegroup(int stat, long min, long max, long intervalSize) throws ImhotepOutOfMemoryException {
        return wrapped.metricRegroup(stat, min, max, intervalSize);
    }

    @Override
    public int metricRegroup(int stat, long min, long max, long intervalSize, boolean noGutters) throws ImhotepOutOfMemoryException {
        return wrapped.metricRegroup(stat, min, max, intervalSize, noGutters);
    }

    @Override
    public int metricRegroup2D(int xStat, long xMin, long xMax, long xIntervalSize, int yStat, long yMin, long yMax, long yIntervalSize) throws ImhotepOutOfMemoryException {
        return wrapped.metricRegroup2D(xStat, xMin, xMax, xIntervalSize, yStat, yMin, yMax, yIntervalSize);
    }

    @Override
    public int metricFilter(int stat, long min, long max, boolean negate) throws ImhotepOutOfMemoryException {
        return wrapped.metricFilter(stat, min, max, negate);
    }

    @Override
    public List<TermCount> approximateTopTerms(String field, boolean isIntField, int k) {
        return wrapped.approximateTopTerms(rewrite(field), isIntField, k);
    }

    @Override
    public int pushStat(String statName) throws ImhotepOutOfMemoryException {
        return wrapped.pushStat(rewriteStat(statName));
    }

    @Override
    public int pushStats(List<String> statNames) throws ImhotepOutOfMemoryException {
        final List<String> rewritten = Lists.newArrayListWithCapacity(statNames.size());
        for (final String statName : statNames) {
            rewritten.add(rewriteStat(statName));
        }
        return wrapped.pushStats(rewritten);
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
    public void createDynamicMetric(String name) throws ImhotepOutOfMemoryException {
        wrapped.createDynamicMetric(name);
    }

    @Override
    public void updateDynamicMetric(String name, int[] deltas) throws ImhotepOutOfMemoryException {
        wrapped.updateDynamicMetric(name, deltas);
    }

    @Override
    public void conditionalUpdateDynamicMetric(String name, RegroupCondition[] conditions, int[] deltas) {
        wrapped.conditionalUpdateDynamicMetric(name, conditions, deltas);
    }

    @Override
    public void groupConditionalUpdateDynamicMetric(String name, int[] groups, RegroupCondition[] conditions, int[] deltas) {
        wrapped.groupConditionalUpdateDynamicMetric(name, groups, conditions, deltas);
    }

    @Override
    public void groupQueryUpdateDynamicMetric(final String name, final int[] groups, final Query[] conditions, final int[] deltas) throws ImhotepOutOfMemoryException {
        final Query[] rewrittenConditions = new Query[conditions.length];
        for (int i = 0; i < conditions.length; i++) {
            rewrittenConditions[i] = rewriteQuery(conditions[i]);
        }
        wrapped.groupQueryUpdateDynamicMetric(name, groups, rewrittenConditions, deltas);
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
    public void rebuildAndFilterIndexes(List<String> intFields, List<String> stringFields) throws ImhotepOutOfMemoryException {
        wrapped.rebuildAndFilterIndexes(rewriteList(intFields), rewriteList(stringFields));
    }

    @Override
    public long getNumDocs() {
        return wrapped.getNumDocs();
    }

    @Override
    public long getLowerBound(int stat) {
        return wrapped.getLowerBound(stat);
    }

    @Override
    public long getUpperBound(int stat) {
        return wrapped.getUpperBound(stat);
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
    public ImhotepSession wrapped() {
        return wrapped;
    }

    @Override
    public GroupStatsIterator getGroupStatsIterator(final int i) {
        return wrapped.getGroupStatsIterator(i);
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
