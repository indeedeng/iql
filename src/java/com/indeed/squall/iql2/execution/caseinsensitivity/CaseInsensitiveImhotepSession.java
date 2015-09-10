package com.indeed.squall.iql2.execution.caseinsensitivity;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.TermCount;
import com.indeed.imhotep.api.DocIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RawFTGSIterator;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CaseInsensitiveImhotepSession implements ImhotepSession {
    private final ImhotepSession wrapped;
    private final Map<String, String> upperCaseToActual;

    public CaseInsensitiveImhotepSession(ImhotepSession wrapped, Set<String> fieldNames) {
        this.wrapped = wrapped;

        final Map<String, String> upperCaseToActual = new HashMap<>();
        for (final String fieldName : fieldNames) {
            upperCaseToActual.put(fieldName.toUpperCase(), fieldName);
        }
        this.upperCaseToActual = upperCaseToActual;
    }

    // Real logic

    private String rewrite(String field) {
        return upperCaseToActual.get(field.toUpperCase());
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
        final String[] result = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            result[i] = rewrite(fields[i]);
        }
        return result;
    }

    private List<String> rewriteList(List<String> fields) {
        final List<String> result = Lists.newArrayListWithCapacity(fields.size());
        for (final String field : fields) {
            result.add(rewrite(field));
        }
        return result;
    }

    private <V> Map<String, V> rewriteMap(Map<String, V> map) {
        final Map<String, V> result = Maps.newHashMapWithExpectedSize(map.size());
        for (final Map.Entry<String, V> entry : map.entrySet()) {
            result.put(rewrite(entry.getKey()), entry.getValue());
        }
        return result;
    }

    private RegroupCondition[] rewriteCondition(RegroupCondition[] conditions) {
        final RegroupCondition[] result = new RegroupCondition[conditions.length];
        for (int i = 0; i < conditions.length; i++) {
            result[i] = rewriteCondition(conditions[i]);
        }
        return result;
    }

    private GroupMultiRemapRule[] rewriteMulti(GroupMultiRemapRule[] rawRules) {
        final GroupMultiRemapRule[] result = new GroupMultiRemapRule[rawRules.length];
        for (int i = 0; i < rawRules.length; i++) {
            result[i] = rewriteMulti(rawRules[i]);
        }
        return result;
    }

    private GroupRemapRule[] rewriteSingle(GroupRemapRule[] rawRules) {
        final GroupRemapRule[] result = new GroupRemapRule[rawRules.length];
        for (int i = 0; i < rawRules.length; i++) {
            result[i] = rewriteSingle(rawRules[i]);
        }
        return result;
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
    public FTGSIterator getSubsetFTGSIterator(Map<String, long[]> intFields, Map<String, String[]> stringFields) {
        return wrapped.getSubsetFTGSIterator(rewriteMap(intFields), rewriteMap(stringFields));
    }

    @Override
    public RawFTGSIterator[] getSubsetFTGSIteratorSplits(Map<String, long[]> intFields, Map<String, String[]> stringFields) {
        return wrapped.getSubsetFTGSIteratorSplits(rewriteMap(intFields), rewriteMap(stringFields));
    }

    @Override
    public DocIterator getDocIterator(String[] intFields, String[] stringFields) throws ImhotepOutOfMemoryException {
        return wrapped.getDocIterator(rewriteArray(intFields), rewriteArray(stringFields));
    }

    @Override
    public RawFTGSIterator[] getFTGSIteratorSplits(String[] intFields, String[] stringFields) {
        return wrapped.getFTGSIteratorSplits(rewriteArray(intFields), rewriteArray(stringFields));
    }

    @Override
    public RawFTGSIterator getFTGSIteratorSplit(String[] intFields, String[] stringFields, int splitIndex, int numSplits) {
        return wrapped.getFTGSIteratorSplit(rewriteArray(intFields), rewriteArray(stringFields), splitIndex, numSplits);
    }

    @Override
    public RawFTGSIterator getSubsetFTGSIteratorSplit(Map<String, long[]> intFields, Map<String, String[]> stringFields, int splitIndex, int numSplits) {
        return wrapped.getSubsetFTGSIteratorSplit(rewriteMap(intFields), rewriteMap(stringFields), splitIndex, numSplits);
    }

    @Override
    public RawFTGSIterator mergeFTGSSplit(String[] intFields, String[] stringFields, String sessionId, InetSocketAddress[] nodes, int splitIndex) {
        return wrapped.mergeFTGSSplit(rewriteArray(intFields), rewriteArray(stringFields), sessionId, nodes, splitIndex);
    }

    @Override
    public RawFTGSIterator mergeSubsetFTGSSplit(Map<String, long[]> intFields, Map<String, String[]> stringFields, String sessionId, InetSocketAddress[] nodes, int splitIndex) {
        return wrapped.mergeSubsetFTGSSplit(rewriteMap(intFields), rewriteMap(stringFields), sessionId, nodes, splitIndex);
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
}
