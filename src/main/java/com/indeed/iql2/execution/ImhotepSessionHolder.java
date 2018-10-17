package com.indeed.iql2.execution;

import com.google.common.base.Throwables;
import com.google.protobuf.InvalidProtocolBufferException;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.io.RequestTools;
import com.indeed.imhotep.marshal.ImhotepClientMarshaller;
import com.indeed.imhotep.marshal.ImhotepDaemonMarshaller;
import com.indeed.imhotep.metrics.aggregate.AggregateStatTree;
import com.indeed.imhotep.protobuf.QueryMessage;
import org.apache.commons.codec.binary.Base64;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author aibragimov
 *
 * Class that holds ImhotepSession and do fields aliasing and case correction
 *
 */
public class ImhotepSessionHolder implements Closeable {
    private final RemoteImhotepMultiSession session;
    // map from uppercase alias to real field
    private final Map<String, String> aliases;
    // map from uppercase field name to real field name
    private final Map<String, String> upperCaseToActual;

    public ImhotepSessionHolder(
            final RemoteImhotepMultiSession session,
            final Map<String, String> fieldAliases,
            final Set<String> fieldNames) {
        this.session = session;
        aliases = new HashMap<>();
        for (final Map.Entry<String, String> entry : fieldAliases.entrySet()) {
            aliases.put(entry.getKey().toUpperCase(), entry.getValue());
        }
        upperCaseToActual = new HashMap<>();
        for (final String field : fieldNames) {
            upperCaseToActual.put(field.toUpperCase(), field);
        }
    }

    // delegate methods with field name substitute

    public int pushStat(final String statName) throws ImhotepOutOfMemoryException {
        final String converted = rewriteStat(statName);
        return session.pushStat(converted);
    }

    public int pushStats(final List<String> statNames) throws ImhotepOutOfMemoryException {
        final List<String> converted = new ArrayList<>(statNames);
        converted.replaceAll(this::rewriteStat);
        return session.pushStats(converted);
    }

    public int popStat() {
        return session.popStat();
    }

    public long[] getGroupStats(final int stat) {
        return session.getGroupStats(stat);
    }

    public int getNumStats() {
        return session.getNumStats();
    }

    public long getNumDocs() {
        return session.getNumDocs();
    }

    public void metricRegroup(
            final int stat,
            final long min,
            final long max,
            final long intervalSize,
            final boolean noGutters) throws ImhotepOutOfMemoryException {
        session.metricRegroup(stat, min, max, intervalSize, noGutters);
    }

    public void randomMetricRegroup(
            final int stat,
            final String salt,
            final double p,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) throws ImhotepOutOfMemoryException {
        session.randomMetricRegroup(stat, salt, p, targetGroup, negativeGroup, positiveGroup);
    }

    public void randomMetricMultiRegroup(
            final int stat,
            final String salt,
            final int targetGroup,
            final double[] percentages,
            final int[] resultGroups) throws ImhotepOutOfMemoryException {
        session.randomMetricMultiRegroup(stat, salt, targetGroup, percentages, resultGroups);
    }

    public void metricFilter(
            final int stat,
            final long min,
            final long max,
            final boolean negate) throws ImhotepOutOfMemoryException {
        session.metricFilter(stat, min, max, negate);
    }

    public void regexRegroup(
            final String field,
            final String regex,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) throws ImhotepOutOfMemoryException {
        session.regexRegroup(convertField(field), regex, targetGroup, negativeGroup, positiveGroup);
    }

    public void randomRegroup(
            final String field,
            final boolean isIntField,
            final String salt,
            final double p,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) throws ImhotepOutOfMemoryException {
        session.randomRegroup(convertField(field), isIntField, salt, p, targetGroup, negativeGroup, positiveGroup);
    }

    public void randomMultiRegroup(
            final String field,
            final boolean isIntField,
            final String salt,
            final int targetGroup,
            final double[] percentages,
            final int[] resultGroups) throws ImhotepOutOfMemoryException {
        session.randomMultiRegroup(convertField(field), isIntField, salt, targetGroup, percentages, resultGroups);
    }

    public void intOrRegroup(
            final String field,
            final long[] terms,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) throws ImhotepOutOfMemoryException {
        session.intOrRegroup(convertField(field), terms, targetGroup, negativeGroup, positiveGroup);
    }

    public void stringOrRegroup(
            final String field,
            final String[] terms,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) throws ImhotepOutOfMemoryException {
        session.stringOrRegroup(convertField(field), terms, targetGroup, negativeGroup, positiveGroup);
    }

    public int regroup(
            final int[] fromGroups,
            final int[] toGroups,
            final boolean filterOutNotTargeted) throws ImhotepOutOfMemoryException {
        return session.regroup(fromGroups, toGroups, filterOutNotTargeted);
    }

    public int regroup(final QueryRemapRule rule) throws ImhotepOutOfMemoryException {
        return session.regroup(rewrite(rule));
    }

    public FTGSIterator getSubsetFTGSIterator(
            final Map<String, long[]> intFields,
            final Map<String, String[]> stringFields) {
        final Map<String, long[]> convertedIntFields = convertMap(intFields);
        final Map<String, String[]> convertedStringFields = convertMap(stringFields);
        return session.getSubsetFTGSIterator(convertedIntFields, convertedStringFields);
    }

    public FTGSIterator getFTGSIterator(
            final String[] intFields,
            final String[] stringFields) {
        return session.getFTGSIterator(convertArray(intFields), convertArray(stringFields));
    }

    public FTGSIterator getFTGSIterator(
            final String[] intFields,
            final String[] stringFields,
            final long termLimit) {
        return session.getFTGSIterator(convertArray(intFields), convertArray(stringFields), termLimit);
    }

    public FTGSIterator getFTGSIterator(
            final String[] intFields,
            final String[] stringFields,
            final long termLimit,
            final int sortStat) {
        return session.getFTGSIterator(convertArray(intFields), convertArray(stringFields), termLimit, sortStat);
    }

    public FTGSIterator getFTGSIterator(final FTGSParams params) {
        final FTGSParams convertedParams = new FTGSParams(
                convertArray(params.intFields),
                convertArray(params.stringFields),
                params.termLimit,
                params.sortStat,
                params.sorted);
        return session.getFTGSIterator(convertedParams);
    }

    public GroupStatsIterator getDistinct(final String field, final boolean isIntField) {
        return session.getDistinct(convertField(field), isIntField);
    }

    public PerformanceStats closeAndGetPerformanceStats() {
        return session.closeAndGetPerformanceStats();
    }

    public RemoteImhotepMultiSession.SessionField buildSessionField(String field) {
        return new RemoteImhotepMultiSession.SessionField(session, convertField(field));
    }

    // some useful methods

    public String getSessionId() {
        return session.getSessionId();
    }

    public long getTempFilesBytesWritten() {
        return session.getTempFilesBytesWritten();
    }

    public int regroupWithSender(
            final RequestTools.GroupMultiRemapRuleSender sender,
            final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        return session.regroupWithRuleSender(sender, errorOnCollisions);
    }

    public AggregateStatTree aggregateStat(final int index) {
        return AggregateStatTree.stat(session, index);
    }

    // converting methods

    public String convertField(final String field) {
        final String aliasResolved = aliases.getOrDefault(field.toUpperCase(), field);
        final String actual = upperCaseToActual.getOrDefault(aliasResolved.toUpperCase(), aliasResolved);
        return actual;
    }

    private boolean isFieldName(final String field) {
        final String aliasResolved = aliases.getOrDefault(field.toUpperCase(), field);
        return upperCaseToActual.containsKey(aliasResolved.toUpperCase());
    }

    private String[] convertArray(final String[] fields) {
        final String[] converted = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            converted[i] = convertField(fields[i]);
        }
        return converted;
    }

    private <V> Map<String, V> convertMap(final Map<String, V> original) {
        final Map<String, V> converted = new HashMap<>(original.size());
        for (final Map.Entry<String, V> entry : original.entrySet()) {
            converted.put(convertField(entry.getKey()), entry.getValue());
        }
        return converted;
    }

    private static final Pattern REGEXMATCH_COMMAND = Pattern.compile("regexmatch\\s+(\\w+)\\s+([0-9]+)\\s(.+)");

    private String rewriteStat(final String statName) {
        if (isFieldName(statName)) {
            return convertField(statName);
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
            return "floatscale " + convertField(statName.substring("floatscale ".length(), multIndex)) + statName.substring(multIndex);
        } else if (statName.startsWith("regexmatch ")) {
            final Matcher matcher = REGEXMATCH_COMMAND.matcher(statName);
            if (matcher.matches()) {
                final String field = matcher.group(1);
                return "regexmatch " + convertField(field) + " " + matcher.group(2) + " " + matcher.group(3);
            } else {
                throw new IllegalArgumentException("Invalid regexmatch command: [" + statName + "]");
            }
        } else if (statName.startsWith("lucene ")) {
            final String queryBase64 = statName.substring(7);
            final byte[] queryBytes = Base64.decodeBase64(queryBase64.getBytes());
            final QueryMessage queryMessage;
            try {
                queryMessage = QueryMessage.parseFrom(queryBytes);
            } catch (final InvalidProtocolBufferException e) {
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
            return "fieldequal " + convertField(fields[0]) + "="+ convertField(fields[1]);
        }
        return statName;
    }

    private String handlePrefixSeparated(final String statName, final String prefix) {
        if (!statName.startsWith(prefix)) {
            throw new IllegalArgumentException("Wrong prefix! statName = [" + statName + "], prefix = [" + prefix + "]");
        }
        final String rest = statName.substring(prefix.length());
        final int colonIndex = rest.indexOf(':');
        if (colonIndex == -1) {
            throw new IllegalArgumentException("Invalid separated field:term: [" + rest + "]");
        }
        final String fieldName = rest.substring(0, colonIndex);
        return prefix + convertField(fieldName) + rest.substring(colonIndex);
    }

    private String handlePrefix(final String statName, final String prefix) {
        if (!statName.startsWith(prefix)) {
            throw new IllegalArgumentException("Wrong prefix! statName = [" + statName + "], prefix = [" + prefix + "]");
        }
        final String field = statName.substring(prefix.length());
        return prefix + convertField(field);
    }

    private Query rewriteQuery(final Query query) {
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

    private Term rewrite(final Term term) {
        if (term.isIntField()) {
            return Term.intTerm(convertField(term.getFieldName()), term.getTermIntVal());
        } else {
            return Term.stringTerm(convertField(term.getFieldName()), term.getTermStringVal());
        }
    }

    private QueryRemapRule rewrite(final QueryRemapRule rule) {
        return new QueryRemapRule(rule.getTargetGroup(), rewriteQuery(rule.getQuery()), rule.getNegativeGroup(), rule.getPositiveGroup());
    }

    @Override
    public void close() {
        session.close();
    }

}