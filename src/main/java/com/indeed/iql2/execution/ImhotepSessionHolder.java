package com.indeed.iql2.execution;

import com.indeed.imhotep.AsynchronousRemoteImhotepMultiSession;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.io.RequestTools;
import com.indeed.imhotep.metrics.aggregate.AggregateStatTree;
import com.indeed.imhotep.protobuf.StatsSortOrder;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;

import java.io.Closeable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author aibragimov
 *
 * Class that holds ImhotepSession and do fields aliasing and case correction
 *
 */
public class ImhotepSessionHolder implements Closeable {
    /**
     * NOT the Imhotep dataset name. The IQL dataset name (alias or whatever).
     */
    private final String datasetName;
    private final ImhotepSession session;

    public ImhotepSessionHolder(
            final String datasetName,
            final ImhotepSession session
    ) {
        if (!(session instanceof RemoteImhotepMultiSession) && !(session instanceof AsynchronousRemoteImhotepMultiSession)) {
            throw new IllegalStateException("Must have RemoteImhotepMultiSession or AsynchronousRemoteImhotepMultiSession");
        }
        this.datasetName = datasetName;
        this.session = session;

    }

    public String getDatasetName() {
        return datasetName;
    }

    // delegate methods with field name substitute

    @Deprecated
    public int pushStats(final List<String> statNames) throws ImhotepOutOfMemoryException {
        return session.pushStats(statNames);
    }

    @Deprecated
    public int popStat() {
        return session.popStat();
    }

    public long[] getGroupStats(final List<String> stat) throws ImhotepOutOfMemoryException {
        return session.getGroupStats(stat);
    }

    public int getNumStats() {
        return session.getNumStats();
    }

    public long getNumDocs() {
        return session.getNumDocs();
    }

    public int getNumGroups() {
        return session.getNumGroups();
    }

    public void metricRegroup(
            final List<String> stat,
            final long min,
            final long max,
            final long intervalSize,
            final boolean noGutters) throws ImhotepOutOfMemoryException {
        session.metricRegroup(stat, min, max, intervalSize, noGutters);
    }

    public void randomMetricRegroup(
            final List<String> stat,
            final String salt,
            final double p,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) throws ImhotepOutOfMemoryException {
        session.randomMetricRegroup(stat, salt, p, targetGroup, negativeGroup, positiveGroup);
    }

    public void randomMetricMultiRegroup(
            final List<String> stat,
            final String salt,
            final int targetGroup,
            final double[] percentages,
            final int[] resultGroups) throws ImhotepOutOfMemoryException {
        session.randomMetricMultiRegroup(stat, salt, targetGroup, percentages, resultGroups);
    }

    public void metricFilter(
            final List<String> stat,
            final long min,
            final long max,
            final boolean negate) throws ImhotepOutOfMemoryException {
        session.metricFilter(stat, min, max, negate);
    }

    public void metricFilter(final List<String> stat, final long min, final long max, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        session.metricFilter(stat, min, max, targetGroup, negativeGroup, positiveGroup);
    }

    public void regexRegroup(
            final String field,
            final String regex,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) throws ImhotepOutOfMemoryException {
        session.regexRegroup(field, regex, targetGroup, negativeGroup, positiveGroup);
    }

    public void randomRegroup(
            final String field,
            final boolean isIntField,
            final String salt,
            final double p,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) throws ImhotepOutOfMemoryException {
        session.randomRegroup(field, isIntField, salt, p, targetGroup, negativeGroup, positiveGroup);
    }

    public void intOrRegroup(
            final String field,
            final long[] terms,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) throws ImhotepOutOfMemoryException {
        session.intOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
    }

    public void stringOrRegroup(
            final String field,
            final String[] terms,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) throws ImhotepOutOfMemoryException {
        session.stringOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
    }

    public int regroup(
            final int[] fromGroups,
            final int[] toGroups,
            final boolean filterOutNotTargeted) throws ImhotepOutOfMemoryException {
        return session.regroup(fromGroups, toGroups, filterOutNotTargeted);
    }

    public int regroup(final QueryRemapRule rule) throws ImhotepOutOfMemoryException {
        return session.regroup(rule);
    }

    public FTGSIterator getSubsetFTGSIterator(
            final Map<FieldSet, long[]> intFields,
            final Map<FieldSet, String[]> stringFields,
            final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        final Map<String, long[]> convertedIntFields = convertMap(intFields);
        final Map<String, String[]> convertedStringFields = convertMap(stringFields);
        return session.getSubsetFTGSIterator(convertedIntFields, convertedStringFields, stats);
    }

    public FTGSIterator getFTGSIterator(
            final String[] intFields,
            final String[] stringFields,
            final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        return session.getFTGSIterator(intFields, stringFields, stats);
    }

    public FTGSIterator getFTGSIterator(
            final String[] intFields,
            final String[] stringFields,
            final long termLimit,
            final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        return session.getFTGSIterator(intFields, stringFields, termLimit, stats);
    }

    public FTGSIterator getFTGSIterator(
            final String[] intFields,
            final String[] stringFields,
            final long termLimit,
            final int sortStat,
            final List<List<String>> stats,
            final StatsSortOrder statsSortOrder) throws ImhotepOutOfMemoryException {
        return session.getFTGSIterator(intFields, stringFields, termLimit, sortStat, stats, statsSortOrder);
    }

    public FTGSIterator getFTGSIterator(final FTGSParams params) throws ImhotepOutOfMemoryException {
        final FTGSParams convertedParams = new FTGSParams(
                params.intFields,
                params.stringFields,
                params.termLimit,
                params.sortStat,
                params.sorted,
                params.stats,
                params.statsSortOrder
        );
        return session.getFTGSIterator(convertedParams);
    }

    public GroupStatsIterator getDistinct(final String field, final boolean isIntField) {
        return session.getDistinct(field, isIntField);
    }

    public PerformanceStats closeAndGetPerformanceStats() {
        return session.closeAndGetPerformanceStats();
    }

    public RemoteImhotepMultiSession.SessionField buildSessionField(String field, final List<List<String>> stats) {
        return new RemoteImhotepMultiSession.SessionField(session, field, stats);
    }

    // some useful methods

    public String getSessionId() {
        return session.getSessionId();
    }

    public long getTempFilesBytesWritten() {
        if (session instanceof RemoteImhotepMultiSession) {
            return ((RemoteImhotepMultiSession) session).getTempFilesBytesWritten();
        } else if (session instanceof AsynchronousRemoteImhotepMultiSession) {
            return ((AsynchronousRemoteImhotepMultiSession) session).getTempFilesBytesWritten();
        }
        throw new IllegalStateException("Must have RemoteImhotepMultiSession or AsynchronousRemoteImhotepMultiSession");
    }

    public int regroupWithSender(
            final RequestTools.GroupMultiRemapRuleSender sender,
            final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        if (session instanceof RemoteImhotepMultiSession) {
            return ((RemoteImhotepMultiSession) session).regroupWithRuleSender(sender, errorOnCollisions);
        } else if (session instanceof AsynchronousRemoteImhotepMultiSession) {
            ((AsynchronousRemoteImhotepMultiSession) session).regroupWithRuleSender(sender, errorOnCollisions);
            return -999;
        }
        throw new IllegalStateException("Must have RemoteImhotepMultiSession or AsynchronousRemoteImhotepMultiSession");
    }

    public AggregateStatTree aggregateStat(final int index) {
        return AggregateStatTree.stat(session, index);
    }

    // converting methods

    private <V> Map<String, V> convertMap(final Map<FieldSet, V> original) {
        final Map<String, V> converted = new HashMap<>(original.size());
        for (final Map.Entry<FieldSet, V> entry : original.entrySet()) {
            converted.put(entry.getKey().datasetFieldName(datasetName), entry.getValue());
        }
        return converted;
    }

    @Override
    public void close() {
        session.close();
    }

}