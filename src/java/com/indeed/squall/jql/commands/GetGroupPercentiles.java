package com.indeed.squall.jql.commands;

import com.google.common.collect.Maps;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.jql.Session;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.LongStream;

public class GetGroupPercentiles {
    public final Set<String> scope;
    public final String field;
    public final double[] percentiles;

    public GetGroupPercentiles(Set<String> scope, String field, double[] percentiles) {
        this.scope = scope;
        this.field = field;
        this.percentiles = percentiles;
    }

    public long[][] execute(Session session) throws ImhotepOutOfMemoryException, IOException {
        final String field = this.field;
        final double[] percentiles = this.percentiles;
        final long[] counts = new long[session.numGroups + 1];
        final Map<String, ImhotepSession> sessionsSubset = Maps.newHashMap();
        this.scope.forEach(s -> sessionsSubset.put(s, session.sessions.get(s).session));
        sessionsSubset.values().forEach(s -> Session.unchecked(() -> {
            s.pushStat("count()");
            final long[] stats = s.getGroupStats(0);
            for (int i = 0; i < stats.length; i++) {
                counts[i] += stats[i];
            }
        }));
        final Map<String, IntList> metricMapping = Maps.newHashMap();
        int index = 0;
        for (final String sessionName : sessionsSubset.keySet()) {
            metricMapping.put(sessionName, IntLists.singleton(index++));
        }
        final double[][] requiredCounts = new double[counts.length][];
        for (int i = 1; i < counts.length; i++) {
            requiredCounts[i] = new double[percentiles.length];
            for (int j = 0; j < percentiles.length; j++) {
                requiredCounts[i][j] = (percentiles[j] / 100.0) * (double)counts[i];
            }
        }
        final long[][] results = new long[percentiles.length][counts.length - 1];
        final long[] runningCounts = new long[counts.length];
        Session.iterateMultiInt(sessionsSubset, metricMapping, field, (term, stats, group) -> {
            final long oldCount = runningCounts[group];
            final long termCount = LongStream.of(stats).sum();
            final long newCount = oldCount + termCount;

            final double[] groupRequiredCountsArray = requiredCounts[group];
            for (int i = 0; i < percentiles.length; i++) {
                final double minRequired = groupRequiredCountsArray[i];
                if (newCount >= minRequired && oldCount < minRequired) {
                    results[i][group - 1] = term;
                }
            }

            runningCounts[group] = newCount;
        });

        sessionsSubset.values().forEach(ImhotepSession::popStat);
        return results;
    }
}
