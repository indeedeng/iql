package com.indeed.imhotep.web;

import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.util.core.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides a common path for logging query metrics.
 */
public final class QueryMetrics {
    public static void logQueryMetrics(
            final int iqlVersion,
            final String queryType,
            final boolean errorOccurred,
            final boolean systemErrorOccurred,
            final long timeTaken,
            final MetricStatsEmitter metricStatsEmitter
    ) {
        final List<Pair<String, String>> metricTags = new ArrayList<>();
        metricTags.add(new Pair<>("iqlversion", Integer.toString(iqlVersion)));
        metricTags.add(new Pair<>("statement", queryType));
        metricTags.add(new Pair<>("error", errorOccurred ? "1" : "0"));
        metricTags.add(new Pair<>("systemerror", systemErrorOccurred ? "1" : "0"));
        metricStatsEmitter.histogram("query.time.ms", timeTaken, metricTags);
    }
}
