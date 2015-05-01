package com.indeed.squall.jql.commands;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.jql.Session;

import java.util.List;
import java.util.Map;

public class FilterDocs {
    public final Map<String, List<String>> perDatasetFilterMetric;

    public FilterDocs(Map<String, List<String>> perDatasetFilterMetric) {
        final Map<String, List<String>> copy = Maps.newHashMap();
        for (final Map.Entry<String, List<String>> entry : perDatasetFilterMetric.entrySet()) {
            copy.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
        }
        this.perDatasetFilterMetric = copy;
    }

    public static void filterDocs(FilterDocs filterDocs, Session s) throws ImhotepOutOfMemoryException {
        // TODO: Do these in parallel?
        for (final Map.Entry<String,List<String>> entry : filterDocs.perDatasetFilterMetric.entrySet()) {
            final ImhotepSession session = s.sessions.get(entry.getKey()).session;
            final int index = session.pushStats(entry.getValue());
            if (index != 1) {
                throw new IllegalArgumentException("Didn't end up with 1 stat after pushing in index named \"" + entry.getKey() + "\"");
            }
            session.metricFilter(0, 1, 1, false);
            session.popStat();
        }
    }
}
