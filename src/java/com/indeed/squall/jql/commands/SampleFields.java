package com.indeed.squall.jql.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.jql.Session;

import java.util.List;
import java.util.Map;

public class SampleFields {
    private final Map<String, List<SampleDefinition>> perDatasetSamples;

    public SampleFields(Map<String, List<SampleDefinition>> perDatasetSamples) {
        this.perDatasetSamples = perDatasetSamples;
    }

    public void execute(Session session) throws ImhotepOutOfMemoryException {
        for (final Map.Entry<String, List<SampleDefinition>> entry : perDatasetSamples.entrySet()) {
            final String dataset = entry.getKey();
            final List<SampleDefinition> samples = entry.getValue();
            final ImhotepSession s = session.getSessionsMapRaw().get(dataset);
            for (final SampleDefinition sample : samples) {
                s.randomRegroup(sample.field, session.isIntField(sample.field), sample.seed, sample.fraction, 1, 1, 0);
            }
        }
    }

    public static class SampleDefinition {
        public final String field;
        public final double fraction;
        public final String seed;

        public SampleDefinition(String field, double fraction, String seed) {
            this.field = field;
            this.fraction = fraction;
            this.seed = seed;
        }
    }
}
