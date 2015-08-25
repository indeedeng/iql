package com.indeed.squall.iql2.execution.commands;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;

import java.util.List;
import java.util.Map;

public class SampleFields implements Command {
    public final Map<String, List<SampleDefinition>> perDatasetSamples;

    public SampleFields(Map<String, List<SampleDefinition>> perDatasetSamples) {
        this.perDatasetSamples = perDatasetSamples;
    }

    @Override
    public void execute(Session session, Consumer<String> out) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        for (final Map.Entry<String, List<SampleDefinition>> entry : perDatasetSamples.entrySet()) {
            final String dataset = entry.getKey();
            final List<SampleDefinition> samples = entry.getValue();
            final ImhotepSession s = session.getSessionsMapRaw().get(dataset);
            for (final SampleDefinition sample : samples) {
                session.timer.push("randomRegroup");
                s.randomRegroup(sample.field, session.isIntField(sample.field), sample.seed, sample.fraction, 1, 1, 0);
                session.timer.pop();
            }
        }
        out.accept("SampledFields");
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
