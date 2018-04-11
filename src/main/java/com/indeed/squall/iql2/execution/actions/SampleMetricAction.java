package com.indeed.squall.iql2.execution.actions;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.SessionCallback;
import com.indeed.util.core.TreeTimer;

import java.util.List;
import java.util.Map;

public class SampleMetricAction implements Action {
    public final Map<String, List<String>> perDatasetMetric;
    public final double probability;
    public final String seed;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public SampleMetricAction(final Map<String, List<String>> perDatasetMetric,
                              final double probability,
                              final String seed,
                              final int targetGroup,
                              final int positiveGroup,
                              final int negativeGroup) {
        this.perDatasetMetric = perDatasetMetric;
        this.probability = probability;
        this.seed = seed;
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void apply(final Session session) throws ImhotepOutOfMemoryException {
        if ((targetGroup == 1) && (session.numGroups == 1) && ((positiveGroup == 1 && negativeGroup == 0) || (positiveGroup == 0 && negativeGroup == 1))) {
            // TODO: Parallelize
            session.process(new SessionCallback() {
                @Override
                public void handle(final TreeTimer timer, final String name, final ImhotepSession session) throws ImhotepOutOfMemoryException {
                    if (!perDatasetMetric.containsKey(name)) {
                        return;
                    }
                    final List<String> pushes = perDatasetMetric.get(name);

                    timer.push("pushStats");
                    final int numStats = session.pushStats(pushes);
                    timer.pop();

                    if (numStats != 1) {
                        throw new IllegalStateException("Pushed more than one stat!: " + pushes);
                    }

                    timer.push("randomMetricRegroup");
                    session.randomMetricRegroup(0, seed, probability, targetGroup, negativeGroup, positiveGroup);
                    timer.pop();

                    timer.push("popStat");
                    session.popStat();
                    timer.pop();
                }
            });
        } else {
            throw new UnsupportedOperationException("Can only do SampleMetricAction filters when negativeGroup or positive group > 1.");
        }
    }

    @Override
    public String toString() {
        return "SampleMetricAction{" +
                "perDatasetMetric=" + perDatasetMetric +
                ", probability=" + probability +
                ", seed='" + seed + '\'' +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }
}
