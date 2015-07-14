package com.indeed.squall.iql2.execution.metrics.aggregate;

import com.indeed.squall.iql2.execution.Pushable;

/**
 * @author jwolfe
 */
public interface AggregateMetric extends Pushable {
    double[] getGroupStats(long[][] stats, int numGroups);

    double apply(String term, long[] stats, int group);
    double apply(long term, long[] stats, int group);
}
