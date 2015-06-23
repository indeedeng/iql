package com.indeed.squall.jql.metrics.aggregate;

import com.indeed.squall.jql.Pushable;

/**
 * @author jwolfe
 */
public interface AggregateMetric extends Pushable {
    double[] getGroupStats(long[][] stats, int numGroups);

    double apply(String term, long[] stats, int group);
    double apply(long term, long[] stats, int group);
}
