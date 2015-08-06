package com.indeed.squall.iql2.execution.commands.misc;

import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;

public class TopK {
    public final int limit;
    public final AggregateMetric metric;

    TopK(int limit, AggregateMetric metric) {
        this.limit = limit;
        this.metric = metric;
    }
}
