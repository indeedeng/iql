package com.indeed.squall.jql.commands;

import com.indeed.squall.jql.metrics.aggregate.AggregateMetric;

public class TopK {
    public final int limit;
    public final AggregateMetric metric;

    TopK(int limit, AggregateMetric metric) {
        this.limit = limit;
        this.metric = metric;
    }
}
