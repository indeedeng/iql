package com.indeed.squall.iql2.execution.commands.misc;

import com.google.common.base.Optional;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;

public class TopK {
    public final Optional<Integer> limit;
    public final Optional<AggregateMetric> metric;

    TopK(Optional<Integer> limit, Optional<AggregateMetric> metric) {
        this.limit = limit;
        this.metric = metric;
    }
}
