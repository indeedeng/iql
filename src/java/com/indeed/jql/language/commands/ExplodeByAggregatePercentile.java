package com.indeed.jql.language.commands;

import com.indeed.jql.language.AggregateMetric;

public class ExplodeByAggregatePercentile implements Command {
    public final String field;
    public final AggregateMetric metric;
    public final int numBuckets;

    public ExplodeByAggregatePercentile(String field, AggregateMetric metric, int numBuckets) {
        this.field = field;
        this.metric = metric;
        this.numBuckets = numBuckets;
    }
}
