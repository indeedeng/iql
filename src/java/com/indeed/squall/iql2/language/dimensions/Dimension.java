package com.indeed.squall.iql2.language.dimensions;

import com.indeed.squall.iql2.language.AggregateMetric;

public class Dimension {
    public final String name;
    public final String expression;
    public final AggregateMetric metric;

    public Dimension(final String name, final String expression, final AggregateMetric metric) {
        this.name = name;
        this.expression = expression;
        this.metric = metric;
    }
}
