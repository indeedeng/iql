package com.indeed.squall.iql2.execution.metrics.aggregate;

public class Add extends AggregateMetric.Binary {
    public Add(final AggregateMetric m1, final AggregateMetric m2) {
        super(m1, m2);
    }

    @Override
    double eval(final double left, final double right) {
        return left + right;
    }
}
