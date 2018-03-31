package com.indeed.squall.iql2.execution.metrics.aggregate;

public class Power extends AggregateMetric.Binary {
    public Power(final AggregateMetric m1, final AggregateMetric m2) {
        super(m1, m2);
    }

    @Override
    double eval(final double left, final double right) {
        return Math.pow(left, right);
    }
}
