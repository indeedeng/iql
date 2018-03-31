package com.indeed.squall.iql2.execution.metrics.aggregate;

public class Modulus extends AggregateMetric.Binary {
    public Modulus(final AggregateMetric left, final AggregateMetric right) {
        super(left, right);
    }

    @Override
    double eval(final double left, final double right) {
        return left % right;
    }
}
