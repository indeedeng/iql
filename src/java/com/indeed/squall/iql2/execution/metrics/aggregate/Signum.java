package com.indeed.squall.iql2.execution.metrics.aggregate;

public class Signum extends AggregateMetric.Unary {
    public Signum(final AggregateMetric value) {
        super(value);
    }

    @Override
    public double eval(final double value) {
        return Math.signum(value);
    }
}
