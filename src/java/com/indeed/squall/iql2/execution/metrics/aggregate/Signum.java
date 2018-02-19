package com.indeed.squall.iql2.execution.metrics.aggregate;

// todo: looks like this class is never used, but not 100% sure. Delete after confirming.
public class Signum extends AggregateMetric.Unary {
    public Signum(final AggregateMetric value) {
        super(value);
    }

    @Override
    public double eval(final double value) {
        return Math.signum(value);
    }
}
