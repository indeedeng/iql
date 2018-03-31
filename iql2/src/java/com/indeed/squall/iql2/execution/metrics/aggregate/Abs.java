package com.indeed.squall.iql2.execution.metrics.aggregate;

public class Abs extends AggregateMetric.Unary {
    public Abs(final AggregateMetric value) {
        super(value);
    }

    @Override
    public double eval(final double value) {
        return Math.abs(value);
    }
}
