package com.indeed.squall.iql2.execution.metrics.aggregate;

public class Log extends AggregateMetric.Unary {
    public Log(final AggregateMetric value) {
        super(value);
    }

    @Override
    public double eval(final double value) {
        return Math.log(value);
    }
}
