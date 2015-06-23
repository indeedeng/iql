package com.indeed.squall.jql;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.indeed.squall.jql.metrics.aggregate.AggregateMetric;
import com.indeed.squall.jql.metrics.aggregate.AggregateMetrics;
import com.indeed.squall.jql.metrics.aggregate.PerGroupConstant;

public class AggregateFilters {
    public static AggregateFilter fromJson(final JsonNode node, final Function<String, PerGroupConstant> namedMetricLookup) {
        final Supplier<AggregateMetric> m1 = new Supplier<AggregateMetric>() {
            @Override
            public AggregateMetric get() {
                return AggregateMetrics.fromJson(node.get("arg1"), namedMetricLookup);
            }
        };
        final Supplier<AggregateMetric> m2 = new Supplier<AggregateMetric>() {
            @Override
            public AggregateMetric get() {
                return AggregateMetrics.fromJson(node.get("arg2"), namedMetricLookup);
            }
        };
        final Supplier<AggregateFilter> f1 = new Supplier<AggregateFilter>() {
            public AggregateFilter get() {
                return fromJson(node.get("arg1"), namedMetricLookup);
            }
        };
        final Supplier<AggregateFilter> f2 = new Supplier<AggregateFilter>() {
            public AggregateFilter get() {
                return fromJson(node.get("arg2"), namedMetricLookup);
            }
        };
        switch (node.get("type").textValue()) {
            case "termEquals":
                return new AggregateFilter.TermEquals(Term.fromJson(node.get("value")));
            case "not":
                return new AggregateFilter.Not(fromJson(node.get("value"), namedMetricLookup));
            case "regex":
                return new AggregateFilter.RegexFilter(node.get("field").textValue(), node.get("value").textValue());
            case "metricEquals":
                return new AggregateFilter.MetricEquals(m1.get(), m2.get());
            case "metricNotEquals":
                return new AggregateFilter.MetricNotEquals(m1.get(), m2.get());
            case "greaterThan":
                return new AggregateFilter.GreaterThan(m1.get(), m2.get());
            case "greaterThanOrEquals":
                return new AggregateFilter.GreaterThanOrEquals(m1.get(), m2.get());
            case "lessThan":
                return new AggregateFilter.LessThan(m1.get(), m2.get());
            case "lessThanOrEquals":
                return new AggregateFilter.LessThanOrEquals(m1.get(), m2.get());
            case "and":
                return new AggregateFilter.And(f1.get(), f2.get());
            case "or":
                return new AggregateFilter.Or(f1.get(), f2.get());
            case "always":
                return new AggregateFilter.Constant(true);
        }
        throw new RuntimeException("Oops: " + node);
    }
}
