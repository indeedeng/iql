package com.indeed.squall.iql2.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.indeed.squall.iql2.execution.metrics.document.DocMetric;
import com.indeed.squall.iql2.execution.metrics.document.DocMetrics;

import java.util.List;

public class DocFilters {
    public static DocFilter fromJson(final JsonNode node) {
        final Supplier<DocMetric> m1 = new Supplier<DocMetric>() {
            public DocMetric get() {
                return DocMetrics.fromJson(node.get("arg1"));
            }
        };
        final Supplier<DocMetric> m2 = new Supplier<DocMetric>() {
            public DocMetric get() {
                return DocMetrics.fromJson(node.get("arg2"));
            }
        };
        final Supplier<DocFilter> f1 = new Supplier<DocFilter>() {
            public DocFilter get() {
                return fromJson(node.get("arg1"));
            }
        };
        final Supplier<DocFilter> f2 = new Supplier<DocFilter>() {
            public DocFilter get() {
                return fromJson(node.get("arg2"));
            }
        };
        switch (node.get("type").textValue()) {
            case "fieldEquals":
                return new DocFilter.FieldEquals(node.get("field").textValue(), Term.fromJson(node.get("value")));
            case "fieldNotEquals":
                return new DocFilter.FieldNotEquals(node.get("field").textValue(), Term.fromJson(node.get("value")));
            case "not":
                return new DocFilter.Not(fromJson(node.get("value")));
            case "regex":
                return new DocFilter.RegexFilter(node.get("field").textValue(), node.get("value").textValue(), false);
            case "notRegex":
                return new DocFilter.RegexFilter(node.get("field").textValue(), node.get("value").textValue(), true);
            case "metricEquals":
                return new DocFilter.MetricEquals(m1.get(), m2.get());
            case "greaterThan":
                return new DocFilter.GreaterThan(m1.get(), m2.get());
            case "lessThan":
                return new DocFilter.LessThan(m1.get(), m2.get());
            case "and":
                return new DocFilter.And(f1.get(), f2.get());
            case "or":
                return new DocFilter.Or(f1.get(), f2.get());
            case "qualified": {
                final List<String> names = Lists.newArrayList();
                final JsonNode namesArr = node.get("names");
                for (int i = 0; i < namesArr.size(); i++) {
                    names.add(namesArr.get(i).textValue());
                }
                return new DocFilter.QualifiedFilter(names, fromJson(node.get("filter")));
            }
        }
        throw new RuntimeException("Oops: " + node);
    }
}
