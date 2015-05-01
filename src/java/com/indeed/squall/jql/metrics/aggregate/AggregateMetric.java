package com.indeed.squall.jql.metrics.aggregate;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.indeed.squall.jql.QualifiedPush;
import com.indeed.squall.jql.Session;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author jwolfe
 */
public interface AggregateMetric {
    Set<QualifiedPush> requires();

    void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys);
    double apply(String term, long[] stats, int group);
    double apply(long term, long[] stats, int group);

    static AggregateMetric fromJson(JsonNode node, Function<String, PerGroupConstant> namedMetricLookup) {
        final Supplier<AggregateMetric> m1 = () -> AggregateMetric.fromJson(node.get("m1"), namedMetricLookup);
        final Supplier<AggregateMetric> m2 = () -> AggregateMetric.fromJson(node.get("m2"), namedMetricLookup);
        final Supplier<AggregateMetric> value = () -> AggregateMetric.fromJson(node.get("value"), namedMetricLookup);
        switch (node.get("type").textValue()) {
            case "docStats": {
                return createPushMetric(node.get("sessionName").textValue(), node.get("pushes"));
            }
            case "groupStatsLookup": {
                return namedMetricLookup.apply(node.get("name").textValue());
            }
            case "addition":
                return new Add(m1.get(), m2.get());
            case "subtraction":
                return new Subtract(m1.get(), m2.get());
            case "division":
                return new Divide(m1.get(), m2.get());
            case "multiplication":
                return new Multiply(m1.get(), m2.get());
            case "power":
                return new Power(m1.get(), m2.get());
            case "abs":
                return new Abs(value.get());
            case "signum":
                return new Signum(value.get());
            case "log":
                return new Log(value.get());
            case "constant":
                return new Constant(node.get("value").doubleValue());
            case "perGroupConstants": {
                final JsonNode constantsArray = node.get("values");
                final double[] perGroupConstants = new double[constantsArray.size() + 1];
                for (int i = 0; i < constantsArray.size(); i++) {
                    perGroupConstants[i + 1] = constantsArray.get(i).doubleValue();
                }
                return new PerGroupConstant(perGroupConstants);
            }
            case "lag": {
                final int delay = node.get("delay").intValue();
                final AggregateMetric metric = AggregateMetric.fromJson(node.get("m"), namedMetricLookup);
                return new ParentLag(delay, metric);
            }
            case "iterateLag": {
                final int delay = node.get("delay").intValue();
                final AggregateMetric metric = AggregateMetric.fromJson(node.get("m"), namedMetricLookup);
                return new IterateLag(delay, metric);
            }
            case "modulus": {
                return new Modulus(m1.get(), m2.get());
            }
            case "running": {
                return new Running(value.get(), node.get("offset").intValue());
            }
            case "window": {
                return new Window(node.get("size").intValue(), value.get());
            }
        }
        throw new RuntimeException("Oops: " + node);
    }

    static AggregateMetric createPushMetric(String sessionName, JsonNode pushes1) {
        final List<String> pushes = Lists.newArrayList();
        for (final JsonNode push : pushes1) {
            pushes.add(push.textValue());
        }
        return new DocumentLevelMetric(sessionName, pushes);
    }
}
