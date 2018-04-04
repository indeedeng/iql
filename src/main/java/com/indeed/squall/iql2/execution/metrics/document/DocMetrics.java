package com.indeed.squall.iql2.execution.metrics.document;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

import java.util.List;

public class DocMetrics {
    public static DocMetric fromJson(JsonNode node) {
        switch (node.get("type").textValue()) {
            case "docStats": {
                final JsonNode pushes = node.get("pushes");
                final List<String> statPushes = Lists.newArrayList();
                for (final JsonNode push : pushes) {
                    statPushes.add(push.textValue());
                }
                return new DocMetric.BaseMetric(statPushes);
            }
        }
        throw new RuntimeException("Oops: " + node);
    }
}
