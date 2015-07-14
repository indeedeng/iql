package com.indeed.squall.iql2.execution.commands;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.indeed.squall.iql2.execution.AggregateFilter;
import com.indeed.squall.iql2.execution.AggregateFilters;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetrics;
import com.indeed.squall.iql2.execution.metrics.aggregate.PerGroupConstant;

public class FieldIterateOpts {
    public Optional<Integer> limit = Optional.absent();
    public Optional<TopK> topK = Optional.absent();
    public Optional<AggregateFilter> filter = Optional.absent();

    public void parseFrom(JsonNode options, Function<String, PerGroupConstant> namedMetricLookup) {
        for (final JsonNode option : options) {
            switch (option.get("type").textValue()) {
                case "filter": {
                    this.filter = Optional.of(AggregateFilters.fromJson(option.get("filter"), namedMetricLookup));
                    break;
                }
                case "limit": {
                    this.limit = Optional.of(option.get("k").intValue());
                    break;
                }
                case "top": {
                    final int k = option.get("k").intValue();
                    final AggregateMetric metric = AggregateMetrics.fromJson(option.get("metric"), namedMetricLookup);
                    this.topK = Optional.of(new TopK(k, metric));
                    break;
                }
            }
        }
    }

    public FieldIterateOpts copy() {
        final FieldIterateOpts result = new FieldIterateOpts();
        result.limit = this.limit;
        result.topK = this.topK;
        result.filter = this.filter;
        return result;
    }
}
