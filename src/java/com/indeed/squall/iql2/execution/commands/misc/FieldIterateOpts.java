package com.indeed.squall.iql2.execution.commands.misc;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.indeed.squall.iql2.execution.AggregateFilter;
import com.indeed.squall.iql2.execution.AggregateFilters;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetrics;
import com.indeed.squall.iql2.execution.metrics.aggregate.PerGroupConstant;

import java.util.Arrays;

public class FieldIterateOpts {
    public Optional<Integer> limit = Optional.absent();
    public Optional<TopK> topK = Optional.absent();
    public Optional<AggregateFilter> filter = Optional.absent();
    public Optional<long[]> sortedIntTermSubset = Optional.absent();
    public Optional<String[]> sortedStringTermSubset = Optional.absent();

    public void parseFrom(JsonNode options, Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, boolean allowTermSubsets) {
        for (final JsonNode option : options) {
            switch (option.get("type").textValue()) {
                case "filter": {
                    this.filter = Optional.of(AggregateFilters.fromJson(option.get("filter"), namedMetricLookup, groupKeySet));
                    break;
                }
                case "limit": {
                    this.limit = Optional.of(option.get("k").intValue());
                    break;
                }
                case "top": {
                    final Optional<Integer> k;
                    if (option.has("k")) {
                        k = Optional.of(option.get("k").intValue());
                    } else {
                        k = Optional.absent();
                    }
                    final Optional<AggregateMetric> metric;
                    if (option.has("metric")) {
                        metric = Optional.of(AggregateMetrics.fromJson(option.get("metric"), namedMetricLookup, groupKeySet));
                    } else {
                        metric = Optional.absent();
                    }
                    this.topK = Optional.of(new TopK(k, metric));
                    break;
                }
                case "intTermSubset": {
                    if (!allowTermSubsets) {
                        throw new IllegalStateException("term subsets not valid in this context");
                    }
                    final JsonNode node = option.get("terms");
                    final long[] longTermSubset = new long[node.size()];
                    for (int i = 0; i < node.size(); i++) {
                        longTermSubset[i] = node.get(i).longValue();
                    }
                    Arrays.sort(longTermSubset);
                    this.sortedIntTermSubset = Optional.of(longTermSubset);
                    break;
                }
                case "stringTermSubset": {
                    if (!allowTermSubsets) {
                        throw new IllegalStateException("term subsets not valid in this context");
                    }
                    final JsonNode node = option.get("terms");
                    final String[] stringTermSubset = new String[node.size()];
                    for (int i = 0; i < node.size(); i++) {
                        stringTermSubset[i] = node.get(i).textValue();
                    }
                    Arrays.sort(stringTermSubset);
                    this.sortedStringTermSubset = Optional.of(stringTermSubset);
                    break;
                }
                default: {
                    throw new IllegalStateException("Unknown option type: " + option.get("type").textValue());
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
