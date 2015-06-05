package com.indeed.jql.language.commands;

import com.indeed.jql.language.AggregateMetric;

import java.util.List;

public class GetGroupStats implements Command {
    public final List<AggregateMetric> metrics;
    public final boolean returnGroupKeys;

    public GetGroupStats(List<AggregateMetric> metrics, boolean returnGroupKeys) {
        this.metrics = metrics;
        this.returnGroupKeys = returnGroupKeys;
    }
}
