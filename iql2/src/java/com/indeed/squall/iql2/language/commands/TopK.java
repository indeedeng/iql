package com.indeed.squall.iql2.language.commands;

import com.google.common.base.Optional;
import com.indeed.squall.iql2.language.AggregateMetric;

import java.util.Objects;

public class TopK {
    public final Optional<Long> limit;
    public final Optional<AggregateMetric> metric;

    public TopK(Optional<Long> limit, Optional<AggregateMetric> metric) {
        this.limit = limit;
        this.metric = metric;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopK topK = (TopK) o;
        return Objects.equals(limit, topK.limit) &&
                Objects.equals(metric, topK.metric);
    }

    @Override
    public int hashCode() {
        return Objects.hash(limit, metric);
    }

    @Override
    public String toString() {
        return "TopK{" +
                "limit=" + limit +
                ", metric=" + metric +
                '}';
    }
}
