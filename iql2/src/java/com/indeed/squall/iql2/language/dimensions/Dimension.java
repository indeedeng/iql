package com.indeed.squall.iql2.language.dimensions;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Optional;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocMetric;

public class Dimension {
    public final String name;
    public final String expression;
    public final String description;
    @JsonIgnore
    public final AggregateMetric metric;
    @JsonIgnore
    public final boolean isAlias;

    public Dimension(final String name, final String expression, final String description, final AggregateMetric metric) {
        this.name = name;
        this.expression = expression;
        this.description = description;
        this.metric = metric;
        this.isAlias = isAliasDimension(metric);
    }


    @JsonIgnore
    private boolean isAliasDimension(AggregateMetric metric) {
        return getAliasActualField().isPresent();
    }

    @JsonIgnore
    public Optional<String> getAliasActualField() {
        if ((metric instanceof AggregateMetric.DocStats)
                && (((AggregateMetric.DocStats) metric).docMetric instanceof DocMetric.Field)) {
            return Optional.of(((DocMetric.Field) ((AggregateMetric.DocStats) metric).docMetric).field);
        } else {
            return Optional.absent();
        }
    }
}
