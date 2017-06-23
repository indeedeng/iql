package com.indeed.squall.iql2.language.dimensions;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.indeed.squall.iql2.language.AggregateMetric;

@JsonIgnoreProperties({"metric", "isAlias"})
public class Dimension {
    public final String name;
    public final String expression;
    public final String description;
    public final AggregateMetric metric;
    public final boolean isAlias;

    public Dimension(final String name, final String expression, final String description, final AggregateMetric metric) {
        this.name = name;
        this.expression = expression;
        this.description = description;
        this.metric = metric;
        this.isAlias = DatasetDimensions.getAlias(metric).isPresent();
    }
}
