package com.indeed.squall.iql2.language.dimensions;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Strings;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocMetric;

@JsonIgnoreProperties({"metric"})
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
        if (!Strings.isNullOrEmpty(expression) && ((metric instanceof AggregateMetric.ImplicitDocStats)
                && (((AggregateMetric.ImplicitDocStats) metric).docMetric instanceof DocMetric.Field))) {
            isAlias = true;
        } else {
            isAlias = false;
        }
    }
}
