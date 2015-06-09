package com.indeed.jql.language.util;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.indeed.jql.language.AggregateFilter;
import com.indeed.jql.language.AggregateMetric;
import com.indeed.jql.language.DocFilter;
import com.indeed.jql.language.DocMetric;

public class Optionals {
    public static Optional<AggregateFilter> transform(Optional<AggregateFilter> filter, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i) {
        if (filter.isPresent()) {
            return Optional.of(filter.get().transform(f, g, h, i));
        } else {
            return filter;
        }
    }
}
