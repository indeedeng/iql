package com.indeed.jql.language.util;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.indeed.jql.language.AggregateFilter;
import com.indeed.jql.language.AggregateMetric;
import com.indeed.jql.language.DocFilter;
import com.indeed.jql.language.DocMetric;
import com.indeed.jql.language.query.GroupBy;

public class Optionals {
    public static Optional<AggregateFilter> transform(Optional<AggregateFilter> filter, Function<AggregateMetric, AggregateMetric> f, Function<DocMetric, DocMetric> g, Function<AggregateFilter, AggregateFilter> h, Function<DocFilter, DocFilter> i, Function<GroupBy, GroupBy> groupByFunction) {
        if (filter.isPresent()) {
            return Optional.of(filter.get().transform(f, g, h, i, groupByFunction));
        } else {
            return filter;
        }
    }

    public static Optional<AggregateFilter> traverse1(Optional<AggregateFilter> filter, Function<AggregateMetric, AggregateMetric> f) {
        if (filter.isPresent()) {
            return Optional.of(filter.get().traverse1(f));
        } else {
            return filter;
        }
    }
}
