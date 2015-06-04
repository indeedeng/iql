package com.indeed.jql.language.query;

import com.google.common.base.Optional;
import com.indeed.jql.language.AggregateFilter;
import com.indeed.jql.language.AggregateMetric;
import com.indeed.jql.language.AggregateMetrics;
import com.indeed.jql.language.DocFilter;
import com.indeed.jql.language.DocFilters;
import com.indeed.jql.language.JQLParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Query<F, M> {
    public final List<com.indeed.jql.language.query.Dataset> datasets;
    public final Optional<DocFilter> filter;
    public final List<com.indeed.jql.language.query.GroupBy> groupBys;
    public final List<M> selects;

    public Query(List<com.indeed.jql.language.query.Dataset> datasets, Optional<DocFilter> filter, List<com.indeed.jql.language.query.GroupBy> groupBys, List<M> selects) {
        this.datasets = datasets;
        this.filter = filter;
        this.groupBys = groupBys;
        this.selects = selects;
    }

    public static Query<AggregateFilter, AggregateMetric> parseQuery(JQLParser.QueryContext queryContext) {
        final List<com.indeed.jql.language.query.Dataset> datasets = com.indeed.jql.language.query.Dataset.parseDatasets(queryContext.fromContents());

        final Optional<DocFilter> whereFilter;
        if (queryContext.docFilter() != null) {
            final List<DocFilter> filters = new ArrayList<>();
            for (final JQLParser.DocFilterContext ctx : queryContext.docFilter()) {
                filters.add(DocFilters.parseDocFilter(ctx));
            }
            whereFilter = Optional.of(DocFilters.and(filters));
        } else {
            whereFilter = Optional.absent();
        }

        final List<com.indeed.jql.language.query.GroupBy> groupBys;
        if (queryContext.groupByContents() != null) {
            groupBys = GroupBys.parseGroupBys(queryContext.groupByContents());
        } else {
            groupBys = Collections.emptyList();
        }

        final List<AggregateMetric> selects;
        if (queryContext.selects != null) {
            if (queryContext.selects.size() == 0) {
                selects = Collections.emptyList();
            } else if (queryContext.selects.size() == 1) {
                final JQLParser.SelectContentsContext selectSet = queryContext.selects.get(0);
                final List<JQLParser.AggregateMetricContext> metrics = selectSet.aggregateMetric();
                selects = new ArrayList<>();
                for (final JQLParser.AggregateMetricContext metric : metrics) {
                    selects.add(AggregateMetrics.parseAggregateMetric(metric));
                }
            } else {
                throw new IllegalArgumentException("Invalid number of select clauses! numClauses = " + queryContext.selects.size());
            }
        } else {
            selects = Collections.emptyList();
        }

        return new Query<>(datasets, whereFilter, groupBys, selects);
    }
}
