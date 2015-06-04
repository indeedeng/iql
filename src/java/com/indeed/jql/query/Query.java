package com.indeed.jql.query;

import com.indeed.jql.AggregateFilter;
import com.indeed.jql.AggregateMetric;
import com.indeed.jql.DocFilter;
import com.indeed.jql.JQLParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class Query<F, M> {
    private final List<Dataset> datasets;
    private final Optional<DocFilter> filter;
    private final List<GroupBy<F, M>> groupBys;
    private final List<M> selects;

    public Query(List<Dataset> datasets, Optional<DocFilter> filter, List<GroupBy<F, M>> groupBys, List<M> selects) {
        this.datasets = datasets;
        this.filter = filter;
        this.groupBys = groupBys;
        this.selects = selects;
    }

    public static Query<AggregateFilter, AggregateMetric> parseQuery(JQLParser.QueryContext queryContext) {
        final List<Dataset> datasets = Dataset.parseDatasets(queryContext.fromContents());

        final Optional<DocFilter> whereFilter;
        if (queryContext.docFilter() != null) {
            final List<DocFilter> filters = new ArrayList<>();
            for (final JQLParser.DocFilterContext ctx : queryContext.docFilter()) {
                filters.add(DocFilter.parseDocFilter(ctx));
            }
            whereFilter = Optional.of(DocFilter.and(filters));
        } else {
            whereFilter = Optional.empty();
        }

        final List<GroupBy<AggregateFilter, AggregateMetric>> groupBys;
        if (queryContext.groupByContents() != null) {
            groupBys = GroupBy.parseGroupBys(queryContext.groupByContents());
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
                    selects.add(AggregateMetric.parseAggregateMetric(metric));
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
