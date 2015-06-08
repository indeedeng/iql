package com.indeed.jql.language.query;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.indeed.jql.language.AggregateFilter;
import com.indeed.jql.language.AggregateMetric;
import com.indeed.jql.language.AggregateMetrics;
import com.indeed.jql.language.DocFilter;
import com.indeed.jql.language.DocFilters;
import com.indeed.jql.language.DocMetric;
import com.indeed.jql.language.JQLParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Query {
    public final List<com.indeed.jql.language.query.Dataset> datasets;
    public final Optional<DocFilter> filter;
    public final List<com.indeed.jql.language.query.GroupBy> groupBys;
    public final List<AggregateMetric> selects;

    public Query(List<com.indeed.jql.language.query.Dataset> datasets, Optional<DocFilter> filter, List<com.indeed.jql.language.query.GroupBy> groupBys, List<AggregateMetric> selects) {
        this.datasets = datasets;
        this.filter = filter;
        this.groupBys = groupBys;
        this.selects = selects;
    }

    public static Query parseQuery(JQLParser.QueryContext queryContext) {
        final List<com.indeed.jql.language.query.Dataset> datasets = com.indeed.jql.language.query.Dataset.parseDatasets(queryContext.fromContents());

        final Optional<DocFilter> whereFilter;
        if (queryContext.docFilter() != null) {
            final List<DocFilter> filters = new ArrayList<>();
            for (final JQLParser.DocFilterContext ctx : queryContext.docFilter()) {
                filters.add(DocFilters.parseDocFilter(ctx));
            }
            if (filters.isEmpty()) {
                whereFilter = Optional.absent();
            } else {
                whereFilter = Optional.of(DocFilters.and(filters));
            }
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

        return new Query(datasets, whereFilter, groupBys, selects);
    }

    public Query transform(
            Function<GroupBy, GroupBy> groupBy,
            Function<AggregateMetric, AggregateMetric> f,
            Function<DocMetric, DocMetric> g,
            Function<AggregateFilter, AggregateFilter> h,
            Function<DocFilter, DocFilter> i
    ) {
        final Optional<DocFilter> filter;
        if (this.filter.isPresent()) {
            filter = Optional.of(this.filter.get().transform(g, i));
        } else {
            filter = Optional.absent();
        }
        final List<GroupBy> groupBys = Lists.newArrayList();
        for (final GroupBy gb : this.groupBys) {
            groupBys.add(gb.transform(groupBy, f, g, h, i));
        }
        final List<AggregateMetric> selects = Lists.newArrayList();
        for (final AggregateMetric select : this.selects) {
            selects.add(select.transform(f, g, h, i));
        }
        return new Query(datasets, filter, groupBys, selects);
    }

    public Query traverse1(Function<AggregateMetric, AggregateMetric> f) {
        final List<GroupBy> groupBys = Lists.newArrayList();
        for (final GroupBy gb : this.groupBys) {
            groupBys.add(gb.traverse1(f));
        }
        final List<AggregateMetric> selects = Lists.newArrayList();
        for (final AggregateMetric select : this.selects) {
            selects.add(select.traverse1(f));
        }
        return new Query(datasets, filter, groupBys, selects);
    }

    public Set<String> extractDatasetNames() {
        final Set<String> names = new HashSet<>();
        for (final Dataset dataset : datasets) {
            final String name;
            if (dataset.alias.isPresent()) {
                name = dataset.alias.get();
            } else {
                name = dataset.dataset;
            }
            if (names.contains(name)) {
                throw new IllegalArgumentException("Duplicate name encountered: " + name);
            }
            names.add(name);
        }
        return names;
    }


    @Override
    public String toString() {
        return "Query{" +
                "datasets=" + datasets +
                ", filter=" + filter +
                ", groupBys=" + groupBys +
                ", selects=" + selects +
                '}';
    }
}
