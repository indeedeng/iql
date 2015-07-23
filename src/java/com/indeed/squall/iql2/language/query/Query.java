package com.indeed.squall.iql2.language.query;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.AggregateMetrics;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocFilters;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.JQLParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Query {
    public final List<com.indeed.squall.iql2.language.query.Dataset> datasets;
    public final Optional<DocFilter> filter;
    public final List<com.indeed.squall.iql2.language.query.GroupBy> groupBys;
    public final List<AggregateMetric> selects;
    public final Optional<Integer> rowLimit;

    public Query(List<Dataset> datasets, Optional<DocFilter> filter, List<GroupBy> groupBys, List<AggregateMetric> selects, Optional<Integer> rowLimit) {
        this.datasets = datasets;
        this.filter = filter;
        this.groupBys = groupBys;
        this.selects = selects;
        this.rowLimit = rowLimit;
    }

    public static Query parseQuery(JQLParser.QueryContext queryContext, Map<String, Set<String>> datasetToKeywordAnalyzerFields, Map<String, Set<String>> datasetToIntFields) {
        final List<com.indeed.squall.iql2.language.query.Dataset> datasets = com.indeed.squall.iql2.language.query.Dataset.parseDatasets(queryContext.fromContents());

        final Optional<DocFilter> whereFilter;
        if (queryContext.whereContents() != null) {
            final List<DocFilter> filters = new ArrayList<>();
            for (final JQLParser.DocFilterContext ctx : queryContext.whereContents().docFilter()) {
                filters.add(DocFilters.parseDocFilter(ctx, datasetToKeywordAnalyzerFields, datasetToIntFields));
            }
            if (filters.isEmpty()) {
                whereFilter = Optional.absent();
            } else {
                whereFilter = Optional.of(DocFilters.and(filters));
            }
        } else {
            whereFilter = Optional.absent();
        }

        final List<com.indeed.squall.iql2.language.query.GroupBy> groupBys;
        if (queryContext.groupByContents() != null) {
            groupBys = GroupBys.parseGroupBys(queryContext.groupByContents(), datasetToKeywordAnalyzerFields, datasetToIntFields);
        } else {
            groupBys = Collections.emptyList();
        }

        final List<AggregateMetric> selects;
        if (queryContext.selects != null) {
            if (queryContext.selects.size() == 0) {
                selects = Collections.<AggregateMetric>singletonList(new AggregateMetric.DocStats(new DocMetric.Field("count()")));
            } else if (queryContext.selects.size() == 1) {
                final JQLParser.SelectContentsContext selectSet = queryContext.selects.get(0);
                final List<JQLParser.AggregateMetricContext> metrics = selectSet.aggregateMetric();
                selects = new ArrayList<>();
                for (final JQLParser.AggregateMetricContext metric : metrics) {
                    selects.add(AggregateMetrics.parseAggregateMetric(metric, datasetToKeywordAnalyzerFields, datasetToIntFields));
                }
            } else {
                throw new IllegalArgumentException("Invalid number of select clauses! numClauses = " + queryContext.selects.size());
            }
        } else {
            selects = Collections.<AggregateMetric>singletonList(new AggregateMetric.DocStats(new DocMetric.Field("count()")));
        }

        final Optional<Integer> rowLimit;
        if (queryContext.limit == null) {
            rowLimit = Optional.absent();
        } else {
            rowLimit = Optional.of(Integer.parseInt(queryContext.limit.getText()));
        }

        return new Query(datasets, whereFilter, groupBys, selects, rowLimit);
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
            selects.add(select.transform(f, g, h, i, groupBy));
        }
        return new Query(datasets, filter, groupBys, selects, rowLimit);
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
        return new Query(datasets, filter, groupBys, selects, rowLimit);
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

    public Map<String, String> nameToIndex() {
        final Map<String, String> nameToIndex = new HashMap<>();
        for (final Dataset dataset : datasets) {
            final String name;
            if (dataset.alias.isPresent()) {
                name = dataset.alias.get();
            } else {
                name = dataset.dataset;
            }
            if (nameToIndex.containsKey(name)) {
                throw new IllegalArgumentException("Duplicate name encountered: " + name);
            }
            nameToIndex.put(name, dataset.dataset);
        }
        return nameToIndex;
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
