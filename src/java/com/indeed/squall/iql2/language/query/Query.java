package com.indeed.squall.iql2.language.query;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.indeed.common.util.time.WallClock;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.AggregateMetrics;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocFilters;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.GroupByMaybeHaving;
import com.indeed.squall.iql2.language.JQLParser;
import com.indeed.squall.iql2.language.ParserCommon;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.util.core.Pair;
import org.antlr.v4.runtime.Token;

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
    public final List<GroupByMaybeHaving> groupBys;
    public final List<AggregateMetric> selects;
    public final List<Optional<String>> formatStrings;
    public final Optional<Integer> rowLimit;

    public Query(List<Dataset> datasets, Optional<DocFilter> filter, List<GroupByMaybeHaving> groupBys, List<AggregateMetric> selects, List<Optional<String>> formatStrings, Optional<Integer> rowLimit) {
        this.datasets = datasets;
        this.filter = filter;
        this.groupBys = groupBys;
        this.selects = selects;
        this.formatStrings = formatStrings;
        this.rowLimit = rowLimit;
    }

    public static Query parseQuery(
            JQLParser.FromContentsContext fromContents,
            Optional<JQLParser.WhereContentsContext> whereContents,
            Optional<JQLParser.GroupByContentsContext> groupByContents,
            List<JQLParser.SelectContentsContext> selects,
            Token limit,
            Map<String, Set<String>> datasetToKeywordAnalyzerFields,
            Map<String, Set<String>> datasetToIntFields,
            Consumer<String> warn,
            WallClock clock
    ) {
        final List<Pair<Dataset, Optional<DocFilter>>> datasetsWithFilters = com.indeed.squall.iql2.language.query.Dataset.parseDatasets(fromContents, datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);

        final List<Dataset> datasets = Lists.newArrayListWithCapacity(datasetsWithFilters.size());
        final List<DocFilter> allFilters = new ArrayList<>();
        for (final Pair<Dataset, Optional<DocFilter>> dataset : datasetsWithFilters) {
            if (dataset.getSecond().isPresent()) {
                allFilters.add(dataset.getSecond().get());
            }
            datasets.add(dataset.getFirst());
        }
        if (whereContents.isPresent()) {
            for (final JQLParser.DocFilterContext ctx : whereContents.get().docFilter()) {
                allFilters.add(DocFilters.parseDocFilter(ctx, datasetToKeywordAnalyzerFields, datasetToIntFields, fromContents, warn, clock));
            }
        }
        final Optional<DocFilter> whereFilter;
        if (allFilters.isEmpty()) {
            whereFilter = Optional.absent();
        } else {
            whereFilter = Optional.of(DocFilters.and(allFilters));
        }

        final List<GroupByMaybeHaving> groupBys;
        if (groupByContents.isPresent()) {
            groupBys = GroupBys.parseGroupBys(groupByContents.get(), datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock);
        } else {
            groupBys = Collections.emptyList();
        }

        final List<AggregateMetric> selectedMetrics;
        final List<Optional<String>> formatStrings;
        if (selects.isEmpty()) {
            selectedMetrics = Collections.<AggregateMetric>singletonList(new AggregateMetric.DocStats(new DocMetric.Count()));
            formatStrings = Collections.singletonList(Optional.<String>absent());
        } else if (selects.size() == 1) {
            final JQLParser.SelectContentsContext selectSet = selects.get(0);
            final List<JQLParser.AggregateMetricContext> metrics = new ArrayList<>(selectSet.formattedAggregateMetric().size());
            formatStrings = new ArrayList<>();
            for (final JQLParser.FormattedAggregateMetricContext formattedMetric : selectSet.formattedAggregateMetric()) {
                metrics.add(formattedMetric.aggregateMetric());
                if (formattedMetric.STRING_LITERAL() != null) {
                    formatStrings.add(Optional.of(ParserCommon.unquote(formattedMetric.STRING_LITERAL().getText())));
                } else {
                    formatStrings.add(Optional.<String>absent());
                }
            }
            selectedMetrics = new ArrayList<>();
            for (final JQLParser.AggregateMetricContext metric : metrics) {
                selectedMetrics.add(AggregateMetrics.parseAggregateMetric(metric, datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock));
            }
        } else {
            throw new IllegalArgumentException("Invalid number of select clauses! numClauses = " + selects.size());
        }

        final Optional<Integer> rowLimit;
        if (limit == null) {
            rowLimit = Optional.absent();
        } else {
            rowLimit = Optional.of(Integer.parseInt(limit.getText()));
        }

        return new Query(datasets, whereFilter, groupBys, selectedMetrics, formatStrings, rowLimit);

    }

    public static Query parseQuery(JQLParser.QueryContext queryContext, Map<String, Set<String>> datasetToKeywordAnalyzerFields, Map<String, Set<String>> datasetToIntFields, Consumer<String> warn, WallClock clock) {
        return parseQuery(
                queryContext.fromContents(),
                Optional.fromNullable(queryContext.whereContents()),
                Optional.fromNullable(queryContext.groupByContents()),
                queryContext.selects,
                queryContext.limit,
                datasetToKeywordAnalyzerFields,
                datasetToIntFields,
                warn,
                clock
        );
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
        final List<GroupByMaybeHaving> groupBys = Lists.newArrayList();
        for (final GroupByMaybeHaving gb : this.groupBys) {
            groupBys.add(gb.transform(groupBy, f, g, h, i));
        }
        final List<AggregateMetric> selects = Lists.newArrayList();
        for (final AggregateMetric select : this.selects) {
            selects.add(select.transform(f, g, h, i, groupBy));
        }
        return new Query(datasets, filter, groupBys, selects, formatStrings, rowLimit);
    }

    public Query traverse1(Function<AggregateMetric, AggregateMetric> f) {
        final List<GroupByMaybeHaving> groupBys = Lists.newArrayList();
        for (final GroupByMaybeHaving gb : this.groupBys) {
            groupBys.add(gb.traverse1(f));
        }
        final List<AggregateMetric> selects = Lists.newArrayList();
        for (final AggregateMetric select : this.selects) {
            selects.add(select.traverse1(f));
        }
        return new Query(datasets, filter, groupBys, selects, formatStrings, rowLimit);
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Query query = (Query) o;

        if (datasets != null ? !datasets.equals(query.datasets) : query.datasets != null) return false;
        if (filter != null ? !filter.equals(query.filter) : query.filter != null) return false;
        if (groupBys != null ? !groupBys.equals(query.groupBys) : query.groupBys != null) return false;
        if (selects != null ? !selects.equals(query.selects) : query.selects != null) return false;
        return !(rowLimit != null ? !rowLimit.equals(query.rowLimit) : query.rowLimit != null);

    }

    @Override
    public int hashCode() {
        int result = datasets != null ? datasets.hashCode() : 0;
        result = 31 * result + (filter != null ? filter.hashCode() : 0);
        result = 31 * result + (groupBys != null ? groupBys.hashCode() : 0);
        result = 31 * result + (selects != null ? selects.hashCode() : 0);
        result = 31 * result + (rowLimit != null ? rowLimit.hashCode() : 0);
        return result;
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
