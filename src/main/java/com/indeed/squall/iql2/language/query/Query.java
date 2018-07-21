/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.squall.iql2.language.query;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.squall.iql2.language.AbstractPositional;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.AggregateMetrics;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocFilters;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.GroupByEntry;
import com.indeed.squall.iql2.language.JQLParser;
import com.indeed.squall.iql2.language.ParserCommon;
import com.indeed.squall.iql2.language.Positioned;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.metadata.DatasetsMetadata;
import com.indeed.util.core.Pair;
import com.indeed.util.core.time.WallClock;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.antlr.v4.runtime.Token;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Query extends AbstractPositional {
    public final List<Dataset> datasets;
    public final Optional<DocFilter> filter;
    public final List<GroupByEntry> groupBys;
    public final List<AggregateMetric> selects;
    public final List<Optional<String>> formatStrings;
    public final List<String> options;
    public final Optional<Integer> rowLimit;
    public final boolean useLegacy;

    private static final String FORMAT_STRING_TEMPLATE = "%%.%sf";

    public Query(List<Dataset> datasets, Optional<DocFilter> filter, List<GroupByEntry> groupBys, List<AggregateMetric> selects, List<Optional<String>> formatStrings, List<String> options, Optional<Integer> rowLimit, boolean useLegacy) {
        this.datasets = datasets;
        this.filter = filter;
        this.groupBys = groupBys;
        this.selects = selects;
        this.formatStrings = formatStrings;
        this.options = options;
        this.rowLimit = rowLimit;
        this.useLegacy = useLegacy;
    }

    public static Query parseQuery(
            JQLParser.FromContentsContext fromContents,
            Optional<JQLParser.WhereContentsContext> whereContents,
            Optional<JQLParser.GroupByContentsContext> groupByContents,
            List<JQLParser.SelectContentsContext> selects,
            List<String> options,
            Token limit,
            DatasetsMetadata datasetsMetadata,
            Consumer<String> warn,
            WallClock clock,
            boolean useLegacy
    ) {
        final List<Pair<Dataset, Optional<DocFilter>>> datasetsWithFilters = Dataset.parseDatasets(fromContents, options, datasetsMetadata, warn, clock);

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
                allFilters.add(DocFilters.parseDocFilter(ctx, options, datasetsMetadata, fromContents, warn, clock));
            }
        }

        final List<GroupByEntry> groupBys;
        if (groupByContents.isPresent()) {
            groupBys = GroupBys.parseGroupBys(groupByContents.get(), options, datasetsMetadata, warn, clock);
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
            final int numFormattedAggregateMetrics = selectSet.formattedAggregateMetric().size();
            if (numFormattedAggregateMetrics == 0) {
                selectedMetrics = Collections.<AggregateMetric>singletonList(new AggregateMetric.DocStats(new DocMetric.Count()));
                formatStrings = Collections.singletonList(Optional.<String>absent());
            } else {
                final List<JQLParser.AggregateMetricContext> metrics = new ArrayList<>(numFormattedAggregateMetrics);
                formatStrings = new ArrayList<>();
                Optional<String> formatString;
                for (final JQLParser.FormattedAggregateMetricContext formattedMetric : selectSet.formattedAggregateMetric()) {
                    metrics.add(formattedMetric.aggregateMetric());
                    if (formattedMetric.STRING_LITERAL() != null) {
                        formatString = Optional.of(ParserCommon.unquote(formattedMetric.STRING_LITERAL().getText()));
                    } else if (selectSet.precision != null) {
                        formatString = Optional.of(String.format(FORMAT_STRING_TEMPLATE, selectSet.precision.getText()));
                    } else {
                        formatString = Optional.absent();
                    }
                    formatStrings.add(formatString);
                }
                selectedMetrics = new ArrayList<>();
                for (final JQLParser.AggregateMetricContext metric : metrics) {
                    selectedMetrics.add(AggregateMetrics.parseAggregateMetric(metric, options, datasetsMetadata, warn, clock));
                }
            }
        } else {
            throw new IqlKnownException.ParseErrorException("Invalid number of select clauses! numClauses = " + selects.size());
        }

        final Optional<Integer> rowLimit;
        if (limit == null) {
            rowLimit = Optional.absent();
        } else {
            rowLimit = Optional.of(Integer.parseInt(limit.getText()));
        }

        if (useLegacy) {
            rewriteMultiTermIn(allFilters, groupBys);
        }
        final Optional<DocFilter> whereFilter;
        if (allFilters.isEmpty()) {
            whereFilter = Optional.absent();
        } else {
            whereFilter = Optional.of(DocFilters.and(allFilters));
        }
        return new Query(datasets, whereFilter, groupBys, selectedMetrics, formatStrings, options, rowLimit, useLegacy);
    }

    public static Query parseQuery(JQLParser.QueryContext queryContext, DatasetsMetadata datasetsMetadata, Consumer<String> warn, WallClock clock) {
        final List<String> options = queryContext.options.stream().map(x -> ParserCommon.unquote(x.getText())).collect(Collectors.toList());
        final Query query = parseQuery(
                queryContext.fromContents(),
                Optional.fromNullable(queryContext.whereContents()),
                Optional.fromNullable(queryContext.groupByContents()),
                queryContext.selects,
                options,
                queryContext.limit,
                datasetsMetadata,
                warn,
                clock,
                queryContext.useLegacy
        );
        query.copyPosition(queryContext);
        return query;
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
        final List<GroupByEntry> groupBys = Lists.newArrayList();
        for (final GroupByEntry gb : this.groupBys) {
            groupBys.add(gb.transform(groupBy, f, g, h, i));
        }
        final List<AggregateMetric> selects = Lists.newArrayList();
        for (final AggregateMetric select : this.selects) {
            selects.add(select.transform(f, g, h, i, groupBy));
        }
        return new Query(datasets, filter, groupBys, selects, formatStrings, options, rowLimit, useLegacy);
    }

    public Query traverse1(Function<AggregateMetric, AggregateMetric> f) {
        final List<GroupByEntry> groupBys = Lists.newArrayList();
        for (final GroupByEntry gb : this.groupBys) {
            groupBys.add(gb.traverse1(f));
        }
        final List<AggregateMetric> selects = Lists.newArrayList();
        for (final AggregateMetric select : this.selects) {
            selects.add(select.traverse1(f));
        }
        return new Query(datasets, filter, groupBys, selects, formatStrings, options, rowLimit, useLegacy);
    }

    public Set<String> extractDatasetNames() {
        final Set<String> names = new HashSet<>();
        for (final Dataset dataset : datasets) {
            final Positioned<String> name;
            if (dataset.alias.isPresent()) {
                name = dataset.alias.get();
            } else {
                name = dataset.dataset;
            }
            if (names.contains(name.unwrap())) {
                throw new IqlKnownException.ParseErrorException("Duplicate name encountered: " + name.unwrap());
            }
            names.add(name.unwrap());
        }
        return names;
    }

    public Map<String, String> nameToIndex() {
        final Map<String, String> nameToIndex = new HashMap<>();
        for (final Dataset dataset : datasets) {
            final Positioned<String> name;
            if (dataset.alias.isPresent()) {
                name = dataset.alias.get();
            } else {
                name = dataset.dataset;
            }
            if (nameToIndex.containsKey(name.unwrap())) {
                throw new IqlKnownException.ParseErrorException("Duplicate name encountered: " + name.unwrap());
            }
            nameToIndex.put(name.unwrap(), dataset.dataset.unwrap());
        }
        return nameToIndex;
    }

    // rewrite field in (A, B), group by field to group by field in (A, B...)
    private static void rewriteMultiTermIn(final List<DocFilter> filters, final List<GroupByEntry> groupBys) {
        final Set<String> rewrittenFields = new HashSet<>();
        for (int i = 0; i < filters.size(); i++) {
            final DocFilter filter = filters.get(i);
            if ((filter instanceof DocFilter.IntFieldIn) || (filter instanceof DocFilter.StringFieldIn)) {
                final String filterField;
                final LongList intTerms = new LongArrayList();
                final List<String> stringTerms = Lists.newArrayList();
                if (filter instanceof DocFilter.IntFieldIn) {
                    final DocFilter.IntFieldIn intFieldIn = (DocFilter.IntFieldIn)filter;
                    filterField = intFieldIn.field.unwrap();
                    intTerms.addAll(intFieldIn.terms);
                } else {
                    final DocFilter.StringFieldIn stringFieldIn = (DocFilter.StringFieldIn)filter;
                    filterField = stringFieldIn.field.unwrap();
                    stringTerms.addAll(stringFieldIn.terms);
                }
                if (rewrittenFields.contains(filterField)) {
                    continue;
                }
                boolean foundRewriteGroupBy = false;
                for (int j = 0; j < groupBys.size(); j++) {
                    final GroupByEntry groupByEntry = groupBys.get(j);
                    final GroupBy groupBy = groupByEntry.groupBy;
                    if (groupBy instanceof GroupBy.GroupByField) {
                        final GroupBy.GroupByField groupByField = (GroupBy.GroupByField) groupBy;
                        if (filterField.equalsIgnoreCase(groupByField.field.unwrap())) {
                            groupBys.set(j, new GroupByEntry(
                                    new GroupBy.GroupByFieldIn(groupByField.field, intTerms, stringTerms,
                                            groupByField.withDefault),
                                    groupByEntry.filter, groupByEntry.alias));
                            foundRewriteGroupBy = true;
                        }
                    }
                }
                if (foundRewriteGroupBy) {
                    rewrittenFields.add(filterField);
                    filters.remove(i);
                    i--;
                }
            }
        }
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
