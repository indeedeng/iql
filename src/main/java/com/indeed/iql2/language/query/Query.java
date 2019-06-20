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

package com.indeed.iql2.language.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.indeed.common.datastruct.PersistentStack;
import com.indeed.imhotep.Shard;
import com.indeed.iql.Constants;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.execution.QueryOptions;
import com.indeed.iql2.execution.ResultFormat;
import com.indeed.iql2.language.AbstractPositional;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.AggregateMetrics;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocFilters;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.GroupByEntry;
import com.indeed.iql2.language.JQLParser;
import com.indeed.iql2.language.ParserCommon;
import com.indeed.iql2.language.Positional;
import com.indeed.iql2.language.Positioned;
import com.indeed.iql2.language.cachekeys.CacheKey;
import com.indeed.iql2.language.commands.Command;
import com.indeed.iql2.language.query.fieldresolution.FieldResolver;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.query.fieldresolution.ScopedFieldResolver;
import com.indeed.iql2.language.query.shardresolution.RemappingShardResolver;
import com.indeed.iql2.language.query.shardresolution.ShardResolver;
import com.indeed.iql2.server.web.servlets.query.SelectQueryExecution;
import com.indeed.util.core.Pair;
import com.indeed.util.core.time.WallClock;
import com.indeed.util.logging.TracingTreeTimer;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@EqualsAndHashCode
@ToString
public class Query extends AbstractPositional {
    public final List<Dataset> datasets;
    public final Optional<DocFilter> filter;
    public final List<GroupByEntry> groupBys;
    public final List<AggregateMetric> selects;
    public final List<Optional<String>> formatStrings;
    public final List<String> options;
    public final Optional<Integer> rowLimit;
    public final boolean useLegacy;
    public final DateTimeZone timeZone;

    // Lazily initialized on use
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private List<Command> commands = null;
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private Optional<List<AggregateMetric>> totals = null;

    public Query(
            final List<Dataset> datasets,
            final Optional<DocFilter> filter,
            final List<GroupByEntry> groupBys,
            final List<AggregateMetric> selects,
            final List<Optional<String>> formatStrings,
            final List<String> options,
            final Optional<Integer> rowLimit,
            final boolean useLegacy,
            final DateTimeZone timeZone
    ) {
        this.datasets = datasets;
        this.filter = filter;
        this.groupBys = groupBys;
        this.selects = selects;
        this.formatStrings = formatStrings;
        this.options = options;
        this.rowLimit = rowLimit;
        this.useLegacy = useLegacy;
        this.timeZone = timeZone;
    }

    public interface AggregateContext {
        interface Matcher {
            void matchField(final FieldSet field);

            void matchMetric();

            void matchSession();
        }

        @Value
        class Field implements AggregateContext {
            private final FieldSet field;

            @Override
            public void accept(final Matcher matcher) {
                matcher.matchField(field);
            }
        }

        @Value
        class Metric implements AggregateContext {
            @Override
            public void accept(final Matcher matcher) {
                matcher.matchMetric();
            }
        }

        @Value
        class Session implements AggregateContext {
            @Override
            public void accept(final Matcher matcher) {
                matcher.matchSession();
            }
        }

        void accept(final Matcher matcher);
    }

    // Helper class for data that necessary while parsing query.
    public static class Context {
        public final List<String> options;
        public final DatasetsMetadata datasetsMetadata;
        public final JQLParser.FromContentsContext fromContext;
        public final Consumer<String> warn;
        public final WallClock clock;
        public final ScopedFieldResolver fieldResolver;
        public final ShardResolver shardResolver;
        public final PersistentStack<AggregateContext> aggregateContexts;
        public final DateTimeZone timeZone;
        public final TracingTreeTimer timer;

        public Context(
                final List<String> options,
                final DatasetsMetadata datasetsMetadata,
                final JQLParser.FromContentsContext fromContext,
                final Consumer<String> warn,
                final WallClock clock,
                final TracingTreeTimer timer,
                final ScopedFieldResolver fieldResolver,
                final ShardResolver shardResolver,
                final PersistentStack<AggregateContext> aggregateContexts,
                final DateTimeZone timeZone) {
            this.options = options;
            this.datasetsMetadata = datasetsMetadata;
            this.fromContext = fromContext;
            this.warn = warn;
            this.clock = clock;
            this.timer = timer;
            this.fieldResolver = fieldResolver;
            this.shardResolver = shardResolver;
            this.aggregateContexts = aggregateContexts;
            this.timeZone = timeZone;
        }

        public Context copyWithAnotherFromContext(final JQLParser.FromContentsContext newContext) {
            return new Context(options, datasetsMetadata, newContext, warn, clock, timer, fieldResolver, shardResolver, aggregateContexts, timeZone);
        }

        public Context withFieldResolver(final ScopedFieldResolver fieldResolver) {
            return new Context(options, datasetsMetadata, fromContext, warn, clock, timer, fieldResolver, shardResolver, aggregateContexts, timeZone);
        }

        public Context withoutAggregate() {
            return new Context(options, datasetsMetadata, fromContext, warn, clock, timer, fieldResolver, shardResolver, PersistentStack.empty(), timeZone);
        }

        private Context withAggregate(final AggregateContext aggregateContext) {
            return new Context(options, datasetsMetadata, fromContext, warn, clock, timer, fieldResolver, shardResolver, aggregateContexts.pushed(aggregateContext), timeZone);
        }

        public Context withAggregatePopped() {
            return new Context(options, datasetsMetadata, fromContext, warn, clock, timer, fieldResolver, shardResolver, aggregateContexts.popped(), timeZone);
        }

        public Context withFieldAggregate(final FieldSet fieldSet) {
            return withAggregate(new AggregateContext.Field(fieldSet));
        }

        /**
         * Including bucket, random, quantiles, window, running, etc.
         */
        public Context withMetricAggregate() {
            return withAggregate(new AggregateContext.Metric());
        }

        public Context withSessionAggregate() {
            return withAggregate(new AggregateContext.Session());
        }

        public PartialContext partialContext() {
            return new PartialContext(options, datasetsMetadata, fromContext, warn, clock, timer, shardResolver, aggregateContexts, timeZone);
        }

        // Represents Context when we don't yet have a FieldResolver.
        // Only happens in a couple of places so the weight this adds isn't too bad.
        public static class PartialContext {
            public final List<String> options;
            public final DatasetsMetadata datasetsMetadata;
            public final JQLParser.FromContentsContext fromContext;
            public final Consumer<String> warn;
            public final WallClock clock;
            public final TracingTreeTimer timer;
            public final ShardResolver shardResolver;
            public final PersistentStack<AggregateContext> aggregateContexts;
            public final DateTimeZone timeZone;

            public PartialContext(final List<String> options, final DatasetsMetadata datasetsMetadata, final JQLParser.FromContentsContext fromContext, final Consumer<String> warn, final WallClock clock, final TracingTreeTimer timer, final ShardResolver shardResolver, final PersistentStack<AggregateContext> aggregateContexts, final DateTimeZone timeZone) {
                this.options = options;
                this.datasetsMetadata = datasetsMetadata;
                this.fromContext = fromContext;
                this.warn = warn;
                this.clock = clock;
                this.timer = timer;
                this.shardResolver = shardResolver;
                this.aggregateContexts = aggregateContexts;
                this.timeZone = timeZone;
            }

            public Context fullContext(final ScopedFieldResolver fieldResolver) {
                return new Context(options, datasetsMetadata, fromContext, warn, clock, timer, fieldResolver, shardResolver, aggregateContexts, timeZone);
            }
        }
    }

    private static final String FORMAT_STRING_TEMPLATE = "%%.%sf";

    @Override
    public Query copyPosition(final Positional positional) {
        super.copyPosition(positional);
        return this;
    }

    public static Query parseQuery(
            // queryCtx is the parse tree that represents the entire query, and is used for extracting FieldResolver
            // information, such as datasets, field aliases, and metric aliases
            final ParseTree queryCtx,
            // context.fromContext may be different from queryCtx.from in case of sub-queries using FROM SAME
            final Context.PartialContext partialContext,
            final Optional<JQLParser.WhereContentsContext> whereContents,
            final Optional<JQLParser.GroupByContentsContext> groupByContents,
            final List<JQLParser.SelectContentsContext> selects,
            final Token limit,
            final boolean useLegacy,
            final boolean isTopLevelQuery
    ) {
        final FieldResolver fieldResolver = FieldResolver.build(queryCtx, partialContext.fromContext,
                partialContext.datasetsMetadata, useLegacy);
        final Context context = partialContext.fullContext(fieldResolver.universalScope());

        final List<Pair<Dataset, Optional<DocFilter>>> datasetsWithFilters = Dataset.parseDatasets(context);

        final List<Dataset> datasets = Lists.newArrayListWithCapacity(datasetsWithFilters.size());
        List<DocFilter> allFilters = new ArrayList<>();
        for (final Pair<Dataset, Optional<DocFilter>> dataset : datasetsWithFilters) {
            if (dataset.getSecond().isPresent()) {
                allFilters.add(dataset.getSecond().get());
            }
            datasets.add(dataset.getFirst());
        }
        validateDatasetNames(datasets);
        if (whereContents.isPresent()) {
            for (final JQLParser.DocFilterContext ctx : whereContents.get().docFilter()) {
                allFilters.add(DocFilters.parseDocFilter(ctx, context));
            }
        }

        final List<GroupByEntry> groupBys;
        final Context newContext;
        if (groupByContents.isPresent()) {
            final Pair<List<GroupByEntry>, Context> parsed = GroupBys.parseGroupBys(groupByContents.get(), context);
            groupBys = parsed.getFirst();
            newContext = parsed.getSecond();
        } else {
            groupBys = Collections.emptyList();
            newContext = context;
        }

        final List<AggregateMetric> selectedMetrics;
        final List<Optional<String>> formatStrings;
        if (selects.isEmpty()) {
            selectedMetrics = isTopLevelQuery ?
                    Collections.singletonList(new AggregateMetric.DocStats(new DocMetric.Count())) :
                    Collections.emptyList();
            formatStrings = Collections.singletonList(Optional.empty());
        } else if (selects.size() == 1) {
            final JQLParser.SelectContentsContext selectSet = selects.get(0);
            final int numFormattedAggregateMetrics = selectSet.formattedAggregateMetric().size();
            if (numFormattedAggregateMetrics == 0) {
                selectedMetrics = Collections.singletonList(new AggregateMetric.DocStats(new DocMetric.Count()));
                formatStrings = Collections.singletonList(Optional.empty());
            } else {
                final List<JQLParser.AggregateMetricContext> metrics = new ArrayList<>(numFormattedAggregateMetrics);
                formatStrings = new ArrayList<>();
                Optional<String> formatString;
                for (final JQLParser.FormattedAggregateMetricContext formattedMetric : selectSet.formattedAggregateMetric()) {
                    metrics.add(formattedMetric.aggregateMetric());
                    if (formattedMetric.STRING_LITERAL() != null) {
                        formatString = Optional.of(ParserCommon.unquote(formattedMetric.STRING_LITERAL().getText(), useLegacy, context.warn));
                    } else if (selectSet.precision != null) {
                        formatString = Optional.of(String.format(FORMAT_STRING_TEMPLATE, selectSet.precision.getText()));
                    } else {
                        formatString = Optional.empty();
                    }
                    formatStrings.add(formatString);
                }
                selectedMetrics = new ArrayList<>();
                for (final JQLParser.AggregateMetricContext metric : metrics) {
                    selectedMetrics.add(AggregateMetrics.parseAggregateMetric(metric, newContext));
                }
            }
        } else {
            throw new IqlKnownException.ParseErrorException("Invalid number of select clauses! numClauses = " + selects.size());
        }

        final Optional<Integer> rowLimit;
        if (limit == null) {
            rowLimit = Optional.empty();
        } else {
            rowLimit = Optional.of(Integer.parseInt(limit.getText()));
        }

        if (useLegacy) {
            allFilters = DocFilter.And.unwrap(allFilters);
            rewriteMultiTermIn(Iterables.getOnlyElement(datasets), allFilters, groupBys);
        }
        final Optional<DocFilter> whereFilter;
        if (allFilters.isEmpty()) {
            whereFilter = Optional.empty();
        } else {
            whereFilter = Optional.of(DocFilter.And.create(allFilters));
        }

        // Make future errors immediately thrown, while also throwing any errors that have been caused up to this point.
        fieldResolver.setErrorMode(FieldResolver.ErrorMode.IMMEDIATE);

        return new Query(datasets, whereFilter, groupBys, selectedMetrics, formatStrings, context.options, rowLimit, useLegacy, context.timeZone);
    }

    private static DateTimeZone timeZoneForQuery(final JQLParser.QueryContext queryCtx) {
        final List<JQLParser.TimeZoneSpecifierContext> timeZones = queryCtx.timeZones;
        if (timeZones.isEmpty()) {
            return Constants.DEFAULT_IQL_TIME_ZONE;
        }
        if (timeZones.size() > 1) {
            throw new IqlKnownException.ParseErrorException("Requested multiple timezones -- can only use 1!");
        }
        final JQLParser.TimeZoneSpecifierContext timeZoneCtx = timeZones.get(0);
        final int hoursOffset = (timeZoneCtx.hours != null) ? Integer.parseInt(timeZoneCtx.hours.getText()) : 0;
        final int minutesOffset = (timeZoneCtx.minutes != null) ? Integer.parseInt(timeZoneCtx.minutes.getText()) : 0;
        final String signString = (timeZoneCtx.sign == null) ? "+" : timeZoneCtx.sign.getText();
        final int sign;
        switch (signString) {
            case "+":
                sign = 1;
                break;
            case "-":
                sign = -1;
                break;
            default:
                throw new IllegalStateException("Sign cannot be other than + or -");
        }
        return DateTimeZone.forOffsetHoursMinutes(sign * hoursOffset, sign * minutesOffset);
    }

    public static Query parseQuery(
            final JQLParser.QueryContext queryContext,
            final DatasetsMetadata datasetsMetadata,
            final Set<String> defaultOptions,
            final Consumer<String> warn,
            final WallClock clock,
            final TracingTreeTimer timer,
            ShardResolver shardResolver
    ) {
        final List<String> options = queryContext.options.stream().map(x -> ParserCommon.unquote(x.getText(), queryContext.useLegacy, warn)).collect(Collectors.toList());
        options.addAll(defaultOptions);
        if (QueryOptions.Experimental.hasHosts(options)) {
            shardResolver = new RemappingShardResolver(shardResolver, QueryOptions.Experimental.parseHostMappingMethod(options), QueryOptions.Experimental.parseHosts(options));
        }
        final Context.PartialContext context = new Context.PartialContext(
                options,
                datasetsMetadata,
                queryContext.fromContents(),
                warn,
                clock,
                timer,
                shardResolver,
                PersistentStack.empty(),
                timeZoneForQuery(queryContext)
        );
        final Query query = parseQuery(
                queryContext,
                context,
                Optional.ofNullable(queryContext.whereContents()),
                Optional.ofNullable(queryContext.groupByContents()),
                queryContext.selects,
                queryContext.limit,
                queryContext.useLegacy,
                true
        );
        query.copyPosition(queryContext);
        return query;
    }

    public static Query parseSubquery(
            final JQLParser.QueryNoSelectContext queryContext,
            final Context parentQueryContext) {
        // Changing context if necessary
        final Query.Context actualContext =
                (queryContext.same == null) ? parentQueryContext.copyWithAnotherFromContext(queryContext.fromContents()) : parentQueryContext;
        if (actualContext.fromContext == null) {
            throw new IqlKnownException.ParseErrorException("Can't use 'FROM SAME' outside of WHERE or GROUP BY");
        }
        if ((queryContext.groupByContents() == null) || (queryContext.groupByContents().groupByEntry().size() != 1)) {
            throw new IqlKnownException.ParseErrorException("Subqueries must have exactly one group by element");
        }
        final Query query = Query.parseQuery(
                queryContext,
                actualContext.partialContext(),
                Optional.ofNullable(queryContext.whereContents()),
                Optional.of(queryContext.groupByContents()),
                Collections.emptyList(),
                null,
                false,
                false
        );
        query.copyPosition(queryContext);
        return query;
    }

    /**
     * Do a full bottom-up tree traversal of the query, transforming from the leaf nodes up, rebuilding their immediate parent,
     * and then repeating the process starting from the parent and moving upward, until we hit the root of the query.
     * <p>
     * When implementing transform() methods on new classes, the recipe should be:
     * <ol>
     * <li>Call transform() on each of your children
     * <li>Construct a new version of this, where all children have been replaced by their transformed versions
     * <li>Call the transformation function corresponding to this class's interface on the new object
     * <li>Propagate any source information ({@link com.indeed.iql2.language.Positional}) to the newly constructed object
     * <li>Return the object with all transforms and source propagation performed
     * </ol><p>
     * One important caveat is that transform currently STOPS at the boundary of a sub-query.
     * This means that query.transform(increment constants) will increment the constants in the outer query,
     * but will not increment the constants in any sub-query.
     *
     * @param groupBy function to transform GroupBys by
     * @param f       function to transform AggregateMetrics by
     * @param g       function to transform DocMetrics by
     * @param h       function to transform AggregateFilters by
     * @param i       function to transform DocFilters by
     * @return the transformed Query
     * @see AggregateMetric#transform
     * @see AggregateFilter#transform
     * @see DocMetric#transform
     * @see DocFilter#transform
     * @see GroupBy#transform
     */
    public Query transform(
            final Function<GroupBy, GroupBy> groupBy,
            final Function<AggregateMetric, AggregateMetric> f,
            final Function<DocMetric, DocMetric> g,
            final Function<AggregateFilter, AggregateFilter> h,
            final Function<DocFilter, DocFilter> i
    ) {
        final Optional<DocFilter> filter;
        if (this.filter.isPresent()) {
            filter = Optional.of(this.filter.get().transform(g, i));
        } else {
            filter = Optional.empty();
        }
        final List<GroupByEntry> groupBys = Lists.newArrayList();
        for (final GroupByEntry gb : this.groupBys) {
            groupBys.add(gb.transform(groupBy, f, g, h, i));
        }
        final List<AggregateMetric> selects = Lists.newArrayList();
        for (final AggregateMetric select : this.selects) {
            selects.add(select.transform(f, g, h, i, groupBy));
        }
        return new Query(datasets, filter, groupBys, selects, formatStrings, options, rowLimit, useLegacy, timeZone).copyPosition(this);
    }

    private static void validateDatasetNames(final List<Dataset> datasets) {
        final Set<String> names = new HashSet<>();
        for (final Dataset dataset : datasets) {
            final String name = dataset.getDisplayName();
            if (!names.add(name)) {
                throw new IqlKnownException.ParseErrorException("Duplicate name encountered: " + name);
            }
        }
    }

    public Set<String> extractDatasetNames() {
        return datasets.stream().map(Dataset::getDisplayName).collect(Collectors.toSet());
    }

    public List<Dataset> getDatasets() {
        return Collections.unmodifiableList(datasets);
    }

    public List<Dataset> getDatasetsFromScope(Set<String> scope) {
        return datasets.stream().filter(dataset -> scope.contains(dataset.getDisplayName())).collect(Collectors.toList());
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

    /**
     * @return all shards used in this query as well as any sub queries
     */
    public ListMultimap<String, List<Shard>> allShardsUsed() {
        final ListMultimap<String, List<Shard>> allShardsUsed = ArrayListMultimap.create();
        for (final Dataset dataset : datasets) {
            allShardsUsed.put(dataset.getDisplayName(), dataset.shards);
        }
        runOnAllSubQueries(subQuery -> allShardsUsed.putAll(subQuery.query.allShardsUsed()));
        return allShardsUsed;
    }

    /**
     * @return all datasets and any shards they may be missing
     */
    public List<SelectQueryExecution.DatasetWithMissingShards> allMissingShards() {
        final List<SelectQueryExecution.DatasetWithMissingShards> datasetsWithMissingShards = new ArrayList<>();
        for (final Dataset dataset : datasets) {
            datasetsWithMissingShards.add(new SelectQueryExecution.DatasetWithMissingShards(dataset.getDisplayName(), dataset.startInclusive.unwrap(), dataset.endExclusive.unwrap(), dataset.missingShardIntervals));
        }
        runOnAllSubQueries(subQuery -> datasetsWithMissingShards.addAll(subQuery.query.allMissingShards()));
        return datasetsWithMissingShards;
    }

    public long totalNumDocs() {
        return datasets.stream().mapToLong(Dataset::numDocs).sum();
    }

    public int maxConcurrentSessions() {
        final int[] maxValueStorage = new int[]{datasets.size()};
        runOnAllSubQueries(subQuery -> {
            maxValueStorage[0] = Math.max(maxValueStorage[0], subQuery.query.maxConcurrentSessions());
        });
        return maxValueStorage[0];
    }

    private void runOnAllSubQueries(final Consumer<DocFilter.FieldInQuery> consumer) {
        transform(
                Function.identity(),
                Function.identity(),
                Function.identity(),
                Function.identity(),
                new Function<DocFilter, DocFilter>() {
                    @Nullable
                    @Override
                    public DocFilter apply(@Nullable final DocFilter input) {
                        if (input instanceof DocFilter.FieldInQuery) {
                            consumer.accept((DocFilter.FieldInQuery) input);
                        }
                        return input;
                    }
                }
        );
    }

    // rewrite field in (A, B), group by field to group by field in (A, B...)
    // only string fields are processed to be backward compatible with Iql1
    private static void rewriteMultiTermIn(final Dataset dataset, final List<DocFilter> filters, final List<GroupByEntry> groupBys) {
        final String singleDataset = dataset.getDisplayName();
        final Set<String> expectedDatasets = Collections.singleton(singleDataset);

        final Set<String> rewrittenFields = new HashSet<>();
        for (int i = 0; i < filters.size(); i++) {
            final DocFilter filter = filters.get(i);
            if (filter instanceof DocFilter.FieldInTermsSet) {
                final DocFilter.FieldInTermsSet fieldIn = (DocFilter.FieldInTermsSet) filter;
                if (fieldIn.field.isIntField()) {
                    continue;
                }
                Preconditions.checkState(fieldIn.field.datasets().equals(expectedDatasets));
                final String filterField = fieldIn.field.datasetFieldName(singleDataset);
                if (rewrittenFields.contains(filterField)) {
                    continue;
                }
                boolean foundRewriteGroupBy = false;
                for (int j = 0; j < groupBys.size(); j++) {
                    final GroupByEntry groupByEntry = groupBys.get(j);
                    final GroupBy groupBy = groupByEntry.groupBy;
                    if (groupBy instanceof GroupBy.GroupByField) {
                        final GroupBy.GroupByField groupByField = (GroupBy.GroupByField) groupBy;
                        Preconditions.checkState(groupByField.field.datasets().equals(expectedDatasets));
                        final String fieldName = groupByField.field.getOnlyField();
                        if (filterField.equals(fieldName) && !groupByField.isTopK()) {
                            final GroupBy.GroupByFieldIn groupByFieldIn =
                                    new GroupBy.GroupByFieldIn(groupByField.field, fieldIn.terms, groupByField.withDefault, true);
                            groupByFieldIn.copyPosition(groupByField);
                            groupBys.set(j, new GroupByEntry(groupByFieldIn, groupByEntry.filter, groupByEntry.alias));
                            foundRewriteGroupBy = true;
                        }
                    } else if (groupBy instanceof GroupBy.GroupByFieldIn) {
                        final GroupBy.GroupByFieldIn groupByFieldIn = (GroupBy.GroupByFieldIn) groupBy;
                        Preconditions.checkState(groupByFieldIn.field.datasets().equals(expectedDatasets));
                        final String fieldName = groupByFieldIn.field.getOnlyField();
                        if (filterField.equals(fieldName)
                                && (groupByFieldIn.terms.size() == fieldIn.terms.size())
                                && groupByFieldIn.terms.containsAll(fieldIn.terms)) {
                            // This filter does not affect visible result so it could be deleted.
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

    public List<Command> commands() {
        if (commands == null) {
            totals = useLegacy ? Optional.of(new ArrayList<>()) : Optional.empty();
            // TODO: incrementQueryLimit here seems *very* strange
            commands = Queries.queryCommands(SelectQueryExecution.incrementQueryLimit(this), totals);
        }
        return commands;
    }

    public Optional<List<AggregateMetric>> getTotals() {
        commands(); // initializing if not yet
        return totals;
    }

    public CacheKey cacheKey(final ResultFormat resultFormat) {
        return CacheKey.computeCacheKey(this, resultFormat);
    }

    public Set<String> allCacheKeys(final ResultFormat resultFormat) {
        final Set<String> allKeys = new HashSet<>();
        allKeys.add(cacheKey(resultFormat).rawHash);
        runOnAllSubQueries(subQuery -> allKeys.addAll(subQuery.query.allCacheKeys(ResultFormat.CSV)));
        return allKeys;
    }
}
