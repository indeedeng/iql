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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.indeed.imhotep.Shard;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateFilters;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.AggregateMetrics;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocFilters;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.DocMetrics;
import com.indeed.iql2.language.GroupByEntry;
import com.indeed.iql2.language.JQLBaseListener;
import com.indeed.iql2.language.JQLLexer;
import com.indeed.iql2.language.JQLParser;
import com.indeed.iql2.language.Positional;
import com.indeed.iql2.language.UpperCaseInputStream;
import com.indeed.iql2.language.commands.Command;
import com.indeed.iql2.language.execution.ExecutionStep;
import com.indeed.iql2.language.execution.passes.FixFtgsMetricRunning;
import com.indeed.iql2.language.execution.passes.GroupIterations;
import com.indeed.iql2.language.execution.passes.OptimizeLast;
import com.indeed.iql2.language.optimizations.ConstantFolding;
import com.indeed.iql2.language.passes.ExtractNames;
import com.indeed.iql2.language.passes.ExtractPrecomputed;
import com.indeed.iql2.language.passes.FixTopKHaving;
import com.indeed.iql2.language.passes.HandleWhereClause;
import com.indeed.iql2.language.passes.RemoveNames;
import com.indeed.iql2.language.passes.SubstituteNamed;
import com.indeed.iql2.language.query.shardresolution.NullShardResolver;
import com.indeed.iql2.language.query.shardresolution.ShardResolver;
import com.indeed.iql2.language.util.ParserUtil;
import com.indeed.util.core.Pair;
import com.indeed.util.core.time.WallClock;
import com.indeed.util.logging.Loggers;
import com.indeed.util.logging.TracingTreeTimer;
import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Queries {
    private Queries() {
    }

    private static final Logger log = Logger.getLogger(Queries.class);

    public static class QueryDataset {
        public final String dataset;
        public final DateTime start;
        public final DateTime end;
        public final String name;

        public final List<Shard> shards;

        public QueryDataset(
                final String dataset,
                final DateTime start,
                final DateTime end,
                final String name,
                final List<Shard> shards
        ) {
            this.dataset = dataset;
            this.start = start;
            this.end = end;
            this.name = name;
            this.shards = shards;
        }
    }

    public static List<QueryDataset> createDatasetMap(final Query query) {
        final List<QueryDataset> result = new ArrayList<>();
        for (final Dataset dataset : query.datasets) {
            result.add(new QueryDataset(
                    dataset.dataset.unwrap(),
                    dataset.startInclusive.unwrap(),
                    dataset.endExclusive.unwrap(),
                    dataset.alias.orElse(dataset.dataset).unwrap(),
                    dataset.shards
            ));
        }
        return result;
    }

    public static GroupBy parseGroupBy(
            final String rawGroupBy,
            final boolean useLegacy,
            final Query.Context context) {
        return parseGroupByAndGetAggregatedContext(rawGroupBy, useLegacy, context).getFirst();
    }

    public static Pair<GroupBy, Query.Context> parseGroupByAndGetAggregatedContext(
            final String rawGroupBy,
            final boolean useLegacy,
            final Query.Context context) {
        final JQLParser.GroupByElementContext groupByElementContext = runParser(rawGroupBy, new Function<JQLParser, JQLParser.GroupByElementContext>() {
            @Nullable
            @Override
            public JQLParser.GroupByElementContext apply(@Nullable final JQLParser input) {
                return input.groupByElementEof(useLegacy).groupByElement();
            }
        });
        return GroupBys.parseGroupByAndGetAggregatedContext(groupByElementContext, context);
    }

    public static AggregateFilter parseAggregateFilter(
            final String rawAggregateFilter,
            final boolean useLegacy,
            final Query.Context context) {
        final JQLParser.AggregateFilterContext aggregateFilterContext = runParser(rawAggregateFilter, new Function<JQLParser, JQLParser.AggregateFilterContext>() {
            @Nullable
            @Override
            public JQLParser.AggregateFilterContext apply(@Nullable final JQLParser input) {
                return input.aggregateFilterEof(useLegacy).aggregateFilter();
            }
        });
        return AggregateFilters.parseAggregateFilter(aggregateFilterContext, context);
    }

    public static DocFilter parseDocFilter(
            final String rawDocFilter,
            final boolean useLegacy,
            final Query.Context context) {
        final JQLParser.DocFilterContext docFilterContext = runParser(rawDocFilter, new Function<JQLParser, JQLParser.DocFilterContext>() {
            @Nullable
            @Override
            public JQLParser.DocFilterContext apply(@Nullable final JQLParser input) {
                return input.docFilterEof(useLegacy).docFilter();
            }
        });
        return DocFilters.parseDocFilter(docFilterContext, context);
    }

    public static DocMetric parseDocMetric(
            final String rawDocMetric,
            final boolean useLegacy,
            final Query.Context context) {
        final JQLParser.DocMetricContext docMetricContext = runParser(rawDocMetric, new Function<JQLParser, JQLParser.DocMetricContext>() {
            @Nullable
            @Override
            public JQLParser.DocMetricContext apply(@Nullable final JQLParser input) {
                return input.docMetricEof(useLegacy).docMetric();
            }
        });
        return DocMetrics.parseDocMetric(docMetricContext, context);
    }

    public static class ParseResult {
        public final CharStream inputStream;
        public final Query query;

        public ParseResult(CharStream inputStream, Query query) {
            this.inputStream = inputStream;
            this.query = query;
        }
    }

    public static ParseResult parseQuery(String q, boolean useLegacy, DatasetsMetadata datasetsMetadata, final Set<String> defaultOptions, WallClock clock, final TracingTreeTimer timer, final ShardResolver shardResolver) {
        return parseQuery(q, useLegacy, datasetsMetadata, defaultOptions, s -> {}, clock, timer, shardResolver);
    }

    public static ParseResult parseQuery(String q, boolean useLegacy, DatasetsMetadata datasetsMetadata, final Set<String> defaultOptions, Consumer<String> warn, WallClock clock, final TracingTreeTimer timer, final ShardResolver shardResolver) {
        final JQLParser.QueryContext queryContext = parseQueryContext(q, useLegacy);
        return new ParseResult(queryContext.start.getInputStream(), Query.parseQuery(queryContext, datasetsMetadata, defaultOptions, warn, clock, timer, shardResolver));
    }

    private static String getText(CharStream inputStream, ParserRuleContext context, Set<Interval> seenComments) {
        if (context == null) {
            return "";
        }
        StringBuilder textBuilder = new StringBuilder();
        final Optional<Interval> previousNode = ParserUtil.getPreviousNode(context);
        if (previousNode.isPresent() && !seenComments.contains(previousNode.get())) {
            textBuilder.append(inputStream.getText(previousNode.get()));
            seenComments.add(previousNode.get());
        }
        textBuilder.append(getText(inputStream, context));
        final Optional<Interval> nextNode = ParserUtil.getNextNode(context);
        if (nextNode.isPresent() && !seenComments.contains(nextNode.get())) {
            textBuilder.append(inputStream.getText(nextNode.get()));
            seenComments.add(nextNode.get());
        }
        return textBuilder.toString();
    }

    private static String getText(CharStream inputStream, ParserRuleContext context) {
        if (context == null) {
            return "";
        } else {
            return inputStream.getText(new Interval(context.start.getStartIndex(), context.stop.getStopIndex()));
        }
    }

    public static SplitQuery parseSplitQuery(String q, boolean useLegacy, final Set<String> defaultOptions, WallClock clock, final DatasetsMetadata datasetsMetadata) {
        Set<Interval> seenComments = new HashSet<>();
        final JQLParser.QueryContext queryContext = parseQueryContext(q, useLegacy);
        final TracingTreeTimer timer = new TracingTreeTimer();
        final ShardResolver shardResolver = new NullShardResolver();
        final Query parsed = parseQuery(q, useLegacy, datasetsMetadata, defaultOptions, clock, timer, shardResolver).query;
        final CharStream queryInputStream = queryContext.start.getInputStream();
        final String from = getText(queryInputStream, queryContext.fromContents(), seenComments).trim();
        final String where;
        if (queryContext.whereContents() != null) {
            where = queryContext.whereContents().docFilter()
                    .stream()
                    .map(filter -> getText(queryInputStream, filter, seenComments))
                    .collect(Collectors.joining(" "))
                    .trim();
        } else {
            where = "";
        }

        final List<String> groupBys = extractGroupBys(queryContext, queryInputStream);
        final String groupBy = getText(queryInputStream, queryContext.groupByContents(), seenComments).trim();

        final List<String> selects = extractSelects(queryContext, queryInputStream);
        final String select = queryContext.selectContents().stream()
                .map(input -> getText(queryInputStream, input, seenComments))
                .collect(Collectors.joining(" "))
                .trim();

        final String dataset;
        final String start;
        final String startRawString;
        final String end;
        final String endRawString;

        if (queryContext.fromContents().datasetOptTime().isEmpty()) {
            final JQLParser.DatasetContext datasetCtx = queryContext.fromContents().dataset();
            dataset = datasetCtx.index.getText();
            start = parsed.datasets.get(0).startInclusive.unwrap().toString();
            startRawString = removeQuotes(getText(queryInputStream, datasetCtx.startTime, seenComments));
            end = parsed.datasets.get(0).endExclusive.unwrap().toString();
            endRawString = removeQuotes(getText(queryInputStream, datasetCtx.endTime, seenComments));
        } else {
            dataset = "";
            start = "";
            startRawString = "";
            end = "";
            endRawString = "";
        }

        return new SplitQuery(from, where, groupBy, select, "", extractHeaders(parsed),
                groupBys, selects, dataset, start, startRawString, end, endRawString,
                extractDatasets(queryContext.fromContents(), queryInputStream));
    }

    static List<SplitQuery.Dataset> extractDatasets(final JQLParser.FromContentsContext fromContentsContext, final CharStream queryInputStream) {
        final List<SplitQuery.Dataset> datasets = new ArrayList<>();
        final SplitQuery.Dataset ds1 = extractDataset(fromContentsContext.dataset(), queryInputStream);
        datasets.add(ds1);
        for (final JQLParser.DatasetOptTimeContext datasetOptTimeContext : fromContentsContext.datasetOptTime()) {
            datasetOptTimeContext.enterRule(new JQLBaseListener() {
                public void enterFullDataset(JQLParser.FullDatasetContext ctx) {
                    datasets.add(extractDataset(ctx.dataset(), queryInputStream));
                }

                public void enterPartialDataset(JQLParser.PartialDatasetContext ctx) {
                    datasets.add(extractPartialDataset(ctx, queryInputStream, ds1.start, ds1.end));
                }
            });
        }
        return datasets;
    }

    private static SplitQuery.Dataset extractDataset(final JQLParser.DatasetContext datasetContext, final CharStream queryInputStream) {
        final String start, end;
        if (datasetContext.start != null) {
            start = getText(queryInputStream, datasetContext.startTime);
            end = getText(queryInputStream, datasetContext.endTime);
        } else {
            start = "";
            end = "";
        }
        final String dataset = datasetContext.index.getText();
        final String fieldAlias = (datasetContext.aliases() != null) ?
                getText(queryInputStream, datasetContext.aliases()) : "";
        final String alias = (datasetContext.name != null) ? getText(queryInputStream, datasetContext.name) : "";
        final String where = (datasetContext.whereContents() != null) ?
                getText(queryInputStream, datasetContext.whereContents()) : "";
        return new SplitQuery.Dataset(dataset, where, start, end, alias, fieldAlias);
    }

    private static SplitQuery.Dataset extractPartialDataset(
            final JQLParser.PartialDatasetContext datasetContext, final CharStream queryInputStream, final String start, final String end) {
        final String dataset = getText(queryInputStream, datasetContext.index);
        final String fieldAlias = (datasetContext.aliases() != null) ?
                getText(queryInputStream, datasetContext.aliases()) : "";
        final String alias = (datasetContext.name != null) ? getText(queryInputStream, datasetContext.name) : "";
        final String where = (datasetContext.whereContents() != null) ?
                getText(queryInputStream, datasetContext.whereContents()) : "";
        return new SplitQuery.Dataset(dataset, where, start, end, alias, fieldAlias);
    }

    @VisibleForTesting
    public static List<String> extractHeaders(Query parsed) {
        final List<String> result = new ArrayList<>();
        for (GroupByEntry groupBy : parsed.groupBys) {
            result.add(groupBy.alias.orElseGet(groupBy.groupBy::getRawInput));
        }
        if (result.isEmpty()) {
            result.add("");
        }
        for (AggregateMetric metric : parsed.selects) {
            final Positional pos;
            if (metric instanceof AggregateMetric.Named) {
                pos = ((AggregateMetric.Named) metric).name;
            } else {
                if (metric.getStart() == null) {
                    result.add("count()");
                    continue;
                }
                pos = metric;
            }
            result.add(Objects.requireNonNull(pos.getRawInput()));
        }
        return result;
    }

    private static List<String> extractSelects(JQLParser.QueryContext queryContext, CharStream input) {
        final List<String> result = new ArrayList<>();
        for (final JQLParser.SelectContentsContext selectContent : queryContext.selectContents()) {
            for (final JQLParser.FormattedAggregateMetricContext select : selectContent.formattedAggregateMetric()) {
                result.add(getText(input, select));
            }
        }
        return result;
    }

    private static List<String> extractGroupBys(JQLParser.QueryContext queryContext, CharStream input) {
        final JQLParser.GroupByContentsContext groupByContents = queryContext.groupByContents();
        if (groupByContents == null) {
            return Collections.emptyList();
        }
        final List<String> result = new ArrayList<>();
        for (final JQLParser.GroupByEntryContext elem : groupByContents.groupByEntry()) {
            result.add(getText(input, elem));
        }
        return result;
    }

    private static String removeQuotes(String text) {
        if ((text.startsWith("\"") && text.endsWith("\""))
                || (text.startsWith("\'") && text.endsWith("\'"))) {
            return text.substring(1, text.length() - 1);
        }
        return text;
    }

    // Parse string
    // throw error if cannot parse
    public static <T> T runParser(final String input, final Function<JQLParser, T> applyParser) {
        return runParser(input, applyParser, true);
    }

    // Try parse string
    // return null if cannot parse
    public static <T> T tryRunParser(final String input, final Function<JQLParser, T> applyParser) {
        return runParser(input, applyParser, false);
    }

    private static <T> T runParser(
            final String input,
            final Function<JQLParser, T> applyParser,
            final boolean throwOnError) {
        final IqlKnownException.ParseErrorException error;
        try {
            final AtomicReference<IqlKnownException.ParseErrorException> exception = new AtomicReference<>();
            final ANTLRErrorListener errorListener = new BaseErrorListener() {
                @Override
                public void syntaxError(final Recognizer<?, ?> recognizer, final Object offendingSymbol, final int line, final int charPositionInLine, final String msg, final RecognitionException e) {
                    super.syntaxError(recognizer, offendingSymbol, line, charPositionInLine, msg, e);
                    if (exception.get() == null) {
                        exception.set(new IqlKnownException.ParseErrorException("Invalid input: [" + input + "] " + msg, e));
                    }
                }
            };
            final DefaultErrorStrategy errorStrategy = new DefaultErrorStrategy() {
                @Override
                public void reportError(final Parser recognizer, final RecognitionException e) {
                    super.reportError(recognizer, e);
                    if (exception.get() == null) {
                        final String message = e.getExpectedTokens().toString(JQLParser.VOCABULARY);
                        exception.set(new IqlKnownException.ParseErrorException(
                                "Invalid input: [" + input + "]" + ", expected " + message + ", found [" + e.getOffendingToken().getText() + "]",
                                e
                        ));
                    }
                }
            };
            final JQLParser parser = parserForString(input, errorListener, errorStrategy);
            final T result = applyParser.apply(parser);
            if ((parser.getNumberOfSyntaxErrors() == 0) && (exception.get() == null)) {
                return result;
            }

            if (!throwOnError) {
                return null;
            }
            if (exception.get() == null) {
                error = new IqlKnownException.ParseErrorException("Invalid input: [" + input + "]");
            } else {
                error = exception.get();
            }
        } catch (final Throwable t) {
            // Some unexpected error inside parser.
            throw new RuntimeException("Something went wrong inside query parser", t);
        }

        throw error;
    }

    public static JQLParser.QueryContext parseQueryContext(String q, final boolean useLegacy) {
        return runParser(q, parser -> parser.query(useLegacy));
    }

    public static AggregateMetric parseAggregateMetric(
            final String q,
            final boolean useLegacy,
            final Query.Context context) {
        final JQLParser.AggregateMetricContext aggregateMetricContext = runParser(q, new Function<JQLParser, JQLParser.AggregateMetricContext>() {
            @Nullable
            @Override
            public JQLParser.AggregateMetricContext apply(@Nullable final JQLParser input) {
                return input.aggregateMetricEof(useLegacy).aggregateMetric();
            }
        });
        return AggregateMetrics.parseAggregateMetric(aggregateMetricContext, context);
    }

    private static JQLParser parserForString(final String q, final ANTLRErrorListener errorListener, final DefaultErrorStrategy errorStrategy) {
        final JQLLexer lexer = new JQLLexer(new UpperCaseInputStream(new ANTLRInputStream(q)));
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);
        final CommonTokenStream tokens = new CommonTokenStream(lexer);
        final JQLParser parser = new JQLParser(tokens);
        parser.removeErrorListeners();
        parser.setErrorHandler(errorStrategy);
        return parser;
    }

    public static List<Command> queryCommands(final Query query) {
        return queryCommands(query, Optional.empty());
    }

    public static List<Command> queryCommands(
            final Query query,
            final Optional<List<AggregateMetric>> totals) {
        Loggers.trace(log, "query = %s", query);
        final Query query1 = FixTopKHaving.apply(query);
        Loggers.trace(log, "query1 = %s", query1);
        final Map<String, AggregateMetric> named = ExtractNames.extractNames(query1);
        final Query query2 = SubstituteNamed.substituteNamedMetrics(query1, named);
        Loggers.trace(log, "query2 = %s", query2);
        final Query query3 = RemoveNames.removeNames(query2);
        Loggers.trace(log, "query3 = %s", query3);
        final Query query4 = ConstantFolding.apply(query3);
        Loggers.trace(log, "query4 = %s", query4);
        final boolean extractTotals = totals.isPresent() && query4.useLegacy;
        final ExtractPrecomputed.Extracted extracted = ExtractPrecomputed.extractPrecomputed(query4, extractTotals);
        if (extractTotals) {
            totals.get().addAll(extracted.totals.get());
        }
        Loggers.trace(log, "extracted = %s", extracted);
        final HandleWhereClause.Result query5Result = HandleWhereClause.handleWhereClause(extracted.query);
        final List<ExecutionStep> firstSteps = query5Result.steps;

        final List<ExecutionStep> executionSteps = Lists.newArrayList(Iterables.concat(firstSteps, ExtractPrecomputed.querySteps(extracted)));
        if (log.isTraceEnabled()) {
            log.trace("executionSteps = " + executionSteps);
            for (final ExecutionStep executionStep : executionSteps) {
                log.trace("executionStep = " + executionStep);
            }
        }
        final List<ExecutionStep> executionSteps2 = OptimizeLast.optimize(executionSteps, query.rowLimit);
        if (log.isTraceEnabled()) {
            log.trace("executionSteps2 = " + executionSteps2);
            for (final ExecutionStep executionStep : executionSteps2) {
                log.trace("executionStep = " + executionStep);
            }
        }
        final List<ExecutionStep> executionSteps3 = FixFtgsMetricRunning.apply(executionSteps2);
        if (log.isTraceEnabled()) {
            log.trace("executionSteps3 = " + executionSteps3);
            for (final ExecutionStep executionStep : executionSteps3) {
                log.trace("executionStep = " + executionStep);
            }
        }
        final List<ExecutionStep> executionSteps4 = GroupIterations.apply(executionSteps3);
        if (log.isTraceEnabled()) {
            log.trace("executionSteps4 = " + executionSteps4);
            for (final ExecutionStep executionStep : executionSteps4) {
                log.trace("executionStep = " + executionStep);
            }
        }
        return executionSteps4.stream()
                .flatMap(x -> x.commands().stream())
                .collect(Collectors.toList());
    }

    public static List<Dataset> findAllDatasets(Query query) {
        // Cannot be Set, need to know duplicates.
        final List<Dataset> result = new ArrayList<>();
        result.addAll(query.datasets);
        query.transform(Function.identity(), Function.identity(), Function.identity(), Function.identity(), new Function<DocFilter, DocFilter>() {
            public DocFilter apply(DocFilter docFilter) {
                if (docFilter instanceof DocFilter.FieldInQuery) {
                    result.addAll(Queries.findAllDatasets(((DocFilter.FieldInQuery) docFilter).query));
                }
                return docFilter;
            }
        });
        return result;
    }
}
