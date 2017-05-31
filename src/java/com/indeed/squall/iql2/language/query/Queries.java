package com.indeed.squall.iql2.language.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.indeed.common.util.time.WallClock;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.GroupByMaybeHaving;
import com.indeed.squall.iql2.language.JQLBaseListener;
import com.indeed.squall.iql2.language.JQLLexer;
import com.indeed.squall.iql2.language.JQLParser;
import com.indeed.squall.iql2.language.Positional;
import com.indeed.squall.iql2.language.Positioned;
import com.indeed.squall.iql2.language.UpperCaseInputStream;
import com.indeed.squall.iql2.language.commands.Command;
import com.indeed.squall.iql2.language.compat.Consumer;
import com.indeed.squall.iql2.language.execution.ExecutionStep;
import com.indeed.squall.iql2.language.execution.passes.FixDistinctFilterRunning;
import com.indeed.squall.iql2.language.execution.passes.GroupIterations;
import com.indeed.squall.iql2.language.execution.passes.OptimizeLast;
import com.indeed.squall.iql2.language.optimizations.CollapseFilters;
import com.indeed.squall.iql2.language.passes.ExtractNames;
import com.indeed.squall.iql2.language.passes.ExtractPrecomputed;
import com.indeed.squall.iql2.language.passes.FixTopKHaving;
import com.indeed.squall.iql2.language.passes.HandleWhereClause;
import com.indeed.squall.iql2.language.passes.RemoveNames;
import com.indeed.squall.iql2.language.passes.SubstituteNamed;
import com.indeed.util.logging.Loggers;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Queries {
    private static final Logger log = Logger.getLogger(Queries.class);

    public static List<Map<String, String>> createDatasetMap(CharStream inputStream, Query query) {
        final List<Map<String, String>> result = new ArrayList<>();
        final ObjectMapper objectMapper = new ObjectMapper();
        for (final Dataset dataset : query.datasets) {
            final Map<String, String> m = new HashMap<>();
            m.put("dataset", dataset.dataset.unwrap());
            m.put("start", dataset.startInclusive.unwrap().toString());
            m.put("displayName", getRawInput(inputStream, dataset.getDisplayName()));
            m.put("end", dataset.endExclusive.unwrap().toString());
            m.put("name", dataset.alias.or(dataset.dataset).unwrap());
            try {
                final Map<String, String> unPositionedFieldAliases = new HashMap<>();
                for (final Map.Entry<Positioned<String>, Positioned<String>> entry : dataset.fieldAliases.entrySet()) {
                    unPositionedFieldAliases.put(entry.getKey().unwrap(), entry.getValue().unwrap());
                }
                m.put("fieldAliases", objectMapper.writeValueAsString(unPositionedFieldAliases));
            } catch (JsonProcessingException e) {
                // We really shouldn't have a problem serializing a Map<String, String> to a String...
                throw Throwables.propagate(e);
            }
            result.add(m);
        }
        return result;
    }

    public static class ParseResult {
        public final CharStream inputStream;
        public final Query query;

        public ParseResult(CharStream inputStream, Query query) {
            this.inputStream = inputStream;
            this.query = query;
        }
    }

    public static String getRawInput(CharStream inputStream, Positional positional) {
        return inputStream.getText(new Interval(positional.getStart().startIndex, positional.getEnd().stopIndex));
    }

    public static ParseResult parseQuery(String q, boolean useLegacy, Map<String, Set<String>> datasetToKeywordAnalyzerFields, Map<String, Set<String>> datasetToIntFields, WallClock clock) {
        return parseQuery(q, useLegacy, datasetToKeywordAnalyzerFields, datasetToIntFields, new Consumer<String>() {
            @Override
            public void accept(String s) {

            }
        }, clock);
    }

    public static ParseResult parseQuery(String q, boolean useLegacy, Map<String, Set<String>> datasetToKeywordAnalyzerFields, Map<String, Set<String>> datasetToIntFields, Consumer<String> warn, WallClock clock) {
        final JQLParser.QueryContext queryContext = parseQueryContext(q, useLegacy);
        return new ParseResult(queryContext.start.getInputStream(), Query.parseQuery(queryContext, datasetToKeywordAnalyzerFields, datasetToIntFields, warn, clock));
    }

    private static String getText(CharStream inputStream, ParserRuleContext context) {
        if (context == null) {
            return "";
        }
        return inputStream.getText(new Interval(context.start.getStartIndex(), context.stop.getStopIndex()));
    }

    public static SplitQuery parseSplitQuery(String q, boolean useLegacy, WallClock clock) {
        final JQLParser.QueryContext queryContext = parseQueryContext(q, useLegacy);
        final Query parsed = parseQuery(q, useLegacy, Collections.<String, Set<String>>emptyMap(), Collections.<String, Set<String>>emptyMap(), clock).query;
        final CharStream queryInputStream = queryContext.start.getInputStream();
        final String from = getText(queryInputStream, queryContext.fromContents());
        final String where;
        if (queryContext.whereContents() != null) {
            where = Joiner.on(' ').join(Iterables.transform(queryContext.whereContents().docFilter(), new Function<JQLParser.DocFilterContext, String>() {
                public String apply(@Nullable JQLParser.DocFilterContext input) {
                    return getText(queryInputStream, input);
                }
            }));
        } else {
            where = "";
        }

        final List<String> groupBys = extractGroupBys(queryContext, queryInputStream);
        final String groupBy = getText(queryInputStream, queryContext.groupByContents());

        final List<String> selects = extractSelects(queryContext, queryInputStream);
        final String select = Joiner.on(' ').join(Iterables.transform(queryContext.selectContents(), new Function<JQLParser.SelectContentsContext, String>() {
            public String apply(@Nullable JQLParser.SelectContentsContext input) {
                return getText(queryInputStream, input);
            }
        }));

        final String dataset;
        final String start;
        final String startRawString;
        final String end;
        final String endRawString;
        final List<SplitQuery.Dataset> datasets;

        if (queryContext.fromContents().datasetOptTime().isEmpty()) {
            final JQLParser.DatasetContext datasetCtx = queryContext.fromContents().dataset();
            dataset = datasetCtx.index.getText();
            start = parsed.datasets.get(0).startInclusive.unwrap().toString();
            startRawString = removeQuotes(getText(queryInputStream, datasetCtx.start));
            end = parsed.datasets.get(0).endExclusive.unwrap().toString();
            endRawString = removeQuotes(getText(queryInputStream, datasetCtx.end));
        } else {
            dataset = "";
            start = "";
            startRawString = "";
            end = "";
            endRawString = "";
        }

        return new SplitQuery(from, where, groupBy, select, "", extractHeaders(parsed, queryInputStream), groupBys, selects, dataset, start, startRawString, end, endRawString,
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
            start = getText(queryInputStream, datasetContext.start);
            end = getText(queryInputStream, datasetContext.end);
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
    static List<String> extractHeaders(Query parsed, CharStream input) {
        final List<String> result = new ArrayList<>();
        for (GroupByMaybeHaving groupBy : parsed.groupBys) {
            result.add(getRawInput(input, groupBy.groupBy));
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
            result.add(getRawInput(input, pos));
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
        for (final JQLParser.GroupByElementWithHavingContext elem : groupByContents.groupByElementWithHaving()) {
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

    public static <T> T runParser(String input, Function<JQLParser, T> applyParser) {
        final JQLParser parser = parserForString(input);
        final List<RecognitionException> exceptions = new ArrayList<>();
        parser.setErrorHandler(new DefaultErrorStrategy() {
            @Override
            public void reportError(Parser recognizer, RecognitionException e) {
                super.reportError(recognizer, e);
                exceptions.add(e);
            }
        });
        final T result = applyParser.apply(parser);
        if (parser.getNumberOfSyntaxErrors() > 0) {
            final String extra;
            if (exceptions.size() > 0) {
                final RecognitionException anException = exceptions.get(0);
                final String message = anException.getExpectedTokens().toString(JQLParser.VOCABULARY);
                extra = ", expected " + message + ", found [" + anException.getOffendingToken().getText() + "]";
            } else {
                extra = "";
            }
            throw new IllegalArgumentException("Invalid input: [" + input + "]" + extra);
        }
        return result;
    }

    public static JQLParser.QueryContext parseQueryContext(String q, final boolean useLegacy) {
        return runParser(q, new Function<JQLParser, JQLParser.QueryContext>() {
            public JQLParser.QueryContext apply(JQLParser input) {
                return input.query(useLegacy);
            }
        });
    }

    public static JQLParser parserForString(String q) {
        final JQLLexer lexer = new JQLLexer(new UpperCaseInputStream(new ANTLRInputStream(q)));
        final CommonTokenStream tokens = new CommonTokenStream(lexer);
        return new JQLParser(tokens);
    }

    public static List<Command> queryCommands(Query query) {
        Loggers.trace(log, "query = %s", query);
        final Query query1 = FixTopKHaving.apply(query);
        Loggers.trace(log, "query1 = %s", query1);
        final Map<String, AggregateMetric> named = ExtractNames.extractNames(query1);
        final Query query2 = SubstituteNamed.substituteNamedMetrics(query1, named);
        Loggers.trace(log, "query2 = %s", query2);
        final Query query3 = RemoveNames.removeNames(query2);
        Loggers.trace(log, "query3 = %s", query3);
        final Query query4 = CollapseFilters.collapseFilters(query3);
        Loggers.trace(log, "query4 = %s", query4);
        final HandleWhereClause.Result query5Result = HandleWhereClause.handleWhereClause(query4);
        final List<ExecutionStep> firstSteps = query5Result.steps;
        final ExtractPrecomputed.Extracted extracted = ExtractPrecomputed.extractPrecomputed(query5Result.query);
        Loggers.trace(log, "extracted = %s", extracted);
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
        final List<ExecutionStep> executionSteps3 = FixDistinctFilterRunning.apply(executionSteps2);
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
        return asCommands(executionSteps4);
    }

    static List<Command> asCommands(List<ExecutionStep> executionSteps) {
        final List<Command> commands = new ArrayList<>();
        for (final ExecutionStep executionStep : executionSteps) {
            commands.addAll(executionStep.commands());
        }
        return commands;
    }

    public static List<Dataset> findAllDatasets(Query query) {
        // Cannot be Set, need to know duplicates.
        final List<Dataset> result = new ArrayList<>();
        result.addAll(query.datasets);
        query.transform(Functions.<GroupBy>identity(), Functions.<AggregateMetric>identity(), Functions.<DocMetric>identity(), Functions.<AggregateFilter>identity(), new Function<DocFilter, DocFilter>() {
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
