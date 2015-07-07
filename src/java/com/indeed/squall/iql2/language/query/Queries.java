package com.indeed.squall.iql2.language.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.JQLLexer;
import com.indeed.squall.iql2.language.JQLParser;
import com.indeed.squall.iql2.language.UpperCaseInputStream;
import com.indeed.squall.iql2.language.commands.Command;
import com.indeed.squall.iql2.language.commands.SimpleIterate;
import com.indeed.squall.iql2.language.execution.ExecutionStep;
import com.indeed.squall.iql2.language.execution.passes.FixDistinctFilterRunning;
import com.indeed.squall.iql2.language.execution.passes.GroupIterations;
import com.indeed.squall.iql2.language.execution.passes.OptimizeLast;
import com.indeed.squall.iql2.language.optimizations.CollapseFilters;
import com.indeed.squall.iql2.language.passes.ExtractNames;
import com.indeed.squall.iql2.language.passes.ExtractPrecomputed;
import com.indeed.squall.iql2.language.passes.HandleWhereClause;
import com.indeed.squall.iql2.language.passes.RemoveNames;
import com.indeed.squall.iql2.language.passes.SubstituteNamed;
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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Queries {
    private static final Logger log = Logger.getLogger(Queries.class);

    @Nullable
    public static Iterator<String[]> executeQuery(String q, boolean useLegacy, Map<String, Set<String>> keywordAnalyzerWhitelist, Map<String, Set<String>> datasetToIntFields) throws IOException {
        final Query query = parseQuery(q, useLegacy, keywordAnalyzerWhitelist, datasetToIntFields);
        final List<Command> commands = queryCommands(query);
        final ObjectMapper objectMapper = new ObjectMapper();
        if (log.isTraceEnabled()) {
            log.trace("commands = " + commands);
            for (final Command command : commands) {
                log.trace("command = " + command);
                final String s = objectMapper.writeValueAsString(command);
                log.trace("s = " + s);
            }
            final String commandList = objectMapper.writeValueAsString(commands);
            log.trace("commandList = " + commandList);
        }
        final Map<String, Object> request = new HashMap<>();
        request.put("datasets", createDatasetMap(query));
        request.put("commands", commands);

        try {
            final long start = System.currentTimeMillis();
            final String v = objectMapper.writeValueAsString(request);
//            final Socket s = new Socket("jwolfe ", 28347);
            final Socket s = new Socket("dev-squall1", 28347);
            final PrintWriter writer = new PrintWriter(s.getOutputStream());
            System.out.println("v = " + v);
            writer.println(v);
            writer.flush();

            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(s.getInputStream()));

            if (commands.get(commands.size() - 1) instanceof SimpleIterate) {
                return new AbstractIterator<String[]>() {
                    @Override
                    protected String[] computeNext() {
                        try {
                            final String line = bufferedReader.readLine();
                            if (line.isEmpty()) {
                                s.close();
                                final long end = System.currentTimeMillis();
                                System.out.println("Inner (end - start) = " + (end - start));
                                return endOfData();
                            } else {
                                return line.split("\t");
                            }
                        } catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    }
                };
            }

            try {
                final String line = bufferedReader.readLine();
                final String stringResult = objectMapper.readTree(line).get(0).textValue();

                final List<String[]> lines = new ArrayList<>();
                for (final String resultLine : stringResult.split("\n")) {
                    lines.add(resultLine.split("\t"));
                }

                final long end = System.currentTimeMillis();
                System.out.println("Inner (end - start) = " + (end - start));

                return lines.iterator();
            } finally {
                s.close();
            }
        } catch (Exception e) {
            return null;
        }
    }

    public static List<Map<String, String>> createDatasetMap(Query query) {
        final List<Map<String, String>> result = new ArrayList<>();
        for (final Dataset dataset : query.datasets) {
            final Map<String, String> m = new HashMap<>();
            m.put("dataset", dataset.dataset);
            m.put("start", dataset.startInclusive.toString());
            m.put("end", dataset.endExclusive.toString());
            m.put("name", dataset.alias.or(dataset.dataset));
            result.add(m);
        }
        return result;
    }

    public static Query parseQuery(String q, boolean useLegacy, Map<String, Set<String>> datasetToKeywordAnalyzerFields, Map<String, Set<String>> datasetToIntFields) {
        final JQLParser.QueryContext queryContext = parseQueryContext(q, useLegacy);
        return Query.parseQuery(queryContext, datasetToKeywordAnalyzerFields, datasetToIntFields);
    }

    private static String getText(CharStream inputStream, ParserRuleContext context) {
        if (context == null) {
            return "";
        }
        return inputStream.getText(new Interval(context.start.getStartIndex(), context.stop.getStopIndex()));
    }

    public static SplitQuery parseSplitQuery(String q, boolean useLegacy) {
        final JQLParser.QueryContext queryContext = parseQueryContext(q, useLegacy);
        final Query parsed = parseQuery(q, useLegacy, Collections.<String, Set<String>>emptyMap(), Collections.<String, Set<String>>emptyMap());
        final CharStream queryInputStream = queryContext.start.getInputStream();
        final String from = getText(queryInputStream, queryContext.fromContents());
        final String where = Joiner.on(' ').join(Iterables.transform(queryContext.docFilter(), new Function<JQLParser.DocFilterContext, String>() {
            public String apply(@Nullable JQLParser.DocFilterContext input) {
                return getText(queryInputStream, input);
            }
        }));
        final String groupBy = getText(queryInputStream, queryContext.groupByContents());
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

        if (queryContext.fromContents().datasetOptTime().isEmpty()) {
            final JQLParser.DatasetContext datasetCtx = queryContext.fromContents().dataset();
            dataset = datasetCtx.index.getText();
            start = parsed.datasets.get(0).startInclusive.toString();
            startRawString = getText(queryInputStream, datasetCtx.start);
            end = parsed.datasets.get(0).endExclusive.toString();
            endRawString = getText(queryInputStream, datasetCtx.end);
        } else {
            dataset = "";
            start = "";
            startRawString = "";
            end = "";
            endRawString = "";
        }

        return new SplitQuery(from, where, groupBy, select, "", dataset, start, startRawString, end, endRawString);
    }

    public static JQLParser.QueryContext parseQueryContext(String q, boolean useLegacy) {
        final JQLParser parser = parserForString(q);
        final List<RecognitionException> exceptions = new ArrayList<>();
        parser.setErrorHandler(new DefaultErrorStrategy() {
            @Override
            public void reportError(Parser recognizer, RecognitionException e) {
                super.reportError(recognizer, e);
                exceptions.add(e);
            }
        });
        final JQLParser.QueryContext queryContext = parser.query(useLegacy);
        if (parser.getNumberOfSyntaxErrors() > 0) {
            final String extra;
            if (exceptions.size() > 0) {
                final RecognitionException anException = exceptions.get(0);
                final String message = anException.getExpectedTokens().toString(JQLParser.VOCABULARY);
                extra = ", expected " + message + ", found [" + anException.getOffendingToken().getText() + "]";
            } else {
                extra = "";
            }
            throw new IllegalArgumentException("Invalid query: [" + q + "]" + extra);
        }
        return queryContext;
    }

    public static JQLParser parserForString(String q) {
        final JQLLexer lexer = new JQLLexer(new UpperCaseInputStream(new ANTLRInputStream(q)));
        final CommonTokenStream tokens = new CommonTokenStream(lexer);
        return new JQLParser(tokens);
    }

    public static List<Command> queryCommands(Query query) {
        if (log.isTraceEnabled()) {
            log.trace("query = " + query);
        }
        final Map<String, AggregateMetric> named = ExtractNames.extractNames(query);
        final Query query2 = SubstituteNamed.substituteNamedMetrics(query, named);
        if (log.isTraceEnabled()) {
            log.trace("query2 = " + query2);
        }
        final Query query3 = RemoveNames.removeNames(query2);
        if (log.isTraceEnabled()) {
            log.trace("query3 = " + query3);
        }
        final Query query4 = CollapseFilters.collapseFilters(query3);
        if (log.isTraceEnabled()) {
            log.trace("query4 = " + query4);
        }
        final HandleWhereClause.Result query5Result = HandleWhereClause.handleWhereClause(query4);
        final List<ExecutionStep> firstSteps = query5Result.steps;
        final ExtractPrecomputed.Extracted extracted = ExtractPrecomputed.extractPrecomputed(query5Result.query);
        if (log.isTraceEnabled()) {
            log.trace("extracted = " + extracted);
        }
        final List<ExecutionStep> executionSteps = Lists.newArrayList(Iterables.concat(firstSteps, ExtractPrecomputed.querySteps(extracted)));
        if (log.isTraceEnabled()) {
            log.trace("executionSteps = " + executionSteps);
            for (final ExecutionStep executionStep : executionSteps) {
                log.trace("executionStep = " + executionStep);
            }
        }
        final List<ExecutionStep> executionSteps2 = OptimizeLast.optimize(executionSteps);
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
}
