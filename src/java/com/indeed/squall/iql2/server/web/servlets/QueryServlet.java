package com.indeed.squall.iql2.server.web.servlets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.ShardIdWithVersion;
import com.indeed.squall.iql2.server.dimensions.DimensionsLoader;
import com.indeed.squall.iql2.language.commands.Command;
import com.indeed.squall.iql2.language.query.Dataset;
import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.squall.iql2.language.query.Query;
import com.indeed.squall.iql2.server.web.ErrorResult;
import com.indeed.squall.iql2.server.web.ExecutionManager;
import com.indeed.squall.iql2.server.web.QueryLogEntry;
import com.indeed.squall.iql2.server.web.UsernameUtil;
import com.indeed.squall.iql2.server.web.cache.QueryCache;
import com.indeed.squall.iql2.execution.DatasetDescriptor;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.dimensions.DatasetDimensions;
import com.indeed.squall.iql2.server.web.data.KeywordAnalyzerWhitelistLoader;
import com.indeed.util.core.Pair;
import com.indeed.util.core.TreeTimer;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.Charsets;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.annotation.Nonnull;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Controller
public class QueryServlet {
    private static final Logger log = Logger.getLogger(QueryServlet.class);
    private static final Logger dataLog = Logger.getLogger("indeed.logentry");

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ImhotepClient imhotepClient;
    private final QueryCache queryCache;
    private final ExecutionManager executionManager;
    private final DimensionsLoader dimensionsLoader;
    private final KeywordAnalyzerWhitelistLoader keywordAnalyzerWhitelistLoader;

    private static final Pattern DESCRIBE_DATASET_PATTERN = Pattern.compile("((DESC)|(desc)) ([a-zA-Z0-9_]+)");
    private static final Pattern DESCRIBE_DATASET_FIELD_PATTERN = Pattern.compile("((DESC)|(desc)) ([a-zA-Z0-9_]+).([a-zA-Z0-9_]+)");

    @Autowired
    public QueryServlet(
            final ImhotepClient imhotepClient,
            final QueryCache queryCache,
            final ExecutionManager executionManager,
            final DimensionsLoader dimensionsLoader,
            final KeywordAnalyzerWhitelistLoader keywordAnalyzerWhitelistLoader) {
        this.imhotepClient = imhotepClient;
        this.queryCache = queryCache;
        this.executionManager = executionManager;
        this.dimensionsLoader = dimensionsLoader;
        this.keywordAnalyzerWhitelistLoader = keywordAnalyzerWhitelistLoader;
    }

    private Map<String, Set<String>> getKeywordAnalyzerWhitelist() {
        return (Map<String, Set<String>>) keywordAnalyzerWhitelistLoader.getKeywordAnalyzerWhitelist();
    }

    private Map<String, Set<String>> getDatasetToIntFields() throws IOException {
        return (Map<String, Set<String>>) keywordAnalyzerWhitelistLoader.getDatasetToIntFields();
    }

    private Map<String, DatasetDimensions> getDimensions() {
        return dimensionsLoader.getDimensions();
    }

    @RequestMapping(value={"/iql/query","/iql2/query"})
    public void query(
            final HttpServletRequest request,
            final HttpServletResponse response,
            final @Nonnull @RequestParam("q") String query
    ) throws ServletException, IOException, ImhotepOutOfMemoryException, TimeoutException {
        final int version = ServletUtil.getVersion(request);
        final String contentType = request.getHeader("Accept");
        final String httpUsername = UsernameUtil.getUserNameFromRequest(request);
        final String username = Strings.nullToEmpty(Strings.isNullOrEmpty(httpUsername) ? request.getParameter("username") : httpUsername);
        final TreeTimer timer = new TreeTimer();

        Throwable errorOccurred = null;

        long queryStartTimestamp = System.currentTimeMillis();

        final boolean isStream = contentType.contains("text/event-stream");
        // TODO: Check for username and client values
        try {
            final Matcher describeDatasetMatcher = DESCRIBE_DATASET_PATTERN.matcher(query);
            final Matcher describeDatasetFieldMatcher = DESCRIBE_DATASET_FIELD_PATTERN.matcher(query);
            if (describeDatasetMatcher.matches()) {
                final String dataset = describeDatasetMatcher.group(4);
                processDescribeDataset(response, contentType, dataset);
            } else if (describeDatasetFieldMatcher.matches()) {
                final String dataset = describeDatasetFieldMatcher.group(4);
                final String field = describeDatasetFieldMatcher.group(5);
                processDescribeField(response, contentType, dataset, field);
            } else if (query.trim().toLowerCase().equals("show datasets")) {
                processShowDatasets(response, contentType);
            } else {
                queryStartTimestamp = processSelect(response, query, version, username, timer, isStream);
            }
        } catch (Throwable e) {
            final boolean isJson = false;
            final boolean status500 = true;
            handleError(response, isJson, e, status500, isStream);
            errorOccurred = e;
        } finally {
            try {
                String remoteAddr = getForwardedForIPAddress(request);
                if (remoteAddr == null) {
                    remoteAddr = request.getRemoteAddr();
                }
                logQuery(request, query, username, queryStartTimestamp, errorOccurred, remoteAddr);
            } catch (Throwable ignored) {
                // Do nothing
            }
        }
        System.out.println(timer);
    }

    public static void handleError(HttpServletResponse response, boolean isJson, Throwable e, boolean status500, boolean isStream) throws IOException {
        if(!(e instanceof Exception || e instanceof OutOfMemoryError)) {
            throw Throwables.propagate(e);
        }
        // output parse/execute error
        if (!isJson) {
            final PrintWriter printStream = response.getWriter();
            if (isStream) {
                response.setContentType("text/event-stream");
                final String[] stackTrace = Throwables.getStackTraceAsString(e).split("\\n");
                printStream.println("event: servererror");
                for (String s : stackTrace) {
                    printStream.println("data: " + s);
                }
                printStream.println();
            } else {
                response.setStatus(500);
                e.printStackTrace(printStream);
                printStream.close();
            }
        } else {
            if(status500) {
                response.setStatus(500);
            }
            // construct a parsed error object to be JSON serialized
            String clause = "";
            int offset = -1;
//            if(e instanceof IQLParseException) {
//                final IQLParseException IQLParseException = (IQLParseException) e;
//                clause = IQLParseException.getClause();
//                offset = IQLParseException.getOffsetInClause();
//            }
            final String stackTrace = Throwables.getStackTraceAsString(Throwables.getRootCause(e));
            final ErrorResult error = new ErrorResult(e.getClass().getSimpleName(), e.getMessage(), stackTrace, clause, offset);
            response.setContentType("application/json");
            final PrintWriter outputStream = response.getWriter();
            OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(outputStream, error);
            outputStream.close();
        }
    }

    private void processShowDatasets(HttpServletResponse response, String contentType) throws IOException {
        final Map<Host, List<DatasetInfo>> shardListMap = imhotepClient.getShardList();
        final Set<String> datasets = new TreeSet<>();
        for (final List<DatasetInfo> datasetInfos : shardListMap.values()) {
            for (final DatasetInfo datasetInfo : datasetInfos) {
                datasets.add(datasetInfo.getDataset());
            }
        }
        final List<Map<String, String>> datasetWithEmptyDescriptions = new ArrayList<>();
        for (final String dataset : datasets) {
            datasetWithEmptyDescriptions.add(ImmutableMap.of("name", dataset, "description", ""));
        }
        if (contentType.contains("application/json") || contentType.contains("*/*")) {
            response.getWriter().println(OBJECT_MAPPER.writeValueAsString(ImmutableMap.of("datasets", datasetWithEmptyDescriptions)));
        } else {
            throw new IllegalArgumentException("Don't know what to do with request Accept: [" + contentType + "]");
        }
    }

    private void processDescribeField(HttpServletResponse response, String contentType, String dataset, String field) throws IOException {
        final DatasetInfo datasetInfo = Session.getDatasetShardList(imhotepClient, dataset);

        final String type;
        final String imhotepType;
        if (datasetInfo.getIntFields().contains(field)) {
            type = "Integer";
            if (datasetInfo.getStringFields().contains(field)) {
                imhotepType = "String";
            } else {
                imhotepType = "Integer";
            }
        } else if (datasetInfo.getStringFields().contains(field)) {
            type = "String";
            imhotepType = "String";
        } else {
            throw new IllegalArgumentException("[" + field + "] is not present in [" + dataset + "]");
        }

        if (contentType.contains("application/json") || contentType.contains("*/*")) {
            final Map<String, Object> result = new HashMap<>();
            result.put("name", field);
            result.put("description", "");
            result.put("type", type);
            result.put("imhotepType", imhotepType);
            result.put("topTerms", Collections.emptyList());
            response.getWriter().println(OBJECT_MAPPER.writeValueAsString(result));
        } else {
            throw new IllegalArgumentException("Don't know what to do with request Accept: [" + contentType + "]");
        }
    }

    private void processDescribeDataset(HttpServletResponse response, String contentType, String dataset) throws IOException {
        final DatasetDescriptor datasetDescriptor = DatasetDescriptor.from(Session.getDatasetShardList(imhotepClient, dataset));
        if (contentType.contains("application/json") || contentType.contains("*/*")) {
            response.getWriter().println(OBJECT_MAPPER.writeValueAsString(datasetDescriptor));
        } else {
            throw new IllegalArgumentException("Don't know what to do with request Accept: [" + contentType + "]");
        }
    }

    /**
     * Gets the value associated with the last X-Forwarded-For header in the request. WARNING: the contract of HttpServletRequest does not assert anything about
     * the order in which the header values will be returned. I have examined the Tomcat source to establish that it does return the values in order, but this
     * behavior should not be assumed from other servlet containers.
     *
     * @param req request
     * @return the X-Forwarded-For IP address or null if none
     */
    private static String getForwardedForIPAddress(final HttpServletRequest req) {
        return getForwardedForIPAddress(req, "X-Forwarded-For");
    }

    private static String getForwardedForIPAddress(final HttpServletRequest req, final String forwardForHeaderName) {
        final Enumeration headers = req.getHeaders(forwardForHeaderName);
        String value = null;
        while (headers.hasMoreElements()) {
            value = (String) headers.nextElement();
        }
        return value;
    }

    private long processSelect(HttpServletResponse response, String query, int version, String username, TreeTimer timer, boolean isStream) throws TimeoutException, IOException, ImhotepOutOfMemoryException {
        if (isStream) {
            response.setHeader("Content-Type", "text/event-stream;charset=utf-8");
        } else {
            response.setHeader("Content-Type", "text/plain;charset=utf-8");
        }
        final ExecutionManager.QueryTracker queryTracker = executionManager.queryStarted(query, username);
        long queryStartTimestamp;
        try {
            timer.push("Acquire concurrent query lock");
            queryTracker.acquireLocks(); // blocks and waits if necessary
            timer.pop();
            queryStartTimestamp = System.currentTimeMillis(); // ignore time spent waiting
            final PrintWriter outputStream = response.getWriter();
            if (isStream) {
                outputStream.println(": This is the start of the IQL Query Stream");
                outputStream.println();
                outputStream.println("event: resultstream");
            }
            final Consumer<String> out;
            if (isStream) {
                out = new Consumer<String>() {
                    @Override
                    public void accept(String s) {
                        outputStream.print("data: ");
                        outputStream.println(s);
                    }
                };
            } else {
                out = new Consumer<String>() {
                    @Override
                    public void accept(String s) {
                        outputStream.println(s);
                    }
                };
            }
            executeSelect(query, version == 1, getKeywordAnalyzerWhitelist(), getDatasetToIntFields(), out, timer, queryTracker);
            if (isStream) {
                outputStream.println();
                outputStream.println("event: header");

                final Map<String, Object> headerMap = new HashMap<>();
                headerMap.put("IQL-Cached", "false");
                headerMap.put("IQL-Timings", timer.toString().replaceAll("\n", "\t"));
                headerMap.put("IQL-Shard-List", "");
                headerMap.put("IQL-Newest-Shard", DateTime.now().toString());
                headerMap.put("IQL-Imhotep-Temp-Bytes-Written", "0");
                headerMap.put("IQL-Totals", "[]");
                outputStream.println("data: " + OBJECT_MAPPER.writeValueAsString(headerMap));

                outputStream.println();
                outputStream.println("event: complete");
                outputStream.println("data: :)");
            }
        } finally {
            if (!queryTracker.isAsynchronousRelease()) {
                queryTracker.close();
            }
        }
        return queryStartTimestamp;
    }

    private void executeSelect(String q, boolean useLegacy, Map<String, Set<String>> keywordAnalyzerWhitelist, Map<String, Set<String>> datasetToIntFields, Consumer<String> out, TreeTimer timer, ExecutionManager.QueryTracker queryTracker) throws IOException, ImhotepOutOfMemoryException {
        timer.push(q);

        timer.push("parse query");
        final Query query = Queries.parseQuery(q, useLegacy, keywordAnalyzerWhitelist, datasetToIntFields);
        timer.pop();

        timer.push("compute commands");
        final List<Command> commands = Queries.queryCommands(query);
        timer.pop();

        timer.push("compute hash");
        final Set<Pair<String, String>> shards = Sets.newHashSet();
        for (final Dataset dataset : query.datasets) {
            timer.push("get chosen shards");
            final List<ShardIdWithVersion> chosenShards = imhotepClient.sessionBuilder(dataset.dataset, dataset.startInclusive, dataset.endExclusive).getChosenShards();
            timer.pop();
            for (final ShardIdWithVersion chosenShard : chosenShards) {
                shards.add(Pair.of(dataset.dataset, chosenShard.getShardId()));
            }
        }
        final String queryHash = computeQueryHash(commands, shards);
        final String cacheFileName = queryHash + ".tsv";
        timer.pop();

        try (final Closer closer = Closer.create()) {
            if (queryCache.isEnabled()) {
                timer.push("cache check");
                final boolean isCached = queryCache.isFileCached(cacheFileName);
                timer.pop();

                if (isCached) {
                    timer.push("read cache");
                    sendCachedQuery(cacheFileName, out, query.rowLimit);
                    timer.pop();
                    return;
                } else {
                    final Consumer<String> oldOut = out;
                    final Path tmpFile = Files.createTempFile("query", ".cache.tmp");
                    final File cacheFile = tmpFile.toFile();
                    final BufferedWriter cacheWriter = new BufferedWriter(new FileWriter(cacheFile));
                    closer.register(new Closeable() {
                        @Override
                        public void close() throws IOException {
                            // TODO: Do this stuff asynchronously
                            cacheWriter.close();
                            queryCache.writeFromFile(cacheFileName, cacheFile);
                            if (!cacheFile.delete()) {
                                log.warn("Failed to delete  " + cacheFile);
                            }
                        }
                    });
                    final int rowLimit = query.rowLimit.or(Integer.MAX_VALUE);
                    out = new Consumer<String>() {
                        int rowsWritten = 0;

                        @Override
                        public void accept(String s) {
                            if (rowsWritten < rowLimit) {
                                oldOut.accept(s);
                                rowsWritten += 1;
                            }
                            try {
                                cacheWriter.write(s);
                                cacheWriter.newLine();
                            } catch (IOException e) {
                                throw Throwables.propagate(e);
                            }
                        }
                    };
                }
            }

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
            request.put("datasets", Queries.createDatasetMap(query));
            request.put("commands", commands);

            final JsonNode requestJson = OBJECT_MAPPER.valueToTree(request);

            Session.createSession(imhotepClient, requestJson, closer, out, getDimensions(), timer);
        }
        timer.pop();
    }

    private void sendCachedQuery(String cacheFile, Consumer<String> out, Optional<Integer> rowLimit) throws IOException {
        final int limit = rowLimit.or(Integer.MAX_VALUE);
        int rowsWritten = 0;
        try (final BufferedReader stream = new BufferedReader(new InputStreamReader(queryCache.getInputStream(cacheFile)))) {
            String line;
            while ((line = stream.readLine()) != null) {
                out.accept(line);
                rowsWritten += 1;
                if (rowsWritten >= limit) {
                    break;
                }
            }
        }
    }

    private static String computeQueryHash(List<Command> commands, Set<Pair<String, String>> shards) {
        final MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            log.error("Failed to init SHA1", e);
            throw Throwables.propagate(e);
        }
        for (final Command command : commands) {
            sha1.update(command.toString().getBytes(Charsets.UTF_8));
        }
        for (final Pair<String, String> pair : shards) {
            sha1.update(pair.getFirst().getBytes(Charsets.UTF_8));
            sha1.update(pair.getSecond().getBytes(Charsets.UTF_8));
        }
        return Base64.encodeBase64URLSafeString(sha1.digest());
    }

    private static final int QUERY_LENGTH_LIMIT = 55000; // trying to not cause the logentry to overflow from being larger than 2^16

    private void logQuery(HttpServletRequest req,
                          String query,
                          String userName,
                          long queryStartTimestamp,
                          Throwable errorOccurred,
                          String remoteAddr) {
        final long timeTaken = System.currentTimeMillis() - queryStartTimestamp;
        if(timeTaken > 5000) {  // we've already logged the query so only log again if it took a long time to run
            logQueryToLog4J(query, (Strings.isNullOrEmpty(userName) ? remoteAddr : userName), timeTaken);
        }

        final String client = Strings.nullToEmpty(req.getParameter("client"));

        final QueryLogEntry logEntry = new QueryLogEntry();
        logEntry.setProperty("v", 0);
        logEntry.setProperty("username", userName);
        logEntry.setProperty("client", client);
        logEntry.setProperty("raddr", Strings.nullToEmpty(remoteAddr));
        logEntry.setProperty("starttime", Long.toString(queryStartTimestamp));
        logEntry.setProperty("tottime", (int)timeTaken);

        final List<String> params = Lists.newArrayList();
        final Enumeration<String> paramsEnum = req.getParameterNames();
        while(paramsEnum.hasMoreElements()) {
            final String param = paramsEnum.nextElement();
            // TODO: Add whitelist
            params.add(param);
        }
        logEntry.setProperty("params", Joiner.on(' ').join(params));
        final String queryToLog = query.length() > QUERY_LENGTH_LIMIT ? query.substring(0, QUERY_LENGTH_LIMIT) : query;
        logEntry.setProperty("q", queryToLog);
        logEntry.setProperty("qlen", query.length());
        logEntry.setProperty("error", errorOccurred != null ? "1" : "0");
        if(errorOccurred != null) {
            logEntry.setProperty("exceptiontype", errorOccurred.getClass().getSimpleName());
            String message = errorOccurred.getMessage();
            if (message == null) {
                message = "<no msg>";
            }
            logEntry.setProperty("exceptionmsg", message);
        }

        // TODO: Log semantic information about the query.
//        final String queryType = logStatementData(parsedQuery, selectExecutionStats, logEntry);
//        logEntry.setProperty("statement", queryType);

        dataLog.info(logEntry);
    }

    private void logQueryToLog4J(String query, String identification, long timeTaken) {
        if(query.length() > 500) {
            query = query.replaceAll("\\(([^\\)]{0,100}+)[^\\)]+\\)", "\\($1\\.\\.\\.\\)");
        }
        final String timeTakenStr = timeTaken >= 0 ? String.valueOf(timeTaken) : "";
        log.info((timeTaken < 0 ? "+" : "-") + identification + "\t" + timeTakenStr + "\t" + query);
    }
}
