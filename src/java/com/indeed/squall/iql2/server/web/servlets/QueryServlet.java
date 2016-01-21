package com.indeed.squall.iql2.server.web.servlets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.primitives.Ints;
import com.indeed.common.util.time.StoppedClock;
import com.indeed.common.util.time.WallClock;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.ShardIdWithVersion;
import com.indeed.squall.iql2.execution.DatasetDescriptor;
import com.indeed.squall.iql2.execution.FieldDescriptor;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.dimensions.DatasetDimensions;
import com.indeed.squall.iql2.execution.progress.NoOpProgressCallback;
import com.indeed.squall.iql2.execution.progress.ProgressCallback;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.ScopedField;
import com.indeed.squall.iql2.language.commands.Command;
import com.indeed.squall.iql2.language.query.Dataset;
import com.indeed.squall.iql2.language.query.GroupBy;
import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.squall.iql2.language.query.Query;
import com.indeed.squall.iql2.language.util.DatasetsFields;
import com.indeed.squall.iql2.server.SessionCollectingProgressCallback;
import com.indeed.squall.iql2.server.dimensions.DimensionsLoader;
import com.indeed.squall.iql2.server.web.AccessControl;
import com.indeed.squall.iql2.server.web.ErrorResult;
import com.indeed.squall.iql2.server.web.ExecutionManager;
import com.indeed.squall.iql2.server.web.QueryLogEntry;
import com.indeed.squall.iql2.server.web.UsernameUtil;
import com.indeed.squall.iql2.server.web.cache.QueryCache;
import com.indeed.squall.iql2.server.web.data.KeywordAnalyzerWhitelistLoader;
import com.indeed.squall.iql2.server.web.topterms.TopTermsCache;
import com.indeed.util.core.Pair;
import com.indeed.util.core.TreeTimer;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.Charsets;
import org.apache.log4j.Logger;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final AccessControl accessControl;
    private final TopTermsCache topTermsCache;
    private final Long imhotepLocalTempFileSizeLimit;
    private final Long imhotepDaemonTempFileSizeLimit;
    private final WallClock clock;
    private final Long subQueryTermLimit;

    private static final Pattern DESCRIBE_DATASET_PATTERN = Pattern.compile("((DESC)|(desc)) ([a-zA-Z0-9_]+)");
    private static final Pattern DESCRIBE_DATASET_FIELD_PATTERN = Pattern.compile("((DESC)|(desc)) ([a-zA-Z0-9_]+).([a-zA-Z0-9_]+)");

    @Autowired
    public QueryServlet(
            final ImhotepClient imhotepClient,
            final QueryCache queryCache,
            final ExecutionManager executionManager,
            final DimensionsLoader dimensionsLoader,
            final KeywordAnalyzerWhitelistLoader keywordAnalyzerWhitelistLoader,
            final AccessControl accessControl,
            final TopTermsCache topTermsCache,
            final Long imhotepLocalTempFileSizeLimit,
            final Long imhotepDaemonTempFileSizeLimit,
            final WallClock clock,
            final Long subQueryTermLimit
    ) {
        this.imhotepClient = imhotepClient;
        this.queryCache = queryCache;
        this.executionManager = executionManager;
        this.dimensionsLoader = dimensionsLoader;
        this.keywordAnalyzerWhitelistLoader = keywordAnalyzerWhitelistLoader;
        this.accessControl = accessControl;
        this.topTermsCache = topTermsCache;
        this.imhotepLocalTempFileSizeLimit = imhotepLocalTempFileSizeLimit;
        this.imhotepDaemonTempFileSizeLimit = imhotepDaemonTempFileSizeLimit;
        this.clock = clock;
        this.subQueryTermLimit = subQueryTermLimit;
    }

    private static Map<String, Set<String>> upperCaseMapToSet(Map<String, ? extends Set<String>> map) {
        final Map<String, Set<String>> upperCased = new HashMap<>();
        for (final Map.Entry<String, ? extends Set<String>> e : map.entrySet()) {
            final Set<String> upperCaseTerms = new HashSet<>(e.getValue().size());
            for (final String term : e.getValue()) {
                upperCaseTerms.add(term.toUpperCase());
            }
            upperCased.put(e.getKey().toUpperCase(), upperCaseTerms);
        }
        return upperCased;
    }

    private Map<String, Set<String>> getKeywordAnalyzerWhitelist() {
        // TODO: Don't make a copy per use
        return upperCaseMapToSet(keywordAnalyzerWhitelistLoader.getKeywordAnalyzerWhitelist());
    }

    private Map<String, Set<String>> getDatasetToIntFields() throws IOException {
        // TODO: Don't make a copy per use
        return upperCaseMapToSet(keywordAnalyzerWhitelistLoader.getDatasetToIntFields());
    }

    private Map<String, DatasetDimensions> getDimensions() {
        // TODO: Uppercase it?
        return dimensionsLoader.getDimensions();
    }

    @RequestMapping("query")
    public void query(
            final HttpServletRequest request,
            final HttpServletResponse response,
            final @Nonnull @RequestParam("q") String query
    ) throws ServletException, IOException, ImhotepOutOfMemoryException, TimeoutException {
        final WallClock clock = new StoppedClock(this.clock.currentTimeMillis());

        final int version = ServletUtil.getVersion(request);
        final String contentType = request.getHeader("Accept");
        final String httpUsername = UsernameUtil.getUserNameFromRequest(request);
        final String username = Strings.nullToEmpty(Strings.isNullOrEmpty(httpUsername) ? request.getParameter("username") : httpUsername);
        final TreeTimer timer = new TreeTimer() {
            @Override
            public void push(String s) {
                super.push(s);
                log.info(s);
            }
        };

        log.info("Query received from user [" + username + "]");

        Throwable errorOccurred = null;

        long queryStartTimestamp = System.currentTimeMillis();

        final boolean isStream = contentType.contains("text/event-stream");
        String statementType = "invalid";
        // TODO: Check for username and client values
        try {
            if(Strings.isNullOrEmpty(request.getParameter("client")) && Strings.isNullOrEmpty(username)) {
                throw new RuntimeException("IQL query requests have to include parameters 'client' and 'username' for identification");
            }
            accessControl.checkAllowedAccess(username);

            final Matcher describeDatasetMatcher = DESCRIBE_DATASET_PATTERN.matcher(query);
            final Matcher describeDatasetFieldMatcher = DESCRIBE_DATASET_FIELD_PATTERN.matcher(query);
            if (describeDatasetMatcher.matches()) {
                final String dataset = describeDatasetMatcher.group(4);
                processDescribeDataset(response, contentType, dataset);
                statementType = "describe";
            } else if (describeDatasetFieldMatcher.matches()) {
                final String dataset = describeDatasetFieldMatcher.group(4);
                final String field = describeDatasetFieldMatcher.group(5);
                processDescribeField(response, contentType, dataset, field);
                statementType = "describe";
            } else if (query.trim().toLowerCase().equals("show datasets")) {
                processShowDatasets(response, contentType);
                statementType = "show";
            } else {
                final boolean skipValidation = "1".equals(request.getParameter("skipValidation"));
                final Integer groupLimit;
                if (request.getParameter("groupLimit") != null) {
                    groupLimit = Integer.parseInt(request.getParameter("groupLimit"));
                } else {
                    groupLimit = null;
                }
                queryStartTimestamp =
                        processSelect(
                                response,
                                query,
                                version,
                                username,
                                timer,
                                isStream,
                                skipValidation,
                                groupLimit,
                                clock
                        );
                statementType = "select";
            }
        } catch (Throwable e) {
            final boolean isJson = false;
            final boolean status500 = true;
            handleError(response, isJson, e, status500, isStream);
            log.error("Error occurred", e);
            errorOccurred = e;
        } finally {
            try {
                String remoteAddr = getForwardedForIPAddress(request);
                if (remoteAddr == null) {
                    remoteAddr = request.getRemoteAddr();
                }
                logQuery(request, query, username, queryStartTimestamp, errorOccurred, remoteAddr, statementType);
            } catch (Throwable ignored) {
                // Do nothing
            }
        }
        log.info(timer);
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
            final List<String> topTerms = topTermsCache.getTopTerms(dataset, field);
            result.put("topTerms", topTerms);
            response.getWriter().println(OBJECT_MAPPER.writeValueAsString(result));
        } else {
            throw new IllegalArgumentException("Don't know what to do with request Accept: [" + contentType + "]");
        }
    }

    private void processDescribeDataset(HttpServletResponse response, String contentType, String dataset) throws IOException {
        final DatasetDescriptor datasetDescriptor = DatasetDescriptor.from(Session.getDatasetShardList(imhotepClient, dataset), getDimensions().get(dataset), getDatasetToIntFields().get(dataset));
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

    private long processSelect(HttpServletResponse response, String query, int version, String username, TreeTimer timer, final boolean isStream, boolean skipValidation, Integer groupLimit, WallClock clock) throws TimeoutException, IOException, ImhotepOutOfMemoryException {
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
            final long startTime = clock.currentTimeMillis();
            timer.pop();
            queryStartTimestamp = System.currentTimeMillis(); // ignore time spent waiting
            final PrintWriter outputStream = response.getWriter();
            if (isStream) {
                outputStream.println(": This is the start of the IQL Query Stream");
                outputStream.println();
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
            final Set<String> warnings = new HashSet<>();
            final SessionCollectingProgressCallback progressCallback = new SessionCollectingProgressCallback(new NoOpProgressCallback() {
                private int completedChunks = 0;

                @Override
                public void startSession(Optional<Integer> numCommands) {
                    if (isStream) {
                        outputStream.println("event: totalsteps");
                        outputStream.println("data: " + numCommands.get());
                        outputStream.println();
                        outputStream.flush();
                    }
                }

                private void incrementChunksCompleted() {
                    completedChunks += 1;
                    if (isStream) {
                        outputStream.println("event: chunkcomplete");
                        outputStream.println("data: " + completedChunks);
                        outputStream.println();
                        outputStream.flush();
                    }
                }

                @Override
                public void startCommand(com.indeed.squall.iql2.execution.commands.Command command, boolean streamingToTSV) {
                    if (command != null && isStream) {
                        outputStream.println(": Starting " + command.getClass().getSimpleName());
                        outputStream.println();
                    }

                    if (streamingToTSV) {
                        incrementChunksCompleted();

                        if (isStream) {
                            outputStream.println("event: resultstream");
                        }
                    }
                }

                @Override
                public void endCommand(com.indeed.squall.iql2.execution.commands.Command command) {
                    if (isStream) {
                        outputStream.println(": Completed " + command.getClass().getSimpleName());
                        outputStream.println();
                        outputStream.flush();
                    }
                    incrementChunksCompleted();
                }
            });
            final SelectExecutionInformation execInfo = executeSelect(query, version == 1, getKeywordAnalyzerWhitelist(), getDatasetToIntFields(), out, timer, progressCallback, new com.indeed.squall.iql2.language.compat.Consumer<String>() {
                @Override
                public void accept(String s) {
                    warnings.add(s);
                }
            }, skipValidation, groupLimit, clock, username);
            if (isStream) {
                outputStream.println();
                outputStream.println("event: header");

                // TODO: Fix these headers
                final Map<String, Object> headerMap = new HashMap<>();
                headerMap.put("IQL-Cached", execInfo.allCached());
                headerMap.put("IQL-Timings", timer.toString().replaceAll("\n", "\t"));
                headerMap.put("IQL-Shard-Lists", execInfo.perDatasetShardIds().toString());
                headerMap.put("IQL-Newest-Shard", ISODateTimeFormat.dateTime().print(execInfo.newestShard()));
                headerMap.put("IQL-Imhotep-Temp-Bytes-Written", execInfo.imhotepTempBytesWritten);
                headerMap.put("Imhotep-Session-IDs", progressCallback.getSessionIds());
                headerMap.put("IQL-Execution-Time", ISODateTimeFormat.dateTime().print(startTime));
                outputStream.println("data: " + OBJECT_MAPPER.writeValueAsString(headerMap));
                outputStream.println();

                if (!warnings.isEmpty()) {
                    outputStream.println("event: warnings");
                    for (final String warning : warnings) {
                        outputStream.println("data: " + warning);
                    }
                    outputStream.println();
                }

                outputStream.println("event: complete");
                outputStream.println("data: :)");
                outputStream.println();
            }
        } finally {
            if (!queryTracker.isAsynchronousRelease()) {
                queryTracker.close();
            }
        }
        return queryStartTimestamp;
    }

    private static DatasetsFields getDatasetsFields(List<Dataset> relevantDatasets, Map<String, String> nameToUppercaseDataset, ImhotepClient imhotepClient, Map<String, DatasetDimensions> dimensions, Map<String, Set<String>> datasetToIntFields) {
        final Set<String> relevantUpperCaseDatasets = new HashSet<>();
        for (final Dataset dataset : relevantDatasets) {
            relevantUpperCaseDatasets.add(dataset.dataset.toUpperCase());
        }

        final Map<String, String> datasetUpperCaseToActual = new HashMap<>();
        for (final String dataset : Session.getDatasets(imhotepClient)) {
            final String normalized = dataset.toUpperCase();
            if (!relevantUpperCaseDatasets.contains(normalized)) {
                continue;
            }
            if (datasetUpperCaseToActual.containsKey(normalized)) {
                throw new IllegalStateException("Multiple datasets with same uppercase name!");
            }
            datasetUpperCaseToActual.put(normalized, dataset);
        }

        final DatasetsFields.Builder builder = DatasetsFields.builder();
        for (final Map.Entry<String, String> entry : nameToUppercaseDataset.entrySet()) {
            final String dataset = datasetUpperCaseToActual.get(entry.getValue());

            final DatasetInfo datasetInfo = Session.getDatasetShardList(imhotepClient, dataset);
            final DatasetDimensions dimension = dimensions.get(dataset);
            final Set<String> intFields = datasetToIntFields.get(entry.getValue());

            final DatasetDescriptor datasetDescriptor = DatasetDescriptor.from(datasetInfo, dimension, intFields);

            final String name = entry.getKey().toUpperCase();
            for (final FieldDescriptor fieldDescriptor : datasetDescriptor.getFields()) {
                switch (fieldDescriptor.getType()) {
                    case "Integer":
                        builder.addIntField(name, fieldDescriptor.getName().toUpperCase());
                        break;
                    case "String":
                        builder.addStringField(name, fieldDescriptor.getName().toUpperCase());
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid FieldDescriptor type: " + fieldDescriptor.getType());
                }
            }

            builder.addIntField(name, "count()");
        }

        return builder.build();
    }

    private static class SelectExecutionInformation {
        public final Map<String, List<ShardIdWithVersion>> shards;
        public final Map<Query, Boolean> queryCached;
        public final long imhotepTempBytesWritten;

        private SelectExecutionInformation(Map<String, List<ShardIdWithVersion>> shards, Map<Query, Boolean> queryCached, long imhotepTempBytesWritten) {
            this.shards = shards;
            this.queryCached = queryCached;
            this.imhotepTempBytesWritten = imhotepTempBytesWritten;
        }

        public boolean allCached() {
            for (final boolean b : queryCached.values()) {
                if (!b) {
                    return false;
                }
            }
            return true;
        }

        public Map<String, List<String>> perDatasetShardIds() {
            final Map<String, List<String>> result = new HashMap<>(shards.size());
            for (final Map.Entry<String, List<ShardIdWithVersion>> entry : shards.entrySet()) {
                result.put(entry.getKey(), ShardIdWithVersion.keepShardIds(entry.getValue()));
            }
            return result;
        }

        public long newestShard() {
            long newest = -1;
            for (final List<ShardIdWithVersion> shardset : shards.values()) {
                for (final ShardIdWithVersion shard : shardset) {
                    newest = Math.max(newest, shard.getVersion());
                }
            }
            if (newest == -1) {
                throw new IllegalArgumentException("No shards!");
            }
            return DateTimeFormat.forPattern("yyyyMMddHHmmss").parseMillis(String.valueOf(newest));
        }
    }

    private SelectExecutionInformation executeSelect(
            final String q,
            final boolean useLegacy,
            final Map<String, Set<String>> keywordAnalyzerWhitelist,
            final Map<String, Set<String>> datasetToIntFields,
            final Consumer<String> out,
            final TreeTimer timer,
            final ProgressCallback progressCallback,
            final com.indeed.squall.iql2.language.compat.Consumer<String> warn,
            final boolean skipValidation,
            final Integer groupLimit,
            final WallClock clock,
            final String username
    ) throws IOException, ImhotepOutOfMemoryException {
        timer.push(q);

        timer.push("parse query");
        final Query query = Queries.parseQuery(q, useLegacy, keywordAnalyzerWhitelist, datasetToIntFields, warn, clock);
        timer.pop();

        final HashMap<Query, Boolean> queryCached = new HashMap<>();
        final SelectExecutionInformation result = executeParsedQuery(out, timer, progressCallback, query, skipValidation, groupLimit, clock, queryCached, username);
        timer.pop();

        return result;
    }

    private SelectExecutionInformation executeParsedQuery(
            Consumer<String> out,
            final TreeTimer timer,
            final ProgressCallback progressCallback,
            Query query,
            final boolean skipValidation,
            final @Nullable Integer initialGroupLimit,
            final WallClock clock,
            final Map<Query, Boolean> queryCached,
            final String username
    ) throws IOException {

        final int[] totalBytesWritten = {0};

        query = query.transform(
                Functions.<GroupBy>identity(),
                Functions.<AggregateMetric>identity(),
                Functions.<DocMetric>identity(),
                Functions.<AggregateFilter>identity(),
                new Function<DocFilter, DocFilter>() {
                    final Map<Query, Pair<Set<Long>, Set<String>>> queryToResults = new HashMap<>();

                    @Nullable
                    @Override
                    public DocFilter apply(DocFilter input) {
                        if (input instanceof DocFilter.FieldInQuery) {
                            final DocFilter.FieldInQuery fieldInQuery = (DocFilter.FieldInQuery) input;
                            final Query q = fieldInQuery.query;
                            if (!queryToResults.containsKey(q)) {
                                final Set<Long> terms = new LongOpenHashSet();
                                final Set<String> stringTerms = new HashSet<>();
                                timer.push("Execute sub-query: \"" + q + "\"");
                                try {
                                    final SelectExecutionInformation execInfo = executeParsedQuery(new Consumer<String>() {
                                        @Override
                                        public void accept(String s) {
                                            if (subQueryTermLimit > 0 && terms.size() + stringTerms.size() >= subQueryTermLimit) {
                                                throw new IllegalStateException("Sub query cannot have more than [" + subQueryTermLimit + "] terms!");
                                            }
                                            final String term = s.split("\t")[0];
                                            try {
                                                terms.add(Long.parseLong(term));
                                            } catch (NumberFormatException e) {
                                                stringTerms.add(term);
                                            }
                                        }
                                    }, timer, new SessionCollectingProgressCallback(new NoOpProgressCallback()), q, skipValidation, initialGroupLimit, clock, queryCached, username);
                                    totalBytesWritten[0] += execInfo.imhotepTempBytesWritten;
                                } catch (IOException e) {
                                    throw Throwables.propagate(e);
                                }
                                timer.pop();
                                queryToResults.put(q, Pair.of(terms, stringTerms));
                            }
                            final Pair<Set<Long>, Set<String>> p = queryToResults.get(q);
                            final ScopedField scopedField = fieldInQuery.field;

                            final List<DocFilter> filters = new ArrayList<>();
                            if (!p.getFirst().isEmpty()) {
                                filters.add(new DocFilter.IntFieldIn(scopedField.field, p.getFirst()));
                            }
                            if (!p.getSecond().isEmpty()) {
                                filters.add(new DocFilter.StringFieldIn(getKeywordAnalyzerWhitelist(), scopedField.field, p.getSecond()));
                            }
                            return scopedField.wrap(new DocFilter.Ors(filters));
                        }
                        return input;
                    }
                }
        );

        timer.push("compute commands");
        final List<Command> commands = Queries.queryCommands(query);
        timer.pop();

        timer.push("validate commands");
        final List<String> errors = new ArrayList<>();

        // Urgh different Consumer types...
        // TODO: Where in common/ to put this?
        final com.indeed.squall.iql2.language.compat.Consumer<String> errorConsumer = new com.indeed.squall.iql2.language.compat.Consumer<String>() {
            @Override
            public void accept(String s) {
                errors.add(s);
            }
        };

        final DatasetsFields datasetsFields = addAliasedFields(query.datasets, getDatasetsFields(query.datasets, query.nameToIndex(), imhotepClient, getDimensions(), getDatasetToIntFields()));
        if (!skipValidation) {
            for (final Command command : commands) {
                command.validate(datasetsFields, errorConsumer);
            }
        }

        if (errors.size() > 0) {
            throw new IllegalArgumentException("Errors found when validating query: " + errors);
        }

        timer.pop();

        timer.push("compute dataset normalization");
        final Set<String> datasets = Session.getDatasets(imhotepClient);
        final Map<String, String> upperCaseToActualDataset = Maps.newHashMapWithExpectedSize(datasets.size());
        for (final String dataset : datasets) {
            upperCaseToActualDataset.put(dataset.toUpperCase(), dataset);
        }
        timer.pop();

        timer.push("compute hash");
        final Set<Pair<String, String>> shards = Sets.newHashSet();
        final Map<String, List<ShardIdWithVersion>> datasetToChosenShards = Maps.newHashMap();
        for (final Dataset dataset : query.datasets) {
            timer.push("get chosen shards");
            final String actualDataset = upperCaseToActualDataset.get(dataset.dataset);
            final String sessionName = dataset.alias.or(dataset.dataset);
            final List<ShardIdWithVersion> chosenShards = imhotepClient.sessionBuilder(actualDataset, dataset.startInclusive, dataset.endExclusive).getChosenShards();
            timer.pop();
            for (final ShardIdWithVersion chosenShard : chosenShards) {
                // This needs to be associated with the session name, not just the actualDataset.
                shards.add(Pair.of(sessionName, chosenShard.getShardId() + "-" + chosenShard.getVersion()));
            }
            final List<ShardIdWithVersion> oldShards = datasetToChosenShards.put(sessionName, chosenShards);
            if (oldShards != null) {
                throw new IllegalArgumentException("Overwrote shard list for " + sessionName);
            }
        }
        final String queryHash = computeQueryHash(commands, shards, 7);
        final String cacheFileName = "IQL2-" + queryHash + ".tsv";
        timer.pop();

        final AtomicBoolean errorOccurred = new AtomicBoolean(false);

        try (final Closer closer = Closer.create()) {
            if (queryCache.isEnabled()) {
                timer.push("cache check");
                final boolean isCached = queryCache.isFileCached(cacheFileName);
                timer.pop();

                queryCached.put(query, isCached);

                if (isCached) {
                    timer.push("read cache");
                    // TODO: Don't have this hack
                    progressCallback.startCommand(null, true);
                    sendCachedQuery(cacheFileName, out, query.rowLimit);
                    timer.pop();
                    return new SelectExecutionInformation(datasetToChosenShards, queryCached, 0);
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
                            if (!errorOccurred.get()) {
                                queryCache.writeFromFile(cacheFileName, cacheFile);
                            }
                            if (!cacheFile.delete()) {
                                log.warn("Failed to delete  " + cacheFile);
                            }
                        }
                    });
                    out = new Consumer<String>() {
                        @Override
                        public void accept(String s) {
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

            if (query.rowLimit.isPresent()) {
                final int rowLimit = query.rowLimit.get();
                final Consumer<String> oldOut = out;
                out = new Consumer<String>() {
                    int rowsWritten = 0;

                    @Override
                    public void accept(String s) {
                        if (rowsWritten < rowLimit) {
                            oldOut.accept(s);
                            rowsWritten += 1;
                        }
                    }
                };
            }

            final ObjectMapper objectMapper = new ObjectMapper();
            if (log.isDebugEnabled()) {
                log.debug("commands = " + commands);
                for (final Command command : commands) {
                    log.debug("command = " + command);
                    final String s = objectMapper.writeValueAsString(command);
                    log.debug("s = " + s);
                }
                final String commandList = objectMapper.writeValueAsString(commands);
                log.debug("commandList = " + commandList);
            }

            final Map<String, Object> request = new HashMap<>();
            request.put("datasets", Queries.createDatasetMap(query));
            request.put("commands", commands);

            final Integer groupLimit;
            if (initialGroupLimit == null) {
                groupLimit = 1000000;
            } else {
                groupLimit = initialGroupLimit;
            }
            request.put("groupLimit", groupLimit);

            final JsonNode requestJson = OBJECT_MAPPER.valueToTree(request);

            try {
                final Session.CreateSessionResult createResult = Session.createSession(imhotepClient, datasetToChosenShards, requestJson, closer, out, getDimensions(), timer, progressCallback, imhotepLocalTempFileSizeLimit, imhotepDaemonTempFileSizeLimit, clock, username);
                return new SelectExecutionInformation(datasetToChosenShards, queryCached, createResult.tempFileBytesWritten);
            } catch (Exception e) {
                errorOccurred.set(true);
                throw Throwables.propagate(e);
            }
        }
    }

    private static DatasetsFields addAliasedFields(List<Dataset> datasets, DatasetsFields datasetsFields) {
        final Map<String, Dataset> aliasToDataset = Maps.newHashMap();
        for (final Dataset dataset : datasets) {
            aliasToDataset.put(dataset.alias.or(dataset.dataset), dataset);
        }

        final DatasetsFields.Builder builder = DatasetsFields.builderFrom(datasetsFields);
        for (final String dataset : datasetsFields.datasets()) {
            final ImmutableSet<String> intFields = datasetsFields.getIntFields(dataset);
            final ImmutableSet<String> stringFields = datasetsFields.getStringFields(dataset);
            final Map<String, String> aliasToActual = aliasToDataset.get(dataset).fieldAliases;
            for (final Map.Entry<String, String> entry : aliasToActual.entrySet()) {
                if (intFields.contains(entry.getValue().toUpperCase())) {
                    builder.addIntField(dataset, entry.getKey().toUpperCase());
                } else if (stringFields.contains(entry.getValue().toUpperCase())) {
                    builder.addStringField(dataset, entry.getKey().toUpperCase());
                } else {
                    throw new IllegalArgumentException("Alias for non-existent field: " + entry.getValue() + " in dataset " + dataset);
                }
            }
        }

        return builder.build();
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

    private static String computeQueryHash(List<Command> commands, Set<Pair<String, String>> shards, int version) {
        final MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            log.error("Failed to init SHA1", e);
            throw Throwables.propagate(e);
        }
        sha1.update(Ints.toByteArray(version));
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
                          String remoteAddr,
                          String statementType) {
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

        logEntry.setProperty("statement", statementType);

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
