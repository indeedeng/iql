package com.indeed.squall.iql2.server.web.servlets;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.ShardIdWithVersion;
import com.indeed.squall.iql2.language.commands.Command;
import com.indeed.squall.iql2.language.query.Dataset;
import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.squall.iql2.language.query.Query;
import com.indeed.squall.iql2.server.web.ExecutionManager;
import com.indeed.squall.iql2.server.web.UsernameUtil;
import com.indeed.squall.iql2.server.web.cache.QueryCache;
import com.indeed.squall.jql.DatasetDescriptor;
import com.indeed.squall.jql.Session;
import com.indeed.squall.jql.compat.Consumer;
import com.indeed.squall.jql.dimensions.DatasetDimensions;
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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
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

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ImhotepClient imhotepClient;
    private final QueryCache queryCache;
    private final ExecutionManager executionManager;

    private static final Pattern DESCRIBE_DATASET_PATTERN = Pattern.compile("((DESC)|(desc)) ([a-zA-Z0-9_]+)");
    private static final Pattern DESCRIBE_DATASET_FIELD_PATTERN = Pattern.compile("((DESC)|(desc)) ([a-zA-Z0-9_]+).([a-zA-Z0-9_]+)");

    @Autowired
    public QueryServlet(ImhotepClient imhotepClient, QueryCache queryCache, ExecutionManager executionManager) {
        this.imhotepClient = imhotepClient;
        this.queryCache = queryCache;
        this.executionManager = executionManager;
    }

    // TODO: use a shared reloader, and have actual values
    private Map<String, Set<String>> getKeywordAnalyzerWhitelist() {
        return Collections.emptyMap();
    }

    // TODO: use a shared reloader, and have actual values
    private Map<String, Set<String>> getDatasetToIntFields() {
        return Collections.emptyMap();
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

        // TODO: Check for username and client values

        final Matcher describeDatasetMatcher = DESCRIBE_DATASET_PATTERN.matcher(query);
        final Matcher describeDatasetFieldMatcher = DESCRIBE_DATASET_FIELD_PATTERN.matcher(query);
        if (describeDatasetMatcher.matches()) {
            final String dataset = describeDatasetMatcher.group(4);
            final DatasetDescriptor datasetDescriptor = DatasetDescriptor.from(Session.getDatasetShardList(imhotepClient, dataset));
            if (contentType.contains("application/json") || contentType.contains("*/*")) {
                response.getWriter().println(OBJECT_MAPPER.writeValueAsString(datasetDescriptor));
            } else {
                throw new IllegalArgumentException("Don't know what to do with request Accept: [" + contentType + "]");
            }
        } else if (describeDatasetFieldMatcher.matches()) {
            final String dataset = describeDatasetFieldMatcher.group(4);
            final String field = describeDatasetFieldMatcher.group(5);
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
        } else if (query.trim().toLowerCase().equals("show datasets")) {
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
        } else {
            final boolean isStream = contentType.contains("text/event-stream");
            if (isStream) {
                response.setHeader("Content-Type", "text/event-stream;charset=utf-8");
            } else {
                response.setHeader("Content-Type", "text/plain;charset=utf-8");
            }
            final ExecutionManager.QueryTracker queryTracker = executionManager.queryStarted(query, username);
            try {
                queryTracker.acquireLocks(); // blocks and waits if necessary
                // TODO: Don't count time waiting?
                final PrintWriter outputStream = response.getWriter();
                if (isStream) {
                    outputStream.println(": This is the start of the IQL Query Stream");
                    outputStream.println();
                    outputStream.println("event: resultstream");
                }
                executeSelect(query, version == 1, getKeywordAnalyzerWhitelist(), getDatasetToIntFields(), new Consumer<String>() {
                    @Override
                    public void accept(String s) {
                        if (isStream) {
                            outputStream.print("data: ");
                        }
                        outputStream.println(s);
                    }
                }, timer, queryTracker);
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
        }
        System.out.println(timer);
    }

    public void executeSelect(String q, boolean useLegacy, Map<String, Set<String>> keywordAnalyzerWhitelist, Map<String, Set<String>> datasetToIntFields, Consumer<String> out, TreeTimer timer, ExecutionManager.QueryTracker queryTracker) throws IOException, ImhotepOutOfMemoryException {
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
        final String cacheFile = queryHash + ".tsv";
        timer.pop();

        try (final Closer closer = Closer.create()) {
            if (queryCache.isEnabled()) {
                timer.push("cache check");
                final boolean isCached = queryCache.isFileCached(cacheFile);
                timer.pop();

                if (isCached) {
                    timer.push("read cache");
                    sendCachedQuery(cacheFile, out);
                    timer.pop();
                    return;
                } else {
                    final Consumer<String> oldOut = out;
                    // TODO: Use queryCache.writeFromFile() instead, and call writeFromFile asynchronously
                    final BufferedWriter cacheOutputStream = closer.register(new BufferedWriter(new OutputStreamWriter(queryCache.getOutputStream(cacheFile), Charsets.UTF_8)));
                    out = new Consumer<String>() {
                        @Override
                        public void accept(String s) {
                            oldOut.accept(s);
                            try {
                                cacheOutputStream.write(s);
                                cacheOutputStream.newLine();
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

            Session.createSession(imhotepClient, requestJson, closer, out, Collections.<String, DatasetDimensions>emptyMap(), timer);
        }
        timer.pop();
    }

    private void sendCachedQuery(String cacheFile, Consumer<String> out) throws IOException {
        try (final BufferedReader stream = new BufferedReader(new InputStreamReader(queryCache.getInputStream(cacheFile)))) {
            String line;
            while ((line = stream.readLine()) != null) {
                out.accept(line);
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
}
