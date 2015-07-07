package com.indeed.squall.iql2.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.squall.iql2.language.commands.Command;
import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.squall.iql2.language.query.Query;
import com.indeed.squall.iql2.language.query.SplitQuery;
import com.indeed.squall.jql.DatasetDescriptor;
import com.indeed.squall.jql.Session;
import com.indeed.squall.jql.compat.Consumer;
import com.indeed.squall.jql.dimensions.DatasetDimensions;
import com.indeed.util.core.TreeTimer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.ServletRequestUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Nonnull;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Controller
@RequestMapping("/iql")
public class Server {
    private static final Logger log = Logger.getLogger(Server.class);
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ImhotepClient imhotepClient;

    @Autowired
    public Server(ImhotepClient imhotepClient) {
        this.imhotepClient = imhotepClient;
    }

    private Map<String, Set<String>> getKeywordAnalyzerWhitelist() {
        return Collections.emptyMap();
    }

    private Map<String, Set<String>> getDatasetToIntFields() {
        return Collections.emptyMap();
    }

    @RequestMapping("/elaborateable")
    @ResponseBody
    public List<String> elaborateable() {
        return Collections.emptyList();
    }

    @RequestMapping("/parse")
    @ResponseBody
    public Object parse(
            final HttpServletRequest request,
            final HttpServletResponse response,
            final @Nonnull @RequestParam("q") String q
    ) {
        response.setHeader("Access-Control-Allow-Origin", "*");
        final int version = ServletRequestUtils.getIntParameter(request, "v", 1);
        try {
            response.setHeader("Content-Type", "application/json");
            final Query query = Queries.parseQuery(q, version == 1, getKeywordAnalyzerWhitelist(), getDatasetToIntFields());
            return ImmutableMap.of("parsed", true);
        } catch (Exception e) {
            final HashMap<String, Object> errorMap = new HashMap<>();
            errorMap.put("clause", "where");
            errorMap.put("offsetInClause", 5);
            errorMap.put("exceptionType", e.getClass().getSimpleName());
            errorMap.put("message", e.getMessage());
            errorMap.put("stackTrace", Joiner.on('\n').join(e.getStackTrace()));
            return errorMap;
        }
    }

    @RequestMapping("/split")
    @ResponseBody
    public Object split(
            final HttpServletRequest request,
            final HttpServletResponse response,
            final @Nonnull @RequestParam("q") String q
    ) {
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Content-Type", "application/json");
        final int version = ServletRequestUtils.getIntParameter(request, "v", 1);
        try {
            return Queries.parseSplitQuery(q, version == 1);
        } catch (Exception e) {
            final HashMap<String, Object> errorMap = new HashMap<>();
            errorMap.put("clause", "where");
            errorMap.put("offsetInClause", 5);
            errorMap.put("exceptionType", e.getClass().getSimpleName());
            errorMap.put("message", e.getMessage());
            errorMap.put("stackTrace", Joiner.on('\n').join(e.getStackTrace()));
            return errorMap;
        }
    }

    private static final Pattern DESCRIBE_DATASET_PATTERN = Pattern.compile("((DESC)|(desc)) ([a-zA-Z0-9_]+)");
    private static final Pattern DESCRIBE_DATASET_FIELD_PATTERN = Pattern.compile("((DESC)|(desc)) ([a-zA-Z0-9_]+).([a-zA-Z0-9_]+)");

    @RequestMapping("/query")
    public void handle(final HttpServletRequest request,
                       final HttpServletResponse response,
                       final @Nonnull @RequestParam("q") String query
    ) throws ServletException, IOException, ImhotepOutOfMemoryException {
        final int version = ServletRequestUtils.getIntParameter(request, "v", 1);
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Content-Type", "text/event-stream;charset=utf-8");

        final String contentType = request.getHeader("Accept");
        final boolean isStream = contentType.contains("text/event-stream");
        final TreeTimer timer = new TreeTimer();

        final Matcher describeDatasetMatcher = DESCRIBE_DATASET_PATTERN.matcher(query);
        final Matcher describeDatasetFieldMatcher = DESCRIBE_DATASET_FIELD_PATTERN.matcher(query);
        if (describeDatasetMatcher.matches()) {
            final String dataset = describeDatasetMatcher.group(4);
            final DatasetDescriptor datasetDescriptor = DatasetDescriptor.from(Session.getDatasetShardList(imhotepClient, dataset));
            if (contentType.contains("application/json") || contentType.contains("*/*")) {
                response.getOutputStream().println(OBJECT_MAPPER.writeValueAsString(datasetDescriptor));
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
                response.getOutputStream().println(OBJECT_MAPPER.writeValueAsString(result));
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
                response.getOutputStream().println(OBJECT_MAPPER.writeValueAsString(ImmutableMap.of("datasets", datasetWithEmptyDescriptions)));
            } else {
                throw new IllegalArgumentException("Don't know what to do with request Accept: [" + contentType + "]");
            }
        } else {
            final ServletOutputStream outputStream = response.getOutputStream();
            if (isStream) {
                outputStream.println(": This is the start of the IQL Query Stream");
                outputStream.println();
                outputStream.println("event: resultstream");
            }
            executeQuery(query, version == 1, getKeywordAnalyzerWhitelist(), getDatasetToIntFields(), new Consumer<String>() {
                @Override
                public void accept(String s) {
                    try {
                        if (isStream) {
                            outputStream.print("data: ");
                        }
                        outputStream.println(s);
                    } catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }
            }, timer);
            if (isStream) {
                outputStream.println();
                outputStream.println("event: header");
                final Map<String, Object> headerMap = new HashMap<>();
                headerMap.put("IQL-Cached", false);
                headerMap.put("IQL-Timings", timer.toString().replaceAll("\n","\t"));
                outputStream.println("data: " + OBJECT_MAPPER.writeValueAsString(headerMap));
                outputStream.println();
                outputStream.println("event: complete");
                outputStream.println("data: :)");
            }
        }
        System.out.println("timer = " + timer);
    }

    public void executeQuery(String q, boolean useLegacy, Map<String, Set<String>> keywordAnalyzerWhitelist, Map<String, Set<String>> datasetToIntFields, Consumer<String> out, TreeTimer timer) throws IOException, ImhotepOutOfMemoryException {
        final Query query = Queries.parseQuery(q, useLegacy, keywordAnalyzerWhitelist, datasetToIntFields);
        final List<Command> commands = Queries.queryCommands(query);
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

        try (final Closer closer = Closer.create()) {
            timer.push(q);
            Session.createSession(imhotepClient, requestJson, closer, out, Collections.<String, DatasetDimensions>emptyMap(), timer);
            timer.pop();
        }
    }
}
