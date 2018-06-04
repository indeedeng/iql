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

package com.indeed.squall.iql2.server.web.servlets.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.indeed.imhotep.iql.cache.QueryCache;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.imhotep.web.AccessControl;
import com.indeed.imhotep.web.ClientInfo;
import com.indeed.imhotep.web.ErrorResult;
import com.indeed.imhotep.web.GlobalUncaughtExceptionHandler;
import com.indeed.imhotep.web.Limits;
import com.indeed.imhotep.web.QueryLogEntry;
import com.indeed.imhotep.web.RunningQueriesManager;
import com.indeed.imhotep.web.TopTermsCache;
import com.indeed.util.core.Pair;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.exceptions.ImhotepErrorResolver;
import com.indeed.squall.iql2.language.DatasetDescriptor;
import com.indeed.squall.iql2.language.metadata.DatasetMetadata;
import com.indeed.squall.iql2.language.metadata.DatasetsMetadata;
import com.indeed.squall.iql2.server.web.UsernameUtil;
import com.indeed.squall.iql2.server.web.metadata.MetadataCache;
import com.indeed.squall.iql2.server.web.servlets.ServletUtil;
import com.indeed.util.core.TreeTimer;
import com.indeed.util.core.time.WallClock;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestParam;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Controller("QueryServletV2")
public class QueryServlet {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger log = Logger.getLogger(QueryServlet.class);
    private static final Logger dataLog = Logger.getLogger("indeed.logentry");
    private static String hostname;

    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
        TimeZone.setDefault(TimeZone.getTimeZone("GMT-6"));
        GlobalUncaughtExceptionHandler.register();
        OBJECT_MAPPER.configure(SerializationFeature.INDENT_OUTPUT, true);
        try {
            hostname = java.net.InetAddress.getLocalHost().getHostName();
        } catch (java.net.UnknownHostException ex) {
            hostname = "(unknown)";
        }
    }

    private final ImhotepClient imhotepClient;
    private final QueryCache queryCache;
    private final RunningQueriesManager runningQueriesManager;
    private final MetadataCache metadataCache;
    private final AccessControl accessControl;
    private final TopTermsCache topTermsCache;
    private final MetricStatsEmitter metricStatsEmitter;
    private final WallClock clock;

    private static final Pattern DESCRIBE_DATASET_PATTERN = Pattern.compile("((DESC)|(DESCRIBE)) ([a-zA-Z0-9_]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern DESCRIBE_DATASET_FIELD_PATTERN = Pattern.compile("((DESC)|(DESCRIBE)) ([a-zA-Z0-9_]+).([a-zA-Z0-9_]+)", Pattern.CASE_INSENSITIVE);

    @Autowired
    public QueryServlet(
            final ImhotepClient imhotepClient,
            final QueryCache queryCache,
            final RunningQueriesManager runningQueriesManager,
            final MetadataCache metadataCache,
            final AccessControl accessControl,
            final TopTermsCache topTermsCache,
            final MetricStatsEmitter metricStatsEmitter,
            final WallClock clock
    ) {
        this.imhotepClient = imhotepClient;
        this.queryCache = queryCache;
        this.runningQueriesManager = runningQueriesManager;
        this.metadataCache = metadataCache;
        this.accessControl = accessControl;
        this.topTermsCache = topTermsCache;
        this.metricStatsEmitter = metricStatsEmitter;
        this.clock = clock;
    }

    public static Map<String, Set<String>> upperCaseMapToSet(Map<String, ? extends Set<String>> map) {
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

//    @RequestMapping("query")
    public void query(
            final HttpServletRequest request,
            final HttpServletResponse response,
            final @Nonnull @RequestParam("q") String query
    ) throws ServletException, IOException, ImhotepOutOfMemoryException, TimeoutException {
        final WallClock clock = new StoppedClock(this.clock.currentTimeMillis());

        final int version = ServletUtil.getIQLVersionBasedOnParam(request);
        final String contentType = Optional.ofNullable(request.getHeader("Accept")).orElse("text/plain;charset=utf-8");

        final String httpUsername = UsernameUtil.getUserNameFromRequest(request);
        final String username = Strings.nullToEmpty(Strings.isNullOrEmpty(httpUsername) ? request.getParameter("username") : httpUsername);
        final String author = Strings.nullToEmpty(request.getParameter("author"));
        final String client = Strings.nullToEmpty(request.getParameter("client"));
        final String clientProcessId = Strings.nullToEmpty(request.getParameter("clientProcessId"));
        final String clientProcessName = Strings.nullToEmpty(request.getParameter("clientProcessName"));
        final String clientExecutionId = Strings.nullToEmpty(request.getParameter("clientExecutionId"));
        final ClientInfo clientInfo = new ClientInfo(username, author, client, clientProcessId, clientProcessName,
                clientExecutionId, accessControl.isMultiuserClient(client));
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

        final SelectQueryExecution.QueryInfo queryInfo = new SelectQueryExecution.QueryInfo();

        final boolean isStream = contentType.contains("text/event-stream");
        queryInfo.statementType = "invalid";
        try {
            if (Strings.isNullOrEmpty(request.getParameter("client")) && Strings.isNullOrEmpty(username)) {
                throw new RuntimeException("IQL query requests have to include parameters 'client' and 'username' for identification");
            }
            accessControl.checkAllowedAccess(username);

            final Matcher describeDatasetMatcher = DESCRIBE_DATASET_PATTERN.matcher(query);
            final Matcher describeDatasetFieldMatcher = DESCRIBE_DATASET_FIELD_PATTERN.matcher(query);
            if (describeDatasetMatcher.matches()) {
                final String dataset = describeDatasetMatcher.group(4);
                processDescribeDataset(response, contentType, dataset);
                queryInfo.statementType = "describe";
            } else if (describeDatasetFieldMatcher.matches()) {
                final String dataset = describeDatasetFieldMatcher.group(4);
                final String field = describeDatasetFieldMatcher.group(5);
                processDescribeField(response, contentType, dataset, field);
                queryInfo.statementType = "describe";
            } else if (query.trim().toLowerCase().equals("show datasets")) {
                processShowDatasets(response, contentType);
                queryInfo.statementType = "show";
            } else if (query.trim().toLowerCase().startsWith("explain ")) {
                queryInfo.statementType = "explain";

                final boolean isJSON = contentType.contains("application/json");
                if (isJSON) {
                    response.setHeader("Content-Type", "application/json");
                }
                final String selectQuery = query.trim().substring("explain ".length());
                final ExplainQueryExecution explainQueryExecution = new ExplainQueryExecution(
                        metadataCache.get(), response.getWriter(), selectQuery, version, isJSON, clock);
                explainQueryExecution.processExplain();
            } else {
                final boolean skipValidation = "1".equals(request.getParameter("skipValidation"));

                if (isStream) {
                    response.setHeader("Content-Type", "text/event-stream;charset=utf-8");
                } else {
                    response.setHeader("Content-Type", "text/plain;charset=utf-8");
                }

                final Limits limits = accessControl.getLimitsForIdentity(username, client);

                final SelectQueryExecution selectQueryExecution = new SelectQueryExecution(
                        queryCache, limits, imhotepClient,
                        metadataCache.get(), response.getWriter(), queryInfo, clientInfo, timer, query, version, isStream, skipValidation, clock);
                selectQueryExecution.processSelect(runningQueriesManager);
                queryStartTimestamp = selectQueryExecution.queryStartTimestamp;
            }
        } catch (Throwable e) {
            if (e instanceof Exception) {
                e = ImhotepErrorResolver.resolve((Exception) e);
            }
            final boolean isJson = false;
            final boolean status500 = true;
            handleError(response, isJson, e, status500, isStream);
            log.info("Exception during query handling", e);
            errorOccurred = e;
        } finally {
            try {
                String remoteAddr = getForwardedForIPAddress(request);
                if (remoteAddr == null) {
                    remoteAddr = request.getRemoteAddr();
                }
                logQuery(request, query, queryStartTimestamp, errorOccurred, remoteAddr, queryInfo, clientInfo);
            } catch (Throwable ignored) {
                // Do nothing
            }
        }
        log.info(timer);
    }

    public static void handleError(HttpServletResponse response, boolean isJson, Throwable e, boolean status500, boolean isStream) throws IOException {
        if (!(e instanceof Exception || e instanceof OutOfMemoryError)) {
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
            if (status500) {
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
        final List<String> datasetNames = imhotepClient.getDatasetNames();
        final DatasetsMetadata metadata = metadataCache.get();
        final List<Map<String, String>> datasets = new ArrayList<>();
        for (final String dataset : datasetNames) {
            final String description = metadata.getMetadata(dataset).isPresent() ?
                    Strings.nullToEmpty(metadata.getMetadata(dataset).get().description) : "";
            datasets.add(ImmutableMap.of("name", dataset, "description", description));
        }
        if (contentType.contains("application/json") || contentType.contains("*/*")) {
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            response.getWriter().println(OBJECT_MAPPER.writeValueAsString(ImmutableMap.of("datasets", datasets)));
        } else {
            throw new IllegalArgumentException("Don't know what to do with request Accept: [" + contentType + "]");
        }
    }

    private void processDescribeField(HttpServletResponse response, String contentType, String dataset, String field) throws IOException {
        final DatasetInfo datasetInfo = imhotepClient.getDatasetInfo(dataset);

        final String type;
        if (datasetInfo.getIntFields().contains(field)) {
            type = "Integer";
        } else if (datasetInfo.getStringFields().contains(field)) {
            type = "String";
        } else {
            throw new IllegalArgumentException("[" + field + "] is not present in [" + dataset + "]");
        }

        if (contentType.contains("application/json") || contentType.contains("*/*")) {
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            final Map<String, Object> result = new HashMap<>();
            final DatasetsMetadata metadata = metadataCache.get();
            final com.google.common.base.Optional<DatasetMetadata> targetMetadata = metadata.getMetadata(dataset);
            final String description = !targetMetadata.isPresent() ? "" : targetMetadata.get().getFieldDescription(field);
            result.put("name", field);
            result.put("description", description);
            result.put("type", type);
            final List<String> topTerms = topTermsCache.getTopTerms(dataset, field);
            result.put("topTerms", topTerms);
            response.getWriter().println(OBJECT_MAPPER.writeValueAsString(result));
        } else {
            throw new IllegalArgumentException("Don't know what to do with request Accept: [" + contentType + "]");
        }
    }

    private void processDescribeDataset(HttpServletResponse response, String contentType, String dataset) throws IOException {
        final DatasetsMetadata metadata = metadataCache.get();
        final String content;
        if (!metadata.getMetadata(dataset).isPresent()) {
            content = "dataset " + dataset + " does not exist";
        } else {
            final DatasetDescriptor datasetDescriptor = DatasetDescriptor.from(dataset, metadata.getMetadata(dataset).get());
            content = OBJECT_MAPPER.writeValueAsString(datasetDescriptor);
        }
        if (contentType.contains("application/json") || contentType.contains("*/*")) {
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            response.getWriter().println(content);
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

    private static final int QUERY_LENGTH_LIMIT = 55000; // trying to not cause the logentry to overflow from being larger than 2^16

    private void setIfNotEmpty(QueryLogEntry logEntry, String propName, String propValue) {
        if (propValue == null || propName == null || logEntry == null) {
            return ;
        }
        if (!Strings.isNullOrEmpty(propValue)) {
            logEntry.setProperty(propName, propValue);
        }
    }

    private void logQuery(HttpServletRequest req,
                          String query,
                          long queryStartTimestamp,
                          Throwable errorOccurred,
                          String remoteAddr,
                          SelectQueryExecution.QueryInfo queryInfo,
                          ClientInfo clientInfo) {
        final long timeTaken = System.currentTimeMillis() - queryStartTimestamp;
        if (timeTaken > 5000) {  // we've already logged the query so only log again if it took a long time to run
            logQueryToLog4J(query, (Strings.isNullOrEmpty(clientInfo.username) ? remoteAddr : clientInfo.username), timeTaken);
        }

        final QueryLogEntry logEntry = new QueryLogEntry();
        logEntry.setProperty("v", 0);
        logEntry.setProperty("iqlversion", 2);
        logEntry.setProperty("username", clientInfo.username);
        logEntry.setProperty("client", clientInfo.client);
        logEntry.setProperty("raddr", Strings.nullToEmpty(remoteAddr));
        logEntry.setProperty("starttime", Long.toString(queryStartTimestamp));
        logEntry.setProperty("tottime", (int) timeTaken);
        setIfNotEmpty(logEntry, "author", clientInfo.author);
        setIfNotEmpty(logEntry, "clientProcessId", clientInfo.clientProcessId);
        setIfNotEmpty(logEntry, "clientProcessName", clientInfo.clientProcessName);
        setIfNotEmpty(logEntry, "clientExecutionId", clientInfo.clientExecutionId);

        logString(logEntry, "statement", queryInfo.statementType);

        logBoolean(logEntry, "cached", queryInfo.cached);
        logSet(logEntry, "dataset", queryInfo.datasets);
        if (queryInfo.totalDatasetRange != null) {
            logInteger(logEntry, "days", queryInfo.totalDatasetRange.toStandardDays().getDays());
        }
        logLong(logEntry, "ftgsmb", queryInfo.ftgsMB);
        logLong(logEntry, "imhotepcputimems", queryInfo.imhotepcputimems);
        logLong(logEntry, "imhoteprammb", queryInfo.imhoteprammb);
        logLong(logEntry, "imhotepftgsmb", queryInfo.imhotepftgsmb);
        logLong(logEntry, "imhotepfieldfilesmb", queryInfo.imhotepfieldfilesmb);
        logLong(logEntry, "cpuSlotsExecTimeMs", queryInfo.cpuSlotsExecTimeMs);
        logLong(logEntry, "cpuSlotsWaitTimeMs", queryInfo.cpuSlotsWaitTimeMs);
        logLong(logEntry, "ioSlotsExecTimeMs", queryInfo.ioSlotsExecTimeMs);
        logLong(logEntry, "ioSlotsWaitTimeMs", queryInfo.ioSlotsWaitTimeMs);
        logSet(logEntry, "hash", queryInfo.cacheHashes);
        logString(logEntry, "hostname", hostname);
        logInteger(logEntry, "maxgroups", queryInfo.maxGroups);
        logInteger(logEntry, "maxconcurrentsessions", queryInfo.maxConcurrentSessions);
        logInteger(logEntry, "rows", queryInfo.rows);
        logSet(logEntry, "sessionid", queryInfo.sessionIDs);
        logInteger(logEntry, "shards", queryInfo.numShards);
        if (queryInfo.totalShardPeriod != null) {
            logInteger(logEntry, "shardhours", queryInfo.totalShardPeriod.toStandardHours().getHours());
        }
        logLong(logEntry, "numdocs", queryInfo.numDocs);

        final List<String> params = Lists.newArrayList();
        final Enumeration<String> paramsEnum = req.getParameterNames();
        while (paramsEnum.hasMoreElements()) {
            final String param = paramsEnum.nextElement();
            // TODO: Add whitelist
            params.add(param);
        }
        logEntry.setProperty("params", Joiner.on(' ').join(params));
        final String queryToLog = query.length() > QUERY_LENGTH_LIMIT ? query.substring(0, QUERY_LENGTH_LIMIT) : query;
        logEntry.setProperty("q", queryToLog);
        logEntry.setProperty("qlen", query.length());
        logEntry.setProperty("error", errorOccurred != null ? "1" : "0");
        if (errorOccurred != null) {
            logEntry.setProperty("exceptiontype", errorOccurred.getClass().getSimpleName());
            String message = errorOccurred.getMessage();
            if (message == null) {
                message = "<no msg>";
            }
            logEntry.setProperty("exceptionmsg", message);
        }

        final List<Pair<String, String>> metricTags = new ArrayList<>();
        metricTags.add(new Pair<>("iqlversion", "2"));
        metricTags.add(new Pair<>("statement", queryInfo.statementType));
        metricTags.add(new Pair<>("error", errorOccurred != null ? "1" : "0"));
        this.metricStatsEmitter.histogram("query.time.ms", timeTaken, metricTags);
        dataLog.info(logEntry);
    }

    private void logLong(QueryLogEntry logEntry, String field, @Nullable Long value) {
        if (value != null) {
            logEntry.setProperty(field, value);
        }
    }

    private void logInteger(QueryLogEntry logEntry, String field, @Nullable Integer value) {
        if (value != null) {
            logEntry.setProperty(field, value);
        }
    }

    private void logString(QueryLogEntry logEntry, String field, @Nullable String value) {
        if (value != null) {
            logEntry.setProperty(field, value);
        }
    }

    private void logBoolean(QueryLogEntry logEntry, String field, @Nullable Boolean value) {
        if (value != null) {
            logEntry.setProperty(field, value ? 1 : 0);
        }
    }

    private void logSet(QueryLogEntry logEntry, String field, Collection<String> values) {
        if (values != null) {
            final StringBuilder sb = new StringBuilder();
            boolean appendedAny = false;
            for (final String value : values) {
                if (appendedAny) {
                    sb.append(',');
                }
                sb.append(value);
                appendedAny = true;
            }
            logEntry.setProperty(field, sb.toString());
        }
    }

    private void logQueryToLog4J(String query, String identification, long timeTaken) {
        if (query.length() > 500) {
            query = query.replaceAll("\\(([^\\)]{0,100}+)[^\\)]+\\)", "\\($1\\.\\.\\.\\)");
        }
        final String timeTakenStr = timeTaken >= 0 ? String.valueOf(timeTaken) : "";
        log.info((timeTaken < 0 ? "+" : "-") + identification + "\t" + timeTakenStr + "\t" + query);
    }

}
