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
 package com.indeed.iql.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.exceptions.ImhotepErrorResolver;
import com.indeed.imhotep.exceptions.ImhotepKnownException;
import com.indeed.imhotep.exceptions.QueryCancelledException;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.iql.Constants;
import com.indeed.iql.cache.QueryCache;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.language.DescribeStatement;
import com.indeed.iql.language.ExplainStatement;
import com.indeed.iql.language.IQLStatement;
import com.indeed.iql.language.SelectStatement;
import com.indeed.iql.language.ShowStatement;
import com.indeed.iql.language.StatementParser;
import com.indeed.iql.metadata.DatasetMetadata;
import com.indeed.iql.metadata.FieldMetadata;
import com.indeed.iql.metadata.FieldType;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import com.indeed.iql.web.config.IQLEnv;
import com.indeed.iql2.IQL2Options;
import com.indeed.iql2.execution.QueryOptions;
import com.indeed.iql2.server.web.servlets.query.ExplainQueryExecution;
import com.indeed.iql2.server.web.servlets.query.SelectQueryExecution;
import com.indeed.iql2.sqltoiql.AntlrParserGenerator;
import com.indeed.iql2.sqltoiql.SQLToIQLParser;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.core.time.WallClock;
import com.indeed.util.logging.TracingTreeTimer;
import io.opentracing.ActiveSpan;
import io.opentracing.NoopActiveSpanSource;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
* @author dwahler
*/
@Controller
public class QueryServlet {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
        // TODO: Can we remove this?
        DateTimeZone.setDefault(Constants.DEFAULT_IQL_TIME_ZONE);
        TimeZone.setDefault(TimeZone.getTimeZone("GMT-6"));
        GlobalUncaughtExceptionHandler.register();
        OBJECT_MAPPER.configure(SerializationFeature.INDENT_OUTPUT, true);
        try {
            hostname = java.net.InetAddress.getLocalHost().getHostName();
        }
        catch (java.net.UnknownHostException ex) {
            hostname = "(unknown)";
        }
    }

    private static final Logger log = Logger.getLogger(QueryServlet.class);
    private static final Logger dataLog = Logger.getLogger("indeed.logentry");
    private static String hostname;

    private static final Set<String> USED_PARAMS = Sets.newHashSet("view", "csv", "json", "interactive",
            "nocache", "head", "progress", "totals", "nocacheread", "nocachewrite", "sql", "skipValidation",
            "getversion");

    @Nullable
    private final File tmpDir;
    private final ImhotepClient imhotepClient;
    private final ImhotepMetadataCache metadataCache;
    private final TopTermsCache topTermsCache;
    private final QueryCache queryCache;
    private final RunningQueriesManager runningQueriesManager;
    private final ExecutorService cacheUploadExecutorService;
    private final AccessControl accessControl;
    @Nullable
    private final Long maxCachedQuerySizeLimitBytes;
    private final MetricStatsEmitter metricStatsEmitter;
    private final FieldFrequencyCache fieldFrequencyCache;
    private final WallClock clock;
    private final IQL2Options defaultIQL2Options;
    private final IQLEnv iqlEnv;
    private final SQLToIQLParser sqlToIQLParser;

    @Autowired
    public QueryServlet(@Nullable final File tmpDir,
                        final ImhotepClient imhotepClient,
                        final ImhotepMetadataCache metadataCache,
                        final TopTermsCache topTermsCache,
                        final QueryCache queryCache,
                        final RunningQueriesManager runningQueriesManager,
                        final ExecutorService cacheUploadExecutorService,
                        final AccessControl accessControl,
                        @Nullable final Long maxCachedQuerySizeLimitBytes,
                        final MetricStatsEmitter metricStatsEmitter,
                        final FieldFrequencyCache fieldFrequencyCache,
                        final WallClock clock,
                        final IQL2Options defaultIQL2Options,
                        final IQLEnv iqlEnv) {
        this.tmpDir = tmpDir;
        this.imhotepClient = imhotepClient;
        this.metadataCache = metadataCache;
        this.topTermsCache = topTermsCache;
        this.queryCache = queryCache;
        this.runningQueriesManager = runningQueriesManager;
        this.cacheUploadExecutorService = cacheUploadExecutorService;
        this.accessControl = accessControl;
        this.maxCachedQuerySizeLimitBytes = maxCachedQuerySizeLimitBytes;
        this.metricStatsEmitter = metricStatsEmitter;
        this.fieldFrequencyCache = fieldFrequencyCache;
        this.clock = clock;
        this.defaultIQL2Options = defaultIQL2Options;
        this.iqlEnv = iqlEnv;
        this.sqlToIQLParser = new SQLToIQLParser(new AntlrParserGenerator());
    }

    @RequestMapping("/query")
    public void query(final HttpServletRequest req, final HttpServletResponse resp,
                         @Nonnull @RequestParam("q") String query) throws IOException {
        final Tracer tracer = GlobalTracer.get();
        ActiveSpan activeSpan = NoopActiveSpanSource.NoopActiveSpan.INSTANCE;
        try {
            resp.setCharacterEncoding("UTF-8");

            final WallClock clock = new StoppedClock(this.clock.currentTimeMillis());
            final String contentType = Optional.ofNullable(req.getHeader("Accept")).orElse("text/plain;charset=utf-8");

            final String httpUserName = UsernameUtil.getUserNameFromRequest(req);
            final String userName = Strings.nullToEmpty(Strings.isNullOrEmpty(httpUserName) ? req.getParameter("username") : httpUserName);
            final String author = Strings.nullToEmpty(req.getParameter("author"));
            final String client = Strings.nullToEmpty(req.getParameter("client"));
            final String clientProcessId = Strings.nullToEmpty(req.getParameter("clientProcessId"));
            final String clientProcessName = Strings.nullToEmpty(req.getParameter("clientProcessName"));
            final String clientExecutionId = Strings.nullToEmpty(req.getParameter("clientExecutionId"));
            final ClientInfo clientInfo = new ClientInfo(userName, author, client, clientProcessId, clientProcessName,
                    clientExecutionId, accessControl.isMultiuserClient(client));
            final QueryRequestParams queryRequestParams = new QueryRequestParams(req, clientInfo.username, clientInfo.client, contentType);
            Throwable errorOccurred = null;

            String sqlQuery = null;
            if (queryRequestParams.sql) {
                sqlQuery = query;
                // Convert SQL to IQL2 and pretend the query was IQL2 from the start
                query = sqlToIQLParser.parse(query);
            }

            final QueryInfo queryInfo = new QueryInfo(hostname, query, queryRequestParams.version, System.currentTimeMillis(), sqlQuery);

            try {
                if (Strings.isNullOrEmpty(client) && Strings.isNullOrEmpty(userName)) {
                    throw new IqlKnownException.IdentificationRequiredException("IQL query requests have to include parameters 'client' and 'username' for identification. " +
                            "'client' is the name (e.g. class name) of the tool sending the request. " +
                            "'username' is the LDAP name of the user that requested the query to be performed " +
                            "or in case of automated tools the Google group of the team responsible for the tool.");
                }
                accessControl.checkAllowedIQLAccess(userName, client);

                final IQLStatement iqlStatement = StatementParser.parseIQLToStatement(query);
                queryInfo.statementType = iqlStatement.getStatementType();

                if (iqlStatement instanceof SelectStatement) {
                    closeActiveSpans(tracer);
                    activeSpan = tracer
                            .buildSpan("/query")
                            .withTag("q", QueryInfo.truncateQuery(query))
                            .withTag("iqlversion", queryRequestParams.version)
                            .withTag("host", hostname)
                            .withTag("username", userName)
                            .withTag("client", client)
                            .withTag("env", iqlEnv.id)
                            .startActive();
                    handleSelectStatement((SelectStatement) iqlStatement, queryInfo, clientInfo, queryRequestParams, resp);
                    if (queryInfo.cached != null) {
                        activeSpan.setTag("cached", queryInfo.cached);
                    }
                    if (queryInfo.queryId != null) {
                        activeSpan.setTag("queryid", queryInfo.queryId);
                    }
                } else if (iqlStatement instanceof DescribeStatement) {
                    handleDescribeStatement((DescribeStatement) iqlStatement, queryRequestParams, resp, queryInfo);
                } else if (iqlStatement instanceof ShowStatement) {
                    handleShowStatement(queryRequestParams, resp);
                } else if (iqlStatement instanceof ExplainStatement) {
                    handleExplainStatement((ExplainStatement) iqlStatement, queryRequestParams, clientInfo, resp, clock);
                } else {
                    throw new IqlKnownException.ParseErrorException("Query parsing failed: unknown statement type");
                }
            } catch (Throwable e) {
                if (e instanceof Exception) {
                    e = ImhotepErrorResolver.resolve((Exception) e);
                }
                try {
                    handleError(resp, queryRequestParams.json, e, true, queryRequestParams.isEventStream);
                } catch (Throwable e2) {
                    log.error("Query error couldn't be returned", e);
                    log.error("Error while handling error", e2);
                }
                errorOccurred = e;
            } finally {
                try {
                    String remoteAddr = getForwardedForIPAddress(req);
                    if (remoteAddr == null) {
                        remoteAddr = req.getRemoteAddr();
                    }
                    logQuery(queryInfo, clientInfo, req, errorOccurred, remoteAddr, metricStatsEmitter);
                } catch (Throwable ignored) {
                }
            }
        } finally {
            activeSpan.close();
        }
    }

    private void closeActiveSpans(final Tracer tracer) {
        while (true) {
            final ActiveSpan activeSpan = tracer.activeSpan();
            if (activeSpan == null) {
                return;
            } else {
                log.warn("Closing active span before starting new query: " + activeSpan);
                activeSpan.close();
            }
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

    private void handleSelectStatement(SelectStatement selectStatement, QueryInfo queryInfo, ClientInfo clientInfo,
                                       QueryRequestParams queryRequestParams, HttpServletResponse resp) throws IOException {
        try (final TracingTreeTimer timer = new TracingTreeTimer()) {

            final String query = selectStatement.selectQuery;
            setContentType(resp, queryRequestParams.avoidFileSave, queryRequestParams.csv, queryRequestParams.isEventStream);
            final Limits limits = accessControl.getLimitsForIdentity(clientInfo.username, clientInfo.client);
            queryInfo.priority = (long) limits.priority;
            final String queryInitiator = (Strings.isNullOrEmpty(clientInfo.username) ? queryRequestParams.remoteAddr : clientInfo.username);
            logQueryToLog4J(queryInfo.queryStringTruncatedForPrint, queryInitiator, -1);

            final QueryMetadata queryMetadata = new QueryMetadata(resp);

            queryInfo.engine = (queryRequestParams.version == 2) ? "iql2" : "iql2legacy";

            final Set<String> iql2Options = Sets.newHashSet(defaultIQL2Options.getOptions());
            if (queryRequestParams.cacheReadDisabled && queryRequestParams.cacheWriteDisabled) {
                iql2Options.add(QueryOptions.NO_CACHE);
            }

            final SelectQueryExecution selectQueryExecution = new SelectQueryExecution(
                    tmpDir, queryCache, limits, maxCachedQuerySizeLimitBytes, imhotepClient,
                    metadataCache.get(), resp.getWriter(), queryInfo, clientInfo, timer, query,

                    queryRequestParams.headOnly,
                    queryRequestParams.version, queryRequestParams.isEventStream, queryRequestParams.returnNewestShardVersion, queryRequestParams.skipValidation, queryRequestParams.getTotals,
                    queryRequestParams.csv,

                    clock, queryMetadata, cacheUploadExecutorService, ImmutableSet.copyOf(iql2Options), accessControl);
            selectQueryExecution.processSelect(runningQueriesManager);

            fieldFrequencyCache.acceptDatasetFields(queryInfo.datasetFields, clientInfo);
            log.debug(timer);
        }
    }

    public static List<String> missingShardsToWarnings(String datasetName, DateTime startTime, DateTime endTime, List<Interval> timeIntervalsMissingShards) {
        ArrayList<String> warningList = new ArrayList<>();
        final List<Interval> properTimeIntervalsMissingShards = new ArrayList<>();

        for (Interval interval: timeIntervalsMissingShards){
            if (interval.getStart().withTimeAtStartOfDay().equals(DateTime.now().withTimeAtStartOfDay())) {
                continue;
            }

            if (interval.getStartMillis() + TimeUnit.HOURS.toMillis(12) <= System.currentTimeMillis()){
                if (interval.getEndMillis() <= System.currentTimeMillis()) {
                    properTimeIntervalsMissingShards.add(interval);
                } else {
                    Interval properInterval = new Interval(interval.getStartMillis(),System.currentTimeMillis());
                    properTimeIntervalsMissingShards.add(properInterval);
                }
            }
        }

        if(!properTimeIntervalsMissingShards.isEmpty()) {
            long millisMissing = 0;
            final int countMissingIntervals = properTimeIntervalsMissingShards.size();

            for (Interval interval: properTimeIntervalsMissingShards){
                millisMissing += interval.getEndMillis()-interval.getStartMillis();
            }

            final double totalPeriod = endTime.getMillis()-startTime.getMillis();

            final double percentMissing = millisMissing/totalPeriod*100;
            final String percentAbsent = percentMissing > 1 ?
                    String.valueOf((int) percentMissing) : String.format("%.2f", percentMissing);

            final String shortenedMissingIntervalsString;
            if (countMissingIntervals>5) {
                final List<Interval> properSubList =properTimeIntervalsMissingShards.subList(0, 5);
                shortenedMissingIntervalsString = intervalListToString(properSubList) + ", " + (countMissingIntervals - 5) + " more intervals";
            } else {
                shortenedMissingIntervalsString = intervalListToString(properTimeIntervalsMissingShards);
            }
            warningList.add(percentAbsent + "% of the queried time period is missing in dataset " + datasetName + ": "
                    + shortenedMissingIntervalsString);
        }
        return warningList;
    }

    private static final DateTimeFormatter yyyymmddhh = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH").withZone(Constants.DEFAULT_IQL_TIME_ZONE);
    private static final DateTimeFormatter yyyymmdd = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(Constants.DEFAULT_IQL_TIME_ZONE);

    public static String intervalListToString(Collection<Interval> intervals) {
        if(intervals == null) {
            return "";
        }

        final StringBuilder sb = new StringBuilder();
        for(Interval interval: intervals) {
            if(sb.length() != 0) {
                sb.append(", ");
            }
            if(sb.length() > 6000) {
                sb.append("...");   // truncated to not blow past the header size limit in Tomcat
                break;
            }
            if(interval.getStart().getMillisOfDay() == 0 && interval.getEnd().getMillisOfDay() == 0) {
                //don't have to include hours
                sb.append(interval.getStart().toString(yyyymmdd)).append("/").append(interval.getEnd().toString(yyyymmdd));
            } else {
                sb.append(interval.getStart().toString(yyyymmddhh)).append("/").append(interval.getEnd().toString(yyyymmddhh));
            }
        }
        return sb.toString();
    }

    private void handleExplainStatement(ExplainStatement explainStatement, QueryRequestParams queryRequestParams, final ClientInfo clientInfo, HttpServletResponse resp, WallClock clock) throws IOException {
        if(queryRequestParams.version == 1) {
            throw new IqlKnownException.ParseErrorException("IQL 1 doesn't support EXPLAIN statements");
        }
        if (queryRequestParams.json) {
            resp.setHeader("Content-Type", "application/json");
        }
        final Limits limits = accessControl.getLimitsForIdentity(clientInfo.username, clientInfo.client);
        final ExplainQueryExecution explainQueryExecution = new ExplainQueryExecution(
                metadataCache.get(), resp.getWriter(), explainStatement.selectQuery, queryRequestParams.version, queryRequestParams.json, clock, defaultIQL2Options, limits);
        explainQueryExecution.processExplain();
    }

    private void handleDescribeStatement(DescribeStatement describeStatement, QueryRequestParams queryRequestParams, HttpServletResponse resp, QueryInfo queryInfo) throws IOException {
        queryInfo.datasets = Sets.newHashSet(describeStatement.dataset);
        accessControl.checkAllowedDatasetAccess(queryRequestParams.username, describeStatement.dataset);
        if(Strings.isNullOrEmpty(describeStatement.field)) {
            handleDescribeDataset(describeStatement, queryRequestParams, resp);
        } else {
            handleDescribeField(describeStatement, queryRequestParams, resp, queryInfo);
        }
    }

    private void handleDescribeField(DescribeStatement parsedQuery, QueryRequestParams queryRequestParams, HttpServletResponse resp, QueryInfo queryInfo) throws IOException {
        queryInfo.datasetFields = Sets.newHashSet(parsedQuery.dataset + "." + parsedQuery.field);
        final PrintWriter outputStream = resp.getWriter();
        final String dataset = parsedQuery.dataset;
        final Map<String, String> datasetAliases = metadataCache.get().getDatasetToDimensionAliasFields().getOrDefault(dataset, Collections.emptyMap());
        final String fieldName = datasetAliases.getOrDefault(parsedQuery.field, parsedQuery.field);
        final List<String> topTerms = topTermsCache.getTopTerms(dataset, fieldName);
        FieldMetadata field = metadataCache.getDataset(dataset).getField(fieldName, true);
        final boolean hadDescription;
        if(field == null) {
            field = new FieldMetadata("notfound", FieldType.String);
            field.setDescription("Field not found");
            hadDescription = false;
        } else {
            hadDescription = !Strings.isNullOrEmpty(field.getDescription());
        }
        queryInfo.fieldHadDescription = hadDescription;

        if(queryRequestParams.json) {
            resp.setContentType(MediaType.APPLICATION_JSON_UTF8_VALUE);
            final ObjectNode jsonRoot = OBJECT_MAPPER.createObjectNode();
            field.toJSON(jsonRoot);

            final ArrayNode termsArray = OBJECT_MAPPER.createArrayNode();
            jsonRoot.set("topTerms", termsArray);
            for(String term : topTerms) {
                termsArray.add(term);
            }
            OBJECT_MAPPER.writeValue(outputStream, jsonRoot);
        } else {
            for(String term : topTerms) {
                outputStream.println(term);
            }
        }
        outputStream.close(); // only close on success because on error the stack trace is printed
    }

    private void handleDescribeDataset(DescribeStatement describeStatement, QueryRequestParams queryRequestParams, HttpServletResponse resp) throws IOException {
        final PrintWriter outputStream = resp.getWriter();
        final String dataset = describeStatement.dataset;
        final DatasetMetadata datasetMetadata = metadataCache.getDataset(dataset);
        if (queryRequestParams.json) {
            resp.setContentType(MediaType.APPLICATION_JSON_UTF8_VALUE);
            final ObjectNode jsonRoot = OBJECT_MAPPER.createObjectNode();
            datasetMetadata.toJSON(jsonRoot, OBJECT_MAPPER, false);

            OBJECT_MAPPER.writeValue(outputStream, jsonRoot);
        } else {
            for (FieldMetadata field : datasetMetadata.intFields) {
                outputStream.println(field.toTSV());
            }
            for (FieldMetadata field : datasetMetadata.stringFields) {
                outputStream.println(field.toTSV());
            }
        }
        outputStream.close(); // only close on success because on error the stack trace is printed
    }

    private void handleShowStatement(QueryRequestParams queryRequestParams, final HttpServletResponse resp) throws IOException {
        final String username = queryRequestParams.username;
        final PrintWriter outputStream = resp.getWriter();
        if (queryRequestParams.json) {
            resp.setContentType(MediaType.APPLICATION_JSON_UTF8_VALUE);
            final ObjectNode jsonRoot = OBJECT_MAPPER.createObjectNode();
            final ArrayNode array = OBJECT_MAPPER.createArrayNode();
            jsonRoot.set("datasets", array);
            for (DatasetMetadata dataset : metadataCache.get().getDatasetToMetadata().values()) {
                if (!accessControl.userCanAccessDataset(username, dataset.name)) {
                    continue;
                }
                final ObjectNode datasetInfo = OBJECT_MAPPER.createObjectNode();
                dataset.toJSON(datasetInfo, OBJECT_MAPPER, true);
                array.add(datasetInfo);
            }
            OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(outputStream, jsonRoot);
        } else {
            for (DatasetMetadata dataset : metadataCache.get().getDatasetToMetadata().values()) {
                if (!accessControl.userCanAccessDataset(username, dataset.name)) {
                    continue;
                }
                outputStream.println(dataset.name);
            }
        }
        outputStream.close(); // only close on success because on error the stack trace is printed
    }

    public static void handleError(HttpServletResponse resp, boolean json, Throwable e, boolean setHTTPStatus, boolean isEventStream) throws IOException {
        // output parse/execute error
        if(!json) {
            try (final PrintWriter printWriter = resp.getWriter()) {
                if (isEventStream) {
                    resp.setContentType(MediaType.TEXT_EVENT_STREAM_VALUE);
                    final String[] stackTrace = Throwables.getStackTraceAsString(e).split("\\n");
                    printWriter.println("event: servererror");
                    for (String s : stackTrace) {
                        printWriter.println("data: " + s);
                    }
                    printWriter.println();
                } else {
                    resp.setContentType(MediaType.TEXT_PLAIN_VALUE);
                    resp.setStatus(getHTTPStatusForError(e));
                    e.printStackTrace(printWriter);
                }
            }
        } else {
            if(setHTTPStatus) {
                resp.setStatus(getHTTPStatusForError(e));
            }
            // construct a parsed error object to be JSON serialized
            final String stackTrace = Throwables.getStackTraceAsString(Throwables.getRootCause(e));
            final ErrorResult error = new ErrorResult(e.getClass().getSimpleName(), e.getMessage(), stackTrace);
            resp.setContentType(MediaType.APPLICATION_JSON_UTF8_VALUE);
            try (final PrintWriter printWriter = resp.getWriter()) {
                OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(printWriter, error);
            }
        }
    }

    private static int getHTTPStatusForError(Throwable e) {
        return 500;
        // TODO: improve return codes
//        if(isKnownError(e)) {
//            return 400;
//        } else {
//            return 500;
//        }

        // TODO: return 404 on NoShardsException when it exists
    }


    private static boolean isKnownError(final Throwable error) {
        return (error instanceof ImhotepKnownException) || (error instanceof IqlKnownException);
    }

    // Logging code below

    // trying to not cause the logentry to overflow from being larger than 2^16
    // this is the pre URL-encoded limit and encoding can make it about twice longer
    private static final int LOGGED_FIELD_LENGTH_LIMIT = 8092;

    private static void logQuery(QueryInfo queryInfo,
                                 ClientInfo clientInfo,
                                 HttpServletRequest req,
                                 Throwable errorOccurred,
                                 String remoteAddr,
                                 MetricStatsEmitter metricStatsEmitter) {
        final long timeTaken = System.currentTimeMillis() - queryInfo.queryStartTimestamp;
        final String queryWithShortenedLists = queryInfo.queryStringTruncatedForPrint;
        final boolean error = errorOccurred != null;
        final boolean cancelled = error && (errorOccurred instanceof QueryCancelledException);
        final boolean systemError = error && !isKnownError(errorOccurred);
        if (timeTaken > 5000 || systemError) {  // we've already logged the query so only log again if it took a long time to run
            logQueryToLog4J(queryWithShortenedLists, (Strings.isNullOrEmpty(clientInfo.username) ? remoteAddr : clientInfo.username), timeTaken);
            if (systemError) {
                log.info("System error during query execution", errorOccurred);
            } else {
                log.debug("User error during query execution", errorOccurred);
            }
        }

        final QueryLogEntry logEntry = new QueryLogEntry();
        logEntry.setProperty("v", 0);
        logEntry.setProperty("iqlversion",queryInfo.iqlVersion);
        setIfNotEmpty(logEntry, "engine", queryInfo.engine);
        logEntry.setProperty("username", clientInfo.username);
        logEntry.setProperty("client", clientInfo.client);
        logEntry.setProperty("raddr", Strings.nullToEmpty(remoteAddr));
        logEntry.setProperty("starttime", Long.toString(queryInfo.queryStartTimestamp));
        logEntry.setProperty("tottime", (int) timeTaken);
        setIfNotEmpty(logEntry, "author", clientInfo.author);
        setIfNotEmpty(logEntry, "clientProcessId", clientInfo.clientProcessId);
        setIfNotEmpty(logEntry, "clientProcessName", clientInfo.clientProcessName);
        setIfNotEmpty(logEntry, "clientExecutionId", clientInfo.clientExecutionId);

        logString(logEntry, "statement", queryInfo.statementType);

        logBoolean(logEntry, "cached", queryInfo.cached);
        logBoolean(logEntry, "cacheUploadSkipped", queryInfo.cacheUploadSkipped);
        logLong(logEntry, "resultBytes", queryInfo.resultBytes);
        logSet(logEntry, "dataset", queryInfo.datasets);
        logBoolean(logEntry, "fieldHadDescription", queryInfo.fieldHadDescription);
        if (queryInfo.datasetFields != null && !queryInfo.datasetFields.isEmpty()) {
            logSet(logEntry, "datasetfield", queryInfo.datasetFields);
        }
        if (queryInfo.datasetFieldsNoDescription != null && !queryInfo.datasetFieldsNoDescription.isEmpty()) {
            logSet(logEntry, "datasetFieldsNoDescription", queryInfo.datasetFieldsNoDescription);
        }
        if (queryInfo.totalDatasetRangeDays != null) {
            logInteger(logEntry, "days", queryInfo.totalDatasetRangeDays);
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
        logLong(logEntry, "p2pIoSlotsExecTimeMs", queryInfo.p2pIOSlotsExecTimeMs);
        logLong(logEntry, "p2pIoSlotsWaitTimeMs", queryInfo.p2pIOSlotsWaitTimeMs);

        logLong(logEntry, "cacheCheckMillis", queryInfo.cacheCheckMillis);
        logLong(logEntry, "lockWaitMillis", queryInfo.lockWaitMillis);
        logLong(logEntry, "cacheCheckMillis", queryInfo.cacheCheckMillis);
        logLong(logEntry, "sendToClientMillis", queryInfo.sendToClientMillis);
        logLong(logEntry, "shardsSelectionMillis", queryInfo.shardsSelectionMillis);
        logLong(logEntry, "createSessionMillis", queryInfo.createSessionMillis);
        logLong(logEntry, "timeFilterMillis", queryInfo.timeFilterMillis);
        logLong(logEntry, "conditionFilterMillis", queryInfo.conditionFilterMillis);
        logLong(logEntry, "regroupMillis", queryInfo.regroupMillis);
        logLong(logEntry, "ftgsMillis", queryInfo.ftgsMillis);
        logLong(logEntry, "pushStatsMillis", queryInfo.pushStatsMillis);
        logLong(logEntry, "getStatsMillis", queryInfo.getStatsMillis);
        logLong(logEntry, "imhotepFilesDownloadedMB", queryInfo.imhotepFilesDownloadedMB);
        logLong(logEntry, "imhotepP2PFilesDownloadedMB", queryInfo.imhotepP2PFilesDownloadedMB);

        logSet(logEntry, "hash", queryInfo.cacheHashes);
        logString(logEntry, "hostname", hostname);
        logInteger(logEntry, "maxgroups", queryInfo.maxGroups);
        logInteger(logEntry, "maxconcurrentsessions", queryInfo.maxConcurrentSessions);
        logInteger(logEntry, "rows", queryInfo.rows);
        logSet(logEntry, "sessionid", queryInfo.sessionIDs);
        logInteger(logEntry, "shards", queryInfo.numShards);
        logSet(logEntry, "imhotepServers", queryInfo.imhotepServers);
        logInteger(logEntry, "numImhotepServers", queryInfo.numImhotepServers);
        logLong(logEntry, "priority", queryInfo.priority);
        if (queryInfo.totalShardPeriodHours != null) {
            logInteger(logEntry, "shardhours", queryInfo.totalShardPeriodHours);
        }
        logLong(logEntry, "numdocs", queryInfo.numDocs);
        logInteger(logEntry, "selectcnt", queryInfo.selectCount);
        logInteger(logEntry, "groupbycnt", queryInfo.groupByCount);
        logBoolean(logEntry, "head", queryInfo.headOnly);

        final List<String> params = Lists.newArrayList();
        final Enumeration<String> paramsEnum = req.getParameterNames();
        while (paramsEnum.hasMoreElements()) {
            final String param = paramsEnum.nextElement();
            if(USED_PARAMS.contains(param)) {
                params.add(param);
            }
        }
        logEntry.setProperty("params", Joiner.on(' ').join(params));
        final String queryToLog = truncateToFieldLengthLimit(queryWithShortenedLists);
        logEntry.setProperty("q", queryToLog);
        logEntry.setProperty("qlen", queryInfo.queryLength);
        logEntry.setProperty("sql", queryInfo.sqlQuery != null ? "1" : "0");
        if(queryInfo.sqlQuery != null) {
            final String sqlQuery = truncateToFieldLengthLimit(queryInfo.sqlQuery);
            logEntry.setProperty("sqlQuery", sqlQuery);
        }
        logEntry.setProperty("error", error ? "1" : "0");
        logEntry.setProperty("cancelled", cancelled ? "1" : "0");
        logEntry.setProperty("systemerror", systemError ? "1" : "0");
        if (error) {
            logEntry.setProperty("exceptiontype", errorOccurred.getClass().getSimpleName());
            String exceptionMessage = truncateToFieldLengthLimit(errorOccurred.getMessage());
            if (exceptionMessage == null) {
                exceptionMessage = "<no msg>";
            }
            logEntry.setProperty("exceptionmsg", exceptionMessage);
        }

        final boolean openTracingEnabled = GlobalTracer.isRegistered();
        logEntry.setProperty("opentracingenabled", openTracingEnabled ? "1" : "0");

        QueryMetrics.logQueryMetrics(queryInfo.iqlVersion, queryInfo.statementType, queryInfo.cached, error, cancelled, systemError, timeTaken, metricStatsEmitter);
        dataLog.info(logEntry);
    }

    private static String truncateToFieldLengthLimit(String string) {
        return string.length() > LOGGED_FIELD_LENGTH_LIMIT ? string.substring(0, LOGGED_FIELD_LENGTH_LIMIT) : string;
    }

    private static void setIfNotEmpty(QueryLogEntry logEntry, String propName, String propValue) {
        if (propValue == null || propName == null || logEntry == null) {
            return ;
        }
        if (!Strings.isNullOrEmpty(propValue)) {
            logEntry.setProperty(propName, propValue);
        }
    }

    private static void logLong(QueryLogEntry logEntry, String field, @Nullable Long value) {
        if (value != null) {
            logEntry.setProperty(field, value);
        }
    }

    private static void logInteger(QueryLogEntry logEntry, String field, @Nullable Integer value) {
        if (value != null) {
            logEntry.setProperty(field, value);
        }
    }

    private static void logString(QueryLogEntry logEntry, String field, @Nullable String value) {
        if (value != null) {
            logEntry.setProperty(field, value);
        }
    }

    private static void logBoolean(QueryLogEntry logEntry, String field, @Nullable Boolean value) {
        if (value != null) {
            logEntry.setProperty(field, value ? 1 : 0);
        }
    }

    private static void logSet(QueryLogEntry logEntry, String field, Collection<String> values) {
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

    private static void logQueryToLog4J(String query, String identification, long timeTaken) {
        query = shortenParamsInQuery(query);
        final String timeTakenStr = timeTaken >= 0 ? String.valueOf(timeTaken) : "";
        log.info((timeTaken < 0 ? "+" : "-") + identification + "\t" + timeTakenStr + "\t" + query);
    }

    public static String shortenParamsInQuery(String query) {
        if(query.length() > 500) {
            return query.replaceAll("\\(([^\\)]{0,100}+)[^\\)]+\\)", "\\($1\\.\\.\\.\\)");
        } else {
            return query;
        }
    }

    public static void setContentType(HttpServletResponse resp, boolean avoidFileSave, boolean csv, boolean progress) {
        if(avoidFileSave) {
            resp.setContentType("text/plain;charset=utf-8");
        } else if (csv) {
            resp.setContentType("text/csv;charset=utf-8");
            resp.setHeader("Content-Disposition", "inline; filename=\"iqlresult.csv\"");
        } else if (progress) {
            resp.setContentType("text/event-stream;charset=utf-8");
            resp.setHeader("Cache-Control", "no-cache");
        } else {
            resp.setContentType("text/tab-separated-values;charset=utf-8");
            resp.setHeader("Content-Disposition", "inline; filename=\"iqlresult.tsv\"");
        }
    }

    private class QueryRequestParams {
        public final int version;
        public final boolean sql;
        public final boolean avoidFileSave;
        public final boolean csv;
        public final boolean interactive;
        public final boolean returnNewestShardVersion;
        public final boolean cacheReadDisabled;
        public final boolean cacheWriteDisabled;
        public final boolean headOnly;
        public final boolean json;
        public final boolean isEventStream;
        public final boolean getTotals;
        public final boolean skipValidation;
        public final String imhotepUserName;
        public final String username;
        public final String requestURL;
        public final String remoteAddr;

        QueryRequestParams(HttpServletRequest req, String userName, String clientName, String contentType) {
            avoidFileSave = req.getParameter("view") != null;
            csv = req.getParameter("csv") != null;
            interactive = req.getParameter("interactive") != null;
            returnNewestShardVersion = req.getParameter("getversion") != null;
            cacheReadDisabled = !queryCache.isEnabled() || req.getParameter("nocacheread") != null || req.getParameter("nocache") != null;
            cacheWriteDisabled = !queryCache.isEnabled() || req.getParameter("nocachewrite") != null || req.getParameter("nocache") != null;
            headOnly = "HEAD".equals(req.getMethod()) || req.getParameter("head") != null;
            isEventStream = req.getParameter("progress") != null || contentType.contains("text/event-stream");
            json = req.getParameter("json") != null || contentType.contains("application/json");
            getTotals = req.getParameter("totals") != null;
            skipValidation = "1".equals(req.getParameter("skipValidation"));
            username = userName;
            imhotepUserName = (Strings.isNullOrEmpty(userName) ? clientName : userName);
            requestURL = req.getRequestURL().toString();
            remoteAddr = req.getRemoteAddr();
            sql = req.getParameter("sql") != null;
            version = sql ? 2 : ServletUtil.getIQLVersionBasedOnParam(req);
        }
    }
}
