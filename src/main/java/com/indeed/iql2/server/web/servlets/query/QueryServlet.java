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

package com.indeed.iql2.server.web.servlets.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.exceptions.ImhotepErrorResolver;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import com.indeed.iql.web.QueryInfo;
import com.indeed.iql1.iql.cache.QueryCache;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.web.AccessControl;
import com.indeed.iql.web.ClientInfo;
import com.indeed.iql.web.ErrorResult;
import com.indeed.iql.web.FieldFrequencyCache;
import com.indeed.iql.web.GlobalUncaughtExceptionHandler;
import com.indeed.iql.web.Limits;
import com.indeed.iql.web.QueryLogEntry;
import com.indeed.iql.web.QueryMetrics;
import com.indeed.iql.web.RunningQueriesManager;
import com.indeed.iql.web.TopTermsCache;
import com.indeed.iql2.language.DatasetDescriptor;
import com.indeed.iql.metadata.DatasetMetadata;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.iql.web.UsernameUtil;
import com.indeed.iql.web.ServletUtil;
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
//
//    static {
//        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
//        TimeZone.setDefault(TimeZone.getTimeZone("GMT-6"));
//        GlobalUncaughtExceptionHandler.register();
//        OBJECT_MAPPER.configure(SerializationFeature.INDENT_OUTPUT, true);
//        try {
//            hostname = java.net.InetAddress.getLocalHost().getHostName();
//        } catch (java.net.UnknownHostException ex) {
//            hostname = "(unknown)";
//        }
//    }

    private final ImhotepClient imhotepClient;
    private final QueryCache queryCache;
    private final RunningQueriesManager runningQueriesManager;
    private final ImhotepMetadataCache metadataCache;
    private final AccessControl accessControl;
    private final TopTermsCache topTermsCache;
    private final MetricStatsEmitter metricStatsEmitter;
    private final WallClock clock;
    private final FieldFrequencyCache fieldFrequencyCache;


    private static final Pattern DESCRIBE_DATASET_PATTERN = Pattern.compile("((DESC)|(DESCRIBE)) ([a-zA-Z0-9_]+)", Pattern.CASE_INSENSITIVE);
    private static final Pattern DESCRIBE_DATASET_FIELD_PATTERN = Pattern.compile("((DESC)|(DESCRIBE)) ([a-zA-Z0-9_]+).([a-zA-Z0-9_]+)", Pattern.CASE_INSENSITIVE);

//    @Autowired
    public QueryServlet(
            final ImhotepClient imhotepClient,
            final QueryCache queryCache,
            final RunningQueriesManager runningQueriesManager,
            final ImhotepMetadataCache metadataCacheIQL2,
            final AccessControl accessControl,
            final TopTermsCache topTermsCache,
            final MetricStatsEmitter metricStatsEmitter,
            final WallClock clock,
            final FieldFrequencyCache fieldFrequencyCache) {
        this.imhotepClient = imhotepClient;
        this.queryCache = queryCache;
        this.runningQueriesManager = runningQueriesManager;
        this.metadataCache = metadataCacheIQL2;
        this.accessControl = accessControl;
        this.topTermsCache = topTermsCache;
        this.metricStatsEmitter = metricStatsEmitter;
        this.clock = clock;
        this.fieldFrequencyCache = fieldFrequencyCache;
    }

//    @RequestMapping("query")
    public void query(
            final HttpServletRequest req,
            final HttpServletResponse resp,
            final @Nonnull @RequestParam("q") String query
    ) throws IOException {
        final WallClock clock = new StoppedClock(this.clock.currentTimeMillis());

        final int version = ServletUtil.getIQLVersionBasedOnParam(req);
        final String contentType = Optional.ofNullable(req.getHeader("Accept")).orElse("text/plain;charset=utf-8");

        final String httpUsername = UsernameUtil.getUserNameFromRequest(req);
        final String username = Strings.nullToEmpty(Strings.isNullOrEmpty(httpUsername) ? req.getParameter("username") : httpUsername);
        final String author = Strings.nullToEmpty(req.getParameter("author"));
        final String client = Strings.nullToEmpty(req.getParameter("client"));
        final String clientProcessId = Strings.nullToEmpty(req.getParameter("clientProcessId"));
        final String clientProcessName = Strings.nullToEmpty(req.getParameter("clientProcessName"));
        final String clientExecutionId = Strings.nullToEmpty(req.getParameter("clientExecutionId"));
        final ClientInfo clientInfo = new ClientInfo(username, author, client, clientProcessId, clientProcessName,
                clientExecutionId, accessControl.isMultiuserClient(client));
        final TreeTimer timer = new TreeTimer() {
            @Override
            public void push(String s) {
                super.push(s);
                log.info(s);
            }
        };

        Throwable errorOccurred = null;

        long querySubmitTimestamp = System.currentTimeMillis();

        final QueryInfo queryInfo = new QueryInfo(query, version);

        final boolean isStream = contentType.contains("text/event-stream");
        queryInfo.statementType = "invalid";
        try {
            if (Strings.isNullOrEmpty(req.getParameter("client")) && Strings.isNullOrEmpty(username)) {
                throw new IqlKnownException.IdentificationRequiredException("IQL query requests have to include parameters 'client' and 'username' for identification");
            }
            accessControl.checkAllowedAccess(username);

            final Matcher describeDatasetMatcher = DESCRIBE_DATASET_PATTERN.matcher(query);
            final Matcher describeDatasetFieldMatcher = DESCRIBE_DATASET_FIELD_PATTERN.matcher(query);

            // handling SELECT

                final boolean skipValidation = "1".equals(req.getParameter("skipValidation"));

                if (isStream) {
                    resp.setHeader("Content-Type", "text/event-stream;charset=utf-8");
                } else {
                    resp.setHeader("Content-Type", "text/plain;charset=utf-8");
                }

                final Limits limits = accessControl.getLimitsForIdentity(username, client);

                final SelectQueryExecution selectQueryExecution = new SelectQueryExecution(
                        queryCache, limits, imhotepClient,
                        metadataCache.get(), resp.getWriter(), queryInfo, clientInfo, timer, query, version, isStream, skipValidation, clock);
                selectQueryExecution.processSelect(runningQueriesManager);
                querySubmitTimestamp = selectQueryExecution.queryStartTimestamp;
                fieldFrequencyCache.acceptDatasetFields(queryInfo.datasetFields, clientInfo);
        } catch (Throwable e) {
            if (e instanceof Exception) {
                e = ImhotepErrorResolver.resolve((Exception) e);
            }
            final boolean isJson = false;
            final boolean status500 = true;
            handleError(resp, isJson, e, status500, isStream);
            log.info("Exception during query handling", e);
            errorOccurred = e;
        } finally {
            try {
                String remoteAddr = getForwardedForIPAddress(req);
                if (remoteAddr == null) {
                    remoteAddr = req.getRemoteAddr();
                }
                com.indeed.iql.web.QueryServlet.logQuery(queryInfo, clientInfo, req, querySubmitTimestamp, errorOccurred, remoteAddr, metricStatsEmitter);
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

    private void logQueryToLog4J(String query, String identification, long timeTaken) {
        final String timeTakenStr = timeTaken >= 0 ? String.valueOf(timeTaken) : "";
        log.info((timeTaken < 0 ? "+" : "-") + identification + "\t" + timeTakenStr + "\t" + query);
    }

}
