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
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.exceptions.ImhotepErrorResolver;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.metadata.DatasetMetadata;
import com.indeed.iql.metadata.FieldMetadata;
import com.indeed.iql.metadata.FieldType;
import com.indeed.iql.metadata.ImhotepMetadataCache;
import com.indeed.iql1.iql.GroupStats;
import com.indeed.iql1.iql.IQLQuery;
import com.indeed.iql1.iql.cache.QueryCache;
import com.indeed.iql1.sql.IQLTranslator;
import com.indeed.iql1.sql.ast2.DescribeStatement;
import com.indeed.iql1.sql.ast2.FromClause;
import com.indeed.iql1.sql.ast2.GroupByClause;
import com.indeed.iql1.sql.ast2.IQLStatement;
import com.indeed.iql1.sql.ast2.SelectClause;
import com.indeed.iql1.sql.ast2.SelectStatement;
import com.indeed.iql1.sql.ast2.ShowStatement;
import com.indeed.iql1.sql.parser.StatementParser;
import com.indeed.iql1.web.QueryMetadata;
import com.indeed.util.core.TreeTimer;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.time.StoppedClock;
import com.indeed.util.core.time.WallClock;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
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
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
* @author dwahler
*/
@Controller
public class QueryServlet {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
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
    private static final String METADATA_FILE_SUFFIX = ".meta";
    private static String hostname;

    private static final Set<String> USED_PARAMS = Sets.newHashSet("view", "sync", "csv", "json", "interactive",
            "nocache", "head", "progress", "totals", "getshardlist", "nocacheread", "nocachewrite");

    private final ImhotepClient imhotepClient;
    private final ImhotepMetadataCache metadataCacheIQL1;
    private final ImhotepMetadataCache metadataCacheIQL2;
    private final TopTermsCache topTermsCache;
    private final QueryCache queryCache;
    private final RunningQueriesManager runningQueriesManager;
    private final ExecutorService executorService;
    private final AccessControl accessControl;
    private final com.indeed.iql2.server.web.servlets.query.QueryServlet queryServletV2;
    private final MetricStatsEmitter metricStatsEmitter;
    private final FieldFrequencyCache fieldFrequencyCache;
    private final WallClock clock;

    @Autowired
    public QueryServlet(final ImhotepClient imhotepClient,
                        final ImhotepMetadataCache metadataCacheIQL1,
                        final ImhotepMetadataCache metadataCacheIQL2,
                        final TopTermsCache topTermsCache,
                        final QueryCache queryCache,
                        final RunningQueriesManager runningQueriesManager,
                        final ExecutorService executorService,
                        final AccessControl accessControl,
                        final com.indeed.iql2.server.web.servlets.query.QueryServlet queryServletV2,
                        final MetricStatsEmitter metricStatsEmitter,
                        final FieldFrequencyCache fieldFrequencyCache,
                        final WallClock clock) {
        this.imhotepClient = imhotepClient;
        this.metadataCacheIQL1 = metadataCacheIQL1;
        this.metadataCacheIQL2 = metadataCacheIQL2;
        this.topTermsCache = topTermsCache;
        this.queryCache = queryCache;
        this.runningQueriesManager = runningQueriesManager;
        this.executorService = executorService;
        this.accessControl = accessControl;
        this.queryServletV2 = queryServletV2;
        this.metricStatsEmitter = metricStatsEmitter;
        this.fieldFrequencyCache = fieldFrequencyCache;
        this.clock = clock;
    }

    @RequestMapping("/query")
    protected void query(final HttpServletRequest req, final HttpServletResponse resp,
                         final @Nonnull @RequestParam("q") String query) throws IOException {

        final int version = ServletUtil.getIQLVersionBasedOnParam(req);
        boolean iql2Mode = version == 2;
        boolean iql1Mode = !iql2Mode;
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
        long querySubmitTimestamp = System.currentTimeMillis();
        final TreeTimer timer = new TreeTimer() {
            @Override
            public void push(String s) {
                super.push(s);
                log.info(s);
            }
        };

        final boolean json = req.getParameter("json") != null;
        Throwable errorOccurred = null;
        final QueryInfo queryInfo = new QueryInfo(query, version);


        // TODO remove
        if(ServletUtil.getIQLVersionBasedOnParam(req) == 2) {
            queryServletV2.query(req, resp, query);
            return;
        }

        IQLStatement parsedQuery = null;
        SelectQuery selectQuery = null;
        try {
            if(Strings.isNullOrEmpty(client) && Strings.isNullOrEmpty(userName)) {
                throw new IqlKnownException.IdentificationRequiredException("IQL query requests have to include parameters 'client' and 'username' for identification. " +
                        "'client' is the name (e.g. class name) of the tool sending the request. " +
                        "'username' is the LDAP name of the user that requested the query to be performed " +
                        "or in case of automated tools the Google group of the team responsible for the tool.");
            }
            accessControl.checkAllowedAccess(userName);

            parsedQuery = StatementParser.parse(query, metadataCacheIQL1);
            if(parsedQuery instanceof SelectStatement) {
                queryInfo.statementType = "select";
                setQueryInfoFromSelectStatement((SelectStatement) parsedQuery, queryInfo);

                final Limits limits = accessControl.getLimitsForIdentity(userName, client);

                selectQuery = new SelectQuery(queryInfo, runningQueriesManager, query, clientInfo, limits,
                        new DateTime(querySubmitTimestamp), (SelectStatement) parsedQuery, (byte)1, null);

                logQueryToLog4J(queryInfo.queryStringTruncatedForPrint, (Strings.isNullOrEmpty(userName) ? req.getRemoteAddr() : userName), -1);

                try {
                    selectQuery.lock(); // blocks and waits if necessary

                    querySubmitTimestamp = selectQuery.queryStartTimestamp.getMillis();   // ignore time spent waiting

                    // actually process
                    final SelectRequestArgs selectRequestArgs = new SelectRequestArgs(req, userName, client);

                    handleSelectStatement(selectQuery, selectRequestArgs, resp);
                } finally {
                    // this must be closed. but we may have to defer it to the async thread finishing query processing
                    if(!selectQuery.isAsynchronousRelease()) {
                        Closeables2.closeQuietly(selectQuery, log);
                    }

                }
            } else if(parsedQuery instanceof DescribeStatement) {
                queryInfo.statementType = "describe";
                final DescribeStatement describeStatement = (DescribeStatement) parsedQuery;
                queryInfo.datasets = Sets.newHashSet(describeStatement.dataset);
                queryInfo.datasetFields = Sets.newHashSet(describeStatement.dataset + "." + describeStatement.field);
                handleDescribeStatement(req, resp, describeStatement);
            } else if(parsedQuery instanceof ShowStatement) {
                queryInfo.statementType = "show";
                handleShowStatement(req, resp);
            } else {
                queryInfo.statementType = "invalid";
                throw new IqlKnownException.ParseErrorException("Query parsing failed: unknown statement type");
            }
        } catch (Throwable e) {
            final boolean progress = req.getParameter("progress") != null;
            if (e instanceof Exception) {
                e = ImhotepErrorResolver.resolve((Exception) e);
            }
            handleError(resp, json, e, true, progress);
            errorOccurred = e;
        } finally {
            try {
                String remoteAddr = getForwardedForIPAddress(req);
                if(remoteAddr == null) {
                    remoteAddr = req.getRemoteAddr();
                }
                logQuery(queryInfo, clientInfo, req, querySubmitTimestamp, errorOccurred, remoteAddr, this.metricStatsEmitter);
            } catch (Throwable ignored) { }
        }
    }

    private void setQueryInfoFromSelectStatement(SelectStatement selectStatement, QueryInfo queryInfo) {
        final FromClause from = selectStatement.from;
        final GroupByClause groupBy = selectStatement.groupBy;
        final SelectClause select =  selectStatement.select;

        if(from != null) {
            queryInfo.datasets = Collections.singleton(from.getDataset());

            if(from.getStart() != null && from.getEnd() != null) {
                queryInfo.totalDatasetRangeDays = new Duration(from.getStart(), from.getEnd()).toStandardDays().getDays();
            }
        }

        final int selectCount;
        if(select != null && select.getProjections() != null) {
            selectCount = select.getProjections().size();
        } else {
            selectCount = 0;
        }
        queryInfo.selectCount = selectCount;

        final int groupByCount;
        if(groupBy != null && groupBy.groupings != null) {
            groupByCount = groupBy.groupings.size();
        } else {
            groupByCount = 0;
        }
        queryInfo.groupByCount = groupByCount;
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

    private void handleSelectStatement(final SelectQuery selectQuery, final SelectRequestArgs args, final HttpServletResponse resp) throws IOException {
        final QueryInfo queryInfo = selectQuery.queryInfo;
        final SelectStatement parsedQuery = selectQuery.parsedStatement;
        // hashing is done before calling translate so only original JParsec parsing is considered
        final String queryForHashing = parsedQuery.toHashKeyString();

        final IQLQuery iqlQuery = IQLTranslator.translate(parsedQuery, imhotepClient,
                args.imhotepUserName, metadataCacheIQL1, selectQuery.limits, queryInfo);
        selectQuery.iqlQuery = iqlQuery;

        queryInfo.numShards = iqlQuery.getShards().size();
        queryInfo.datasetFields = iqlQuery.getDatasetFields();
        fieldFrequencyCache.acceptDatasetFields(queryInfo.datasetFields, selectQuery.clientInfo);


        // TODO: handle requested format mismatch: e.g. cached CSV but asked for TSV shouldn't have to rerun the query
        final String queryHash = SelectQuery.getQueryHash(queryForHashing, iqlQuery.getShards(), args.csv);
        queryInfo.cacheHashes = Collections.singleton(queryHash);
        final String cacheFileName = queryHash + (args.csv ? ".csv" : ".tsv");
        long beginTimeMillis = System.currentTimeMillis();
        final boolean isCached = queryCache.isFileCached(cacheFileName);
        long endTimeMillis = System.currentTimeMillis();
        queryInfo.cached = isCached;
        queryInfo.cacheCheckMillis = endTimeMillis - beginTimeMillis;
        final QueryMetadata queryMetadata = new QueryMetadata();

        queryMetadata.addItem("IQL-Cached", isCached, true);

        final DateTime startTime = iqlQuery.getStart();
        final DateTime endTime = iqlQuery.getEnd();

        final DateTime newestShard = getLatestShardVersion(iqlQuery.getShards());
        queryMetadata.addItem("IQL-Newest-Shard", newestShard, args.returnNewestShardVersion);

        final String shardList = shardListToString(iqlQuery.getShards());
        queryMetadata.addItem("IQL-Shard-List", shardList, args.returnShardlist);

        final List<Interval> timeIntervalsMissingShards= iqlQuery.getTimeIntervalsMissingShards();
        final List<Interval> properTimeIntervalsMissingShards = new ArrayList<Interval>();

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

        ArrayList<String> warningList = new ArrayList<>();


        if(properTimeIntervalsMissingShards.size() > 0) {
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
            warningList.add(percentAbsent + "% of the queried time period is missing: " + shortenedMissingIntervalsString);
        }

        if(timeIntervalsMissingShards.size() > 0) {
            final String missingIntervals = intervalListToString(timeIntervalsMissingShards);
            queryMetadata.addItem("IQL-Missing-Shards", missingIntervals);
        }

        queryMetadata.setPendingHeaders(resp);

        queryInfo.headOnly = args.headOnly;
        if (args.headOnly) {
            return;
        }
        final ServletOutputStream outputStream = resp.getOutputStream();
        if (args.progress) {
            outputStream.print(": This is the start of the IQL Query Stream\n\n");
        }

        // TODO remove
        if (true) {
            setContentType(resp, args.avoidFileSave, args.csv, args.progress);
            if (!args.cacheReadDisabled && isCached) {
                log.trace("Returning cached data in " + cacheFileName);

                // read metadata from cache
                try {
                    final InputStream metadataCacheStream = queryCache.getInputStream(cacheFileName + METADATA_FILE_SUFFIX);
                    final QueryMetadata cachedMetadata = QueryMetadata.fromStream(metadataCacheStream);
                    queryMetadata.mergeIn(cachedMetadata);

                    queryMetadata.setPendingHeaders(resp);
                    resp.setHeader("Access-Control-Expose-Headers", StringUtils.join(resp.getHeaderNames(), ", "));
                } catch (Exception e) {
                    log.info("Failed to load metadata cache from " + cacheFileName + METADATA_FILE_SUFFIX, e);
                }

                final InputStream cacheInputStream = queryCache.getInputStream(cacheFileName);
                final int rowsWritten = IQLQuery.copyStream(cacheInputStream, outputStream, Integer.MAX_VALUE, args.progress);
                if(args.progress) {
                    completeStream(outputStream, queryMetadata);
                }
                outputStream.close();
                queryInfo.rows = rowsWritten;
                return;
            }
            final IQLQuery.WriteResults writeResults;
            final IQLQuery.ExecutionResult executionResult;
            try {
                // TODO: should we always get totals? opt out http param?
                final DateTime execStartTime = DateTime.now();

                executionResult = iqlQuery.execute(args.progress, outputStream, true);

                queryMetadata.addItem("IQL-Totals", Arrays.toString(executionResult.getTotals()), args.getTotals);
                queryMetadata.addItem("Imhotep-Session-ID", queryInfo.sessionIDs.iterator().next());
                queryMetadata.addItem("IQL-Execution-Time", execStartTime.toString());

                queryMetadata.setPendingHeaders(resp);
                resp.setHeader("Access-Control-Expose-Headers", StringUtils.join(resp.getHeaderNames(), ", "));

                final Iterator<GroupStats> groupStats = executionResult.getRows();
                final int groupingColumns = Math.max(1, (parsedQuery.groupBy == null || parsedQuery.groupBy.groupings == null) ? 1 : parsedQuery.groupBy.groupings.size());
                final int selectColumns = Math.max(1, (parsedQuery.select == null || parsedQuery.select.getProjections() == null) ? 1 : parsedQuery.select.getProjections().size());
                final long beginSendToClientMillis = System.currentTimeMillis();
                writeResults = iqlQuery.outputResults(groupStats, outputStream, args.csv, args.progress, iqlQuery.getRowLimit(), groupingColumns, selectColumns, args.cacheWriteDisabled);
                queryInfo.sendToClientMillis = System.currentTimeMillis() - beginSendToClientMillis;
                if (writeResults.exceedsLimit) {
                    warningList.add("Only first " + iqlQuery.getRowLimit() + " rows returned sorted on the last group by column");
                }

                if (warningList.size()>0){
                    String warning = "[\"" + StringUtils.join(warningList, "\",\"") + "\"]";
                    queryMetadata.addItem("IQL-Warning", warning);
                }

                queryMetadata.addItem("IQL-Query-Info", queryInfo.toJSON());

                final QueryMetadata queryMetadataToCache = queryMetadata.copy();

                if (!args.cacheWriteDisabled && !isCached) {
                    executorService.submit(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            try {
                                try {
                                    final OutputStream metadataCacheStream = queryCache.getOutputStream(cacheFileName + METADATA_FILE_SUFFIX);
                                    queryMetadataToCache.toStream(metadataCacheStream);
                                    metadataCacheStream.close();
                                } catch (Exception e) {
                                    log.warn("Failed to upload metadata cache: " + cacheFileName, e);
                                }
                                try {
                                    uploadResultsToCache(writeResults, cacheFileName, args.csv);
                                } catch (Exception e) {
                                    log.warn("Failed to upload cache: " + cacheFileName, e);
                                }
                            } finally {
                                Closeables2.closeQuietly(selectQuery, log);
                            }
                            return null;
                        }
                    });
                    selectQuery.markAsynchronousRelease(); // going to be closed asynchronously after cache is uploaded
                }
            } catch (ImhotepOutOfMemoryException e) {
                throw Throwables.propagate(e);
            } finally {
                try {
                    queryInfo.setFromPerformanceStats(iqlQuery.closeAndGetPerformanceStats());
                } catch (Exception e) {
                    log.error("Exception while closing IQLQuery object", e);
                }
            }
            if (queryInfo.cpuSlotsExecTimeMs != null) {
            	queryMetadata.addItem("Imhotep-CPU-Slots-Execution-Time-MS", queryInfo.cpuSlotsExecTimeMs);
            }
            if(args.progress) {
            	completeStream(outputStream, queryMetadata);
            }
            outputStream.close();
            queryInfo.rows = writeResults.rowsWritten;
            // TODO
//            queryInfo.overflowedToDisk = writeResults.didOverflowToDisk();
        }
    }

    private void completeStream(ServletOutputStream outputStream, QueryMetadata queryMetadata) throws IOException {
        outputStream.println();
        outputStream.println("event: header");
        outputStream.print("data: ");
        outputStream.print(queryMetadata.toJSON() + "\n\n");
        outputStream.print("event: complete\ndata: :)\n\n");
        outputStream.flush();
    }

    private static final DateTimeFormatter yyyymmddhhmmss = DateTimeFormat.forPattern("yyyyMMddHHmmss").withZone(DateTimeZone.forOffsetHours(-6));

    @Nullable
    private static DateTime getLatestShardVersion(List<Shard> shardVersionList) {
        long maxVersion = 0;
        if(shardVersionList == null || shardVersionList.size() == 0) {
            return null;
        }
        for(Shard shard : shardVersionList) {
            if(shard.getVersion() > maxVersion) {
                maxVersion = shard.getVersion();
            }
        }
        if(maxVersion == 0) {
            return null;
        }
        return yyyymmddhhmmss.parseDateTime(String.valueOf(maxVersion));
    }

    private static String shardListToString(List<Shard> shardVersionList) {
        if(shardVersionList == null) {
            return "";
        }

        final StringBuilder sb = new StringBuilder();
        for(Shard shard : shardVersionList) {
            if(sb.length() != 0) {
                sb.append(",");
            }
            sb.append(shard.getShardId()).append(".").append(shard.getVersion());
        }
        return sb.toString();
    }

    private static final DateTimeFormatter yyyymmddhh = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH").withZone(DateTimeZone.forOffsetHours(-6));
    private static final DateTimeFormatter yyyymmdd = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.forOffsetHours(-6));

    private static String intervalListToString(List<Interval> intervals) {
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

    private void uploadResultsToCache(IQLQuery.WriteResults writeResults, String cachedFileName, boolean csv) throws IOException {
        if(writeResults.resultCacheIterator != null) {
            // use the memory cached data
            final OutputStream cacheStream = queryCache.getOutputStream(cachedFileName);
            IQLQuery.writeRowsToStream(writeResults.resultCacheIterator, cacheStream, csv, Integer.MAX_VALUE, false);
            cacheStream.close(); // has to be closed
        } else if(writeResults.unsortedFile != null) {
            // cache overflowed to disk so read from file
            try {
                queryCache.writeFromFile(cachedFileName, writeResults.unsortedFile);
            } finally {
                if(!writeResults.unsortedFile.delete()) {
                    log.info("Failed to delete: " + writeResults.unsortedFile.getPath());
                }
            }
        } else {    // this should never happen
            log.warn("Results are not available to upload cache to HDFS: " + cachedFileName);
        }
    }

    private void handleDescribeStatement(HttpServletRequest req, HttpServletResponse resp, DescribeStatement parsedQuery) throws IOException {
        if(Strings.isNullOrEmpty(parsedQuery.field)) {
            handleDescribeDataset(req, resp, parsedQuery);
        } else {
            handleDescribeField(req, resp, parsedQuery);
        }
    }

    private void handleDescribeField(HttpServletRequest req, HttpServletResponse resp, DescribeStatement parsedQuery) throws IOException {
        final PrintWriter outputStream = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(resp.getOutputStream()), Charsets.UTF_8));
        final String dataset = parsedQuery.dataset;
        final String fieldName = parsedQuery.field;
        final List<String> topTerms = topTermsCache.getTopTerms(dataset, fieldName);
        FieldMetadata field = metadataCacheIQL1.getDataset(dataset).getField(fieldName);
        if(field == null) {
            field = new FieldMetadata("notfound", FieldType.String);
            field.setDescription("Field not found");
        }
        final boolean json = req.getParameter("json") != null;
        if(json) {
            resp.setContentType(MediaType.APPLICATION_JSON_VALUE);
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
        outputStream.close();
    }

    private void handleDescribeDataset(HttpServletRequest req, HttpServletResponse resp, DescribeStatement parsedQuery) throws IOException {
        final PrintWriter outputStream = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(resp.getOutputStream()), Charsets.UTF_8));
        final String dataset = parsedQuery.dataset;
        final DatasetMetadata datasetMetadata = metadataCacheIQL1.getDataset(dataset);
        final boolean json = req.getParameter("json") != null;
        if(json) {
            resp.setContentType(MediaType.APPLICATION_JSON_VALUE);
            final ObjectNode jsonRoot = OBJECT_MAPPER.createObjectNode();
            datasetMetadata.toJSON(jsonRoot, OBJECT_MAPPER, false);

            OBJECT_MAPPER.writeValue(outputStream, jsonRoot);
        } else {
            for(FieldMetadata field : datasetMetadata.intFields) {
                outputStream.println(field.toTSV());
            }
            for(FieldMetadata field : datasetMetadata.stringFields) {
                outputStream.println(field.toTSV());
            }
        }
        outputStream.close();
    }

    private void handleShowStatement(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
        final PrintWriter outputStream = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(resp.getOutputStream()), Charsets.UTF_8));
        final boolean json = req.getParameter("json") != null;

        if(json) {
            resp.setContentType(MediaType.APPLICATION_JSON_VALUE);
            final ObjectNode jsonRoot = OBJECT_MAPPER.createObjectNode();
            final ArrayNode array = OBJECT_MAPPER.createArrayNode();
            jsonRoot.set("datasets", array);
            for(DatasetMetadata dataset : metadataCacheIQL1.get().getDatasetToMetadata().values()) {
                final ObjectNode datasetInfo = OBJECT_MAPPER.createObjectNode();
                dataset.toJSON(datasetInfo, OBJECT_MAPPER, true);
                array.add(datasetInfo);
            }
            OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(outputStream, jsonRoot);
        } else {
            for(DatasetMetadata dataset : metadataCacheIQL1.get().getDatasetToMetadata().values()) {
                outputStream.println(dataset.name);
            }
        }
        outputStream.close();
    }

    public static void handleError(HttpServletResponse resp, boolean json, Throwable e, boolean status500, boolean isEventStream) throws IOException {
        if(!(e instanceof Exception || e instanceof OutOfMemoryError)) {
            throw Throwables.propagate(e);
        }
        // output parse/execute error
        if(!json) {
            final ServletOutputStream outputStream = resp.getOutputStream();
            final PrintStream printStream = new PrintStream(outputStream, false, "UTF-8");
            if(isEventStream) {
                resp.setContentType("text/event-stream");
                final String[] stackTrace = Throwables.getStackTraceAsString(e).split("\\n");
                printStream.println("event: servererror");
                for (String s : stackTrace) {
                    printStream.println("data: " + s);
                }
                printStream.println();
            } else {
                resp.setStatus(500);
                e.printStackTrace(printStream);
                printStream.close();
            }
        } else {
            if(status500) {
                resp.setStatus(500);
            }
            // construct a parsed error object to be JSON serialized
            String clause = "";
            int offset = -1;
            if(e instanceof IqlKnownException.StatementParseException) {
                final IqlKnownException.StatementParseException IQLParseException = (IqlKnownException.StatementParseException) e;
                clause = IQLParseException.getClause();
                offset = IQLParseException.getOffsetInClause();
            }
            final String stackTrace = Throwables.getStackTraceAsString(Throwables.getRootCause(e));
            final ErrorResult error = new ErrorResult(e.getClass().getSimpleName(), e.getMessage(), stackTrace, clause, offset);
            resp.setContentType("application/json");
            final ServletOutputStream outputStream = resp.getOutputStream();
            OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(outputStream, error);
            outputStream.close();
        }
    }

    // Logging code below

    // trying to not cause the logentry to overflow from being larger than 2^16
    // this is the pre URL-encoded limit and encoding can make it about twice longer
    private static final int LOGGED_FIELD_LENGTH_LIMIT = 8092;

    // TODO
    public static void logQuery(QueryInfo queryInfo,
                                 ClientInfo clientInfo,
                                 HttpServletRequest req,
                                 long queryStartTimestamp,
                                 Throwable errorOccurred,
                                 String remoteAddr,
                                 MetricStatsEmitter metricStatsEmitter) {
        final long timeTaken = System.currentTimeMillis() - queryStartTimestamp;
        final String queryWithShortenedLists = queryInfo.queryStringTruncatedForPrint;
        if (timeTaken > 5000) {  // we've already logged the query so only log again if it took a long time to run
            logQueryToLog4J(queryWithShortenedLists, (Strings.isNullOrEmpty(clientInfo.username) ? remoteAddr : clientInfo.username), timeTaken);
        }

        final QueryLogEntry logEntry = new QueryLogEntry();
        logEntry.setProperty("v", 0);
        logEntry.setProperty("iqlversion",queryInfo.iqlVersion);
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
        if (queryInfo.datasetFields != null && queryInfo.datasetFields.size() > 0) {
            logSet(logEntry, "datasetfield", queryInfo.datasetFields);
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

        logSet(logEntry, "hash", queryInfo.cacheHashes);
        logString(logEntry, "hostname", hostname);
        logInteger(logEntry, "maxgroups", queryInfo.maxGroups);
        logInteger(logEntry, "maxconcurrentsessions", queryInfo.maxConcurrentSessions);
        logInteger(logEntry, "rows", queryInfo.rows);
        logSet(logEntry, "sessionid", queryInfo.sessionIDs);
        logInteger(logEntry, "shards", queryInfo.numShards);
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
        final String queryToLog = queryWithShortenedLists.length() > LOGGED_FIELD_LENGTH_LIMIT ? queryWithShortenedLists.substring(0, LOGGED_FIELD_LENGTH_LIMIT) : queryWithShortenedLists;
        logEntry.setProperty("q", queryToLog);
        logEntry.setProperty("qlen", queryInfo.queryLength);
        final boolean error = errorOccurred != null;
        final boolean systemError = error && !ServletUtil.isKnownError(errorOccurred);
        logEntry.setProperty("error", error ? "1" : "0");
        logEntry.setProperty("systemerror", systemError ? "1" : "0");
        if (error) {
            logEntry.setProperty("exceptiontype", errorOccurred.getClass().getSimpleName());
            String exceptionMessage = errorOccurred.getMessage();
            if(exceptionMessage != null && exceptionMessage.length() > LOGGED_FIELD_LENGTH_LIMIT) {
                exceptionMessage = exceptionMessage.substring(0, LOGGED_FIELD_LENGTH_LIMIT);
            }
            if (exceptionMessage == null) {
                exceptionMessage = "<no msg>";
            }
            logEntry.setProperty("exceptionmsg", exceptionMessage);
        }

        QueryMetrics.logQueryMetrics(queryInfo.iqlVersion, queryInfo.statementType, error, systemError, timeTaken, metricStatsEmitter);
        dataLog.info(logEntry);
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
            resp.setContentType("text/plain");
        } else if (csv) {
            resp.setContentType("text/csv");
            resp.setHeader("Content-Disposition", "inline; filename=\"iqlresult.csv\"");
        } else if (progress) {
            resp.setContentType("text/event-stream");
            resp.setHeader("Cache-Control", "no-cache");
        } else {
            resp.setContentType("text/tab-separated-values");
            resp.setHeader("Content-Disposition", "inline; filename=\"iqlresult.tsv\"");
        }
    }

    private class SelectRequestArgs {
        public final boolean avoidFileSave;
        public final boolean csv;
        public final boolean interactive;
        public final boolean returnShardlist;
        public final boolean returnNewestShardVersion;
        public final boolean cacheReadDisabled;
        public final boolean cacheWriteDisabled;
        public final boolean headOnly;
        public final boolean progress;
        public final boolean getTotals;
        public final String imhotepUserName;
        public final String requestURL;

        public SelectRequestArgs(HttpServletRequest req, String userName, String clientName) {
            avoidFileSave = req.getParameter("view") != null;
            csv = req.getParameter("csv") != null;
            interactive = req.getParameter("interactive") != null;
            returnShardlist = req.getParameter("getshardlist") != null;
            returnNewestShardVersion = req.getParameter("getversion") != null;
            cacheReadDisabled = !queryCache.isEnabled() || req.getParameter("nocacheread") != null || req.getParameter("nocache") != null;
            cacheWriteDisabled = !queryCache.isEnabled() || req.getParameter("nocachewrite") != null || req.getParameter("nocache") != null;
            headOnly = "HEAD".equals(req.getMethod()) || req.getParameter("head") != null;
            progress = req.getParameter("progress") != null;
            getTotals = req.getParameter("totals") != null;
            imhotepUserName = (!Strings.isNullOrEmpty(userName) ? userName : clientName);
            requestURL = req.getRequestURL().toString();
        }
    }
}
