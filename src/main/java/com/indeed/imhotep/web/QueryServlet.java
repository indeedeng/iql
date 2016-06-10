/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.imhotep.web;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.ShardIdWithVersion;
import com.indeed.imhotep.ez.EZImhotepSession;
import com.indeed.imhotep.iql.GroupStats;
import com.indeed.imhotep.iql.IQLQuery;
import com.indeed.imhotep.iql.SelectExecutionStats;
import com.indeed.imhotep.iql.cache.QueryCache;
import com.indeed.imhotep.metadata.DatasetMetadata;
import com.indeed.imhotep.metadata.FieldMetadata;
import com.indeed.imhotep.metadata.FieldType;
import com.indeed.imhotep.sql.IQLTranslator;
import com.indeed.imhotep.sql.ast2.DescribeStatement;
import com.indeed.imhotep.sql.ast2.FromClause;
import com.indeed.imhotep.sql.ast2.GroupByClause;
import com.indeed.imhotep.sql.ast2.IQLStatement;
import com.indeed.imhotep.sql.ast2.SelectClause;
import com.indeed.imhotep.sql.ast2.SelectStatement;
import com.indeed.imhotep.sql.ast2.ShowStatement;
import com.indeed.imhotep.sql.parser.StatementParser;
import com.indeed.util.core.io.Closeables2;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import javax.servlet.ServletException;
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
import java.net.URL;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
* @author dwahler
*/
@Controller
public class QueryServlet {
    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
        TimeZone.setDefault(TimeZone.getTimeZone("GMT-6"));
        GlobalUncaughtExceptionHandler.register();
        try {
            hostname = java.net.InetAddress.getLocalHost().getHostName();
        }
        catch (java.net.UnknownHostException ex) {
            hostname = "(unknown)";
        }
    }

    private static final Logger log = Logger.getLogger(QueryServlet.class);
    private static final Logger dataLog = Logger.getLogger("indeed.logentry");
    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
    private static final String METADATA_FILE_SUFFIX = ".meta";
    // this can be incremented to invalidate the old cache
    private static final byte VERSION_FOR_HASHING = 2;
    private static String hostname;

    private static final Set<String> USED_PARAMS = Sets.newHashSet("view", "sync", "csv", "json", "interactive", "nocache");

    private final ImhotepClient imhotepClient;
    private final ImhotepClient imhotepInteractiveClient;
    private final ImhotepMetadataCache metadata;
    private final TopTermsCache topTermsCache;
    private final QueryCache queryCache;
    private final ExecutionManager executionManager;
    private final ExecutorService executorService;
    private final long imhotepLocalTempFileSizeLimit;
    private final long imhotepDaemonTempFileSizeLimit;
    private final AccessControl accessControl;

    @Autowired
    public QueryServlet(ImhotepClient imhotepClient,
                        ImhotepClient imhotepInteractiveClient,
                        ImhotepMetadataCache metadata,
                        TopTermsCache topTermsCache,
                        QueryCache queryCache,
                        ExecutionManager executionManager,
                        ExecutorService executorService,
                        Integer rowLimit,
                        Long imhotepLocalTempFileSizeLimit,
                        Long imhotepDaemonTempFileSizeLimit,
                        AccessControl accessControl) {
        this.imhotepClient = imhotepClient;
        this.imhotepInteractiveClient = imhotepInteractiveClient;
        this.metadata = metadata;
        this.topTermsCache = topTermsCache;
        this.queryCache = queryCache;
        this.executionManager = executionManager;
        this.executorService = executorService;
        this.imhotepLocalTempFileSizeLimit = imhotepLocalTempFileSizeLimit;
        this.imhotepDaemonTempFileSizeLimit = imhotepDaemonTempFileSizeLimit;
        this.accessControl = accessControl;
        EZImhotepSession.GROUP_LIMIT = rowLimit;
    }

    @RequestMapping("/query")
    protected void doGet(final HttpServletRequest req, final HttpServletResponse resp,
                         @Nonnull @RequestParam("q") String query) throws ServletException, IOException {

        final String httpUserName = getUserNameFromRequest(req);
        final String userName = Strings.nullToEmpty(Strings.isNullOrEmpty(httpUserName) ? req.getParameter("username") : httpUserName);
        long queryStartTimestamp = System.currentTimeMillis();

        final boolean json = req.getParameter("json") != null;
        IQLStatement parsedQuery = null;
        final SelectExecutionStats selectExecutionStats = new SelectExecutionStats();
        Throwable errorOccurred = null;
        try {
            if(Strings.isNullOrEmpty(req.getParameter("client")) && Strings.isNullOrEmpty(userName)) {
                throw new IdentificationRequiredException("IQL query requests have to include parameters 'client' and 'username' for identification");
            }
            accessControl.checkAllowedAccess(userName);

            parsedQuery = StatementParser.parse(query, metadata);
            if(parsedQuery instanceof SelectStatement) {
                logQueryToLog4J(query, (Strings.isNullOrEmpty(userName) ? req.getRemoteAddr() : userName), -1);

                final ExecutionManager.QueryTracker queryTracker = executionManager.queryStarted(query, userName);
                try {
                    queryTracker.acquireLocks(); // blocks and waits if necessary

                    queryStartTimestamp = System.currentTimeMillis();   // ignore time spent waiting

                    // actually process
                    final SelectRequestArgs selectRequestArgs = new SelectRequestArgs(req, userName);
                    handleSelectStatement(selectRequestArgs, resp, (SelectStatement) parsedQuery, queryTracker, selectExecutionStats);
                } finally {
                    // this must be closed. but we may have to defer it to the async thread finishing query processing
                    if(!queryTracker.isAsynchronousRelease()) {
                        Closeables2.closeQuietly(queryTracker, log);
                    }
                }
            } else if(parsedQuery instanceof DescribeStatement) {
                handleDescribeStatement(req, resp, (DescribeStatement)parsedQuery);
            } else if(parsedQuery instanceof ShowStatement) {
                handleShowStatement(req, resp);
            } else {
                throw new RuntimeException("Query parsing failed: unknown statement type");
            }
        } catch (Throwable e) {
            final boolean progress = req.getParameter("progress") != null;
            handleError(resp, json, e, true, progress);
            errorOccurred = e;
        } finally {
            try {
                String remoteAddr = getForwardedForIPAddress(req);
                if(remoteAddr == null) {
                    remoteAddr = req.getRemoteAddr();
                }
                logQuery(req, query, userName, queryStartTimestamp, parsedQuery, selectExecutionStats, errorOccurred, remoteAddr);
            } catch (Throwable ignored) { }
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

    /**
     * Gets the user name from the HTTP request if it was provided through Basic authentication.
     * 
     * @param request Http request
     * @return User name if Basic auth is used or null otherwise
     */
    private static String getUserNameFromRequest(final HttpServletRequest request) {
        final String authHeader = request.getHeader("Authorization");
        if (authHeader == null) {
            // try simple
            final String rawUser = request.getRemoteUser();
            if (rawUser == null) {
                return null;
            } else {
                return rawUser;
            }
        } else {
            final String credStr;
            if (authHeader.startsWith("user ")) {
                credStr = authHeader.substring(5);
            } else {
                // try basic auth
                if (!authHeader.toUpperCase().startsWith("BASIC ")) {
                    // Not basic
                    return null;
                }

                // remove basic
                final String credEncoded = authHeader.substring(6); //length of 'BASIC '

                final byte[] credRaw = Base64.decodeBase64(credEncoded.getBytes());
                if (credRaw == null) {
                    // invalid decoding
                    return null;
                }

                credStr = new String(credRaw);
            }

            // get username part from username:password
            final String[] x = credStr.split(":");
            if (x.length < 1) {
                // bad split
                return null;
            }

            return x[0];
        }
    }

    private void handleSelectStatement(final SelectRequestArgs args, final HttpServletResponse resp, SelectStatement parsedQuery, final ExecutionManager.QueryTracker queryTracker, final @Nonnull SelectExecutionStats selectExecutionStats) throws IOException {
        // hashing is done before calling translate so only original JParsec parsing is considered
        final String queryForHashing = parsedQuery.toHashKeyString();

        final IQLQuery iqlQuery = IQLTranslator.translate(parsedQuery, args.interactive ? imhotepInteractiveClient : imhotepClient,
                args.imhotepUserName, metadata, imhotepLocalTempFileSizeLimit, imhotepDaemonTempFileSizeLimit);

        selectExecutionStats.shardCount = iqlQuery.getShardVersionList().size();

        // TODO: handle requested format mismatch: e.g. cached CSV but asked for TSV shouldn't have to rerun the query
        final String queryHash = getQueryHash(queryForHashing, iqlQuery.getShardVersionList(), args.csv);
        selectExecutionStats.hashForCaching = queryHash;
        final String cacheFileName = queryHash + (args.csv ? ".csv" : ".tsv");
        long beginTimeMillis = System.currentTimeMillis();
        final boolean isCached = queryCache.isFileCached(cacheFileName);
        long endTimeMillis = System.currentTimeMillis();
        selectExecutionStats.cached = isCached;
        selectExecutionStats.setPhase("cacheCheckMillis", endTimeMillis - beginTimeMillis);
        final QueryMetadata queryMetadata = new QueryMetadata();

        queryMetadata.addItem("IQL-Cached", isCached, true);

        final DateTime startTime = iqlQuery.getStart();
        final DateTime endTime = iqlQuery.getEnd();

        final DateTime newestShard = getLatestShardVersion(iqlQuery.getShardVersionList());
        queryMetadata.addItem("IQL-Newest-Shard", newestShard, args.returnNewestShardVersion);

        final String shardList = shardListToString(iqlQuery.getShardVersionList());
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
            int millisMissing = 0;
            final String missingIntervals;
            final int countMissingIntervals = properTimeIntervalsMissingShards.size();


            for (Interval interval: properTimeIntervalsMissingShards){
                millisMissing += interval.getEndMillis()-interval.getStartMillis();
            }

            final double totalPeriod = endTime.getMillis()-startTime.getMillis();

            final double percentMissing = millisMissing/totalPeriod*100;
            final String percentAbsent = String.format("%.2f",percentMissing);

            if (countMissingIntervals>5) {
                final List<Interval> properSubList =properTimeIntervalsMissingShards.subList(0, 5);
                missingIntervals = intervalListToString(properSubList);
                warningList.add( percentAbsent + "% of the queried time period is missing. Click arrow for more info.");
                warningList.add("The first 5 missing time periods are: " + missingIntervals + " (total of " + countMissingIntervals + ").");
            } else {
                missingIntervals = intervalListToString(properTimeIntervalsMissingShards);
                warningList.add(percentAbsent + "% of the queried time period is missing. Click arrow for more info. ");
                warningList.add( "The missing time periods are: " + missingIntervals);
            }

            queryMetadata.addItem("IQL-Missing-Shards", missingIntervals);

        }

        queryMetadata.setPendingHeaders(resp);

        if (args.headOnly) {
            selectExecutionStats.headOnly = true;
            return;
        }
        final ServletOutputStream outputStream = resp.getOutputStream();
        if (args.progress) {
            outputStream.print(": This is the start of the IQL Query Stream\n\n");
        }
        if (!args.asynchronous) {
            ResultServlet.setContentType(resp, args.avoidFileSave, args.csv, args.progress);
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
                final int rowsWritten = IQLQuery.copyStream(cacheInputStream, outputStream, iqlQuery.getRowLimit(), args.progress);
                if(args.progress) {
                    completeStream(outputStream, queryMetadata);
                }
                outputStream.close();
                selectExecutionStats.rowsWritten = rowsWritten;
                return;
            }
            final IQLQuery.WriteResults writeResults;
            final IQLQuery.ExecutionResult executionResult;
            try {
                // TODO: should we always get totals? opt out http param?
                final DateTime execStartTime = DateTime.now();
                executionResult = iqlQuery.execute(args.progress, outputStream, true, selectExecutionStats);
                queryMetadata.addItem("IQL-Timings", executionResult.getTimings().replace('\n', '\t'), args.progress);
                queryMetadata.addItem("IQL-Imhotep-Temp-Bytes-Written", executionResult.getImhotepTempFilesBytesWritten(), args.progress);
                queryMetadata.addItem("IQL-Totals", Arrays.toString(executionResult.getTotals()), args.getTotals);
                queryMetadata.addItem("Imhotep-Session-ID", selectExecutionStats.sessionId);
                queryMetadata.addItem("IQL-Execution-Time", execStartTime.toString());

                queryMetadata.setPendingHeaders(resp);
                resp.setHeader("Access-Control-Expose-Headers", StringUtils.join(resp.getHeaderNames(), ", "));

                final Iterator<GroupStats> groupStats = executionResult.getRows();
                final int groupingColumns = Math.max(1, (parsedQuery.groupBy == null || parsedQuery.groupBy.groupings == null) ? 1 : parsedQuery.groupBy.groupings.size());
                final int selectColumns = Math.max(1, (parsedQuery.select == null || parsedQuery.select.getProjections() == null) ? 1 : parsedQuery.select.getProjections().size());
                writeResults = iqlQuery.outputResults(groupStats, outputStream, args.csv, args.progress, iqlQuery.getRowLimit(), groupingColumns, selectColumns, args.cacheWriteDisabled);
                if (writeResults.exceedsLimit) {
                    warningList.add("Only first " + iqlQuery.getRowLimit() + " rows returned sorted on the last group by column");
                }
                String warning = "[\"" + StringUtils.join(warningList, "\",\"") + "\"]";
                queryMetadata.addItem("IQL-Warning", warning);


                if(args.progress) {
                    completeStream(outputStream, queryMetadata);
                }

                if (!args.cacheWriteDisabled && !isCached) {
                    executorService.submit(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            try {
                                try {
                                    final OutputStream metadataCacheStream = queryCache.getOutputStream(cacheFileName + METADATA_FILE_SUFFIX);
                                    queryMetadata.toStream(metadataCacheStream);
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
                                Closeables2.closeQuietly(queryTracker, log);
                            }
                            return null;
                        }
                    });
                    queryTracker.markAsynchronousRelease(); // going to be closed asynchronously after cache is uploaded
                }
            } catch (ImhotepOutOfMemoryException e) {
                throw Throwables.propagate(e);
            } finally {
                Closeables2.closeQuietly(iqlQuery, log);
            }
            outputStream.close();
            selectExecutionStats.rowsWritten = writeResults.rowsWritten;
            selectExecutionStats.imhotepTempFilesBytesWritten = executionResult.getImhotepTempFilesBytesWritten();
        } else {
            // TODO: rework the async case to use the same code path as the sync case above except running under an executor
            if (!isCached && args.cacheWriteDisabled) {
                throw new IllegalStateException("Query cache is disabled so only synchronous calls can be served");
            }

            resp.setContentType("application/json");

            if (!isCached) {
                executorService.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        try {
                            // TODO: get totals working with the cache
                            final IQLQuery.ExecutionResult executionResult = iqlQuery.execute(false, null, false, selectExecutionStats);
                            final Iterator<GroupStats> groupStats = executionResult.getRows();

                            final OutputStream cacheStream = queryCache.getOutputStream(cacheFileName);
                            IQLQuery.writeRowsToStream(groupStats, cacheStream, args.csv, Integer.MAX_VALUE, false);
                            cacheStream.close();    // has to be closed
                            return null;
                        } finally {
                            Closeables2.closeQuietly(iqlQuery, log);
                            Closeables2.closeQuietly(queryTracker, log);
                        }
                    }
                });
                queryTracker.markAsynchronousRelease(); // going to be closed asynchronously after cache is uploaded
            }

            final URL baseURL = new URL(args.requestURL);
            final URL resultsURL = new URL(baseURL, "results/" + cacheFileName);

            final ObjectMapper mapper = new ObjectMapper();
            final ObjectNode ret = mapper.createObjectNode();
            ret.put("filename", resultsURL.toString());
            mapper.writeValue(outputStream, ret);
            outputStream.close();
            // we don't know number of rows as it's handled asynchronously
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
    private static DateTime getLatestShardVersion(List<ShardIdWithVersion> shardVersionList) {
        long maxVersion = 0;
        if(shardVersionList == null || shardVersionList.size() == 0) {
            return null;
        }
        for(ShardIdWithVersion shard : shardVersionList) {
            if(shard.getVersion() > maxVersion) {
                maxVersion = shard.getVersion();
            }
        }
        if(maxVersion == 0) {
            return null;
        }
        return yyyymmddhhmmss.parseDateTime(String.valueOf(maxVersion));
    }

    private static String shardListToString(List<ShardIdWithVersion> shardVersionList) {
        if(shardVersionList == null) {
            return "";
        }

        final StringBuilder sb = new StringBuilder();
        for(ShardIdWithVersion shard : shardVersionList) {
            if(sb.length() != 0) {
                sb.append(",");
            }
            sb.append(shard.getShardId()).append(".").append(shard.getVersion());
        }
        return sb.toString();
    }

    private static final DateTimeFormatter yyyymmddhh = DateTimeFormat.forPattern("yyyy-MM-dd.HH").withZone(DateTimeZone.forOffsetHours(-6));

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
            sb.append(interval.getStart().toString(yyyymmddhh)).append(" - ").append(interval.getEnd().toString(yyyymmddhh));
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
        FieldMetadata field = metadata.getDataset(dataset).getField(fieldName);
        if(field == null) {
            field = new FieldMetadata("notfound", FieldType.String);
            field.setDescription("Field not found");
        }
        final boolean json = req.getParameter("json") != null;
        if(json) {
            resp.setContentType(MediaType.APPLICATION_JSON_VALUE);
            final ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
            final ObjectNode jsonRoot = mapper.createObjectNode();
            field.toJSON(jsonRoot);

            final ArrayNode termsArray = mapper.createArrayNode();
            jsonRoot.set("topTerms", termsArray);
            for(String term : topTerms) {
                termsArray.add(term);
            }
            mapper.writeValue(outputStream, jsonRoot);
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
        final DatasetMetadata datasetMetadata = metadata.getDataset(dataset);
        final boolean json = req.getParameter("json") != null;
        if(json) {
            resp.setContentType(MediaType.APPLICATION_JSON_VALUE);
            final ObjectMapper mapper = new ObjectMapper();
            mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
            final ObjectNode jsonRoot = mapper.createObjectNode();
            datasetMetadata.toJSON(jsonRoot, mapper, false);

            mapper.writeValue(outputStream, jsonRoot);
        } else {
            for(FieldMetadata field : datasetMetadata.getFields().values()) {
                final String description = Strings.nullToEmpty(field.getDescription());
                outputStream.println(field.getName() + "\t" + description);
            }
        }
        outputStream.close();
    }

    private void handleShowStatement(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
        final PrintWriter outputStream = new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(resp.getOutputStream()), Charsets.UTF_8));
        final boolean json = req.getParameter("json") != null;

        if(json) {
            resp.setContentType(MediaType.APPLICATION_JSON_VALUE);
            final ObjectMapper mapper = new ObjectMapper();
            final ObjectNode jsonRoot = mapper.createObjectNode();
            final ArrayNode array = mapper.createArrayNode();
            jsonRoot.set("datasets", array);
            for(DatasetMetadata dataset : metadata.getDatasets().values()) {
                final ObjectNode datasetInfo = mapper.createObjectNode();
                dataset.toJSON(datasetInfo, mapper, true);
                array.add(datasetInfo);
            }
            mapper.writeValue(outputStream, jsonRoot);
        } else {
            for(DatasetMetadata dataset : metadata.getDatasets().values()) {
                outputStream.println(dataset.getName());
            }
        }
        outputStream.close();
    }

    /**
     * Produces a Base64 encoded SHA-1 hash of the query and the list of shard names/versions which has to be sorted.
     */
    private String getQueryHash(String query, Collection<ShardIdWithVersion> shards, boolean csv) {
        final MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        }
        catch(NoSuchAlgorithmException e) {
            log.error("Failed to init SHA1", e);
            throw Throwables.propagate(e);
        }
        final String standardizedQuery = query.trim().replace('"', '\'').replaceAll("\\s+", " ");
        sha1.update(standardizedQuery.getBytes(UTF8_CHARSET));
        if(shards != null) {
            for(ShardIdWithVersion shard : shards) {
                sha1.update(shard.getShardId().getBytes(UTF8_CHARSET));
                sha1.update(Longs.toByteArray(shard.getVersion()));
                sha1.update(csv ? (byte)1 : 0);
            }
        }
        sha1.update(VERSION_FOR_HASHING);
        return Base64.encodeBase64URLSafeString(sha1.digest());
    }

    static void handleError(HttpServletResponse resp, boolean json, Throwable e, boolean status500, boolean isEventStream) throws IOException {
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
            if(e instanceof IQLParseException) {
                final IQLParseException IQLParseException = (IQLParseException) e;
                clause = IQLParseException.getClause();
                offset = IQLParseException.getOffsetInClause();
            }
            final String stackTrace = Throwables.getStackTraceAsString(Throwables.getRootCause(e));
            final ErrorResult error = new ErrorResult(e.getClass().getSimpleName(), e.getMessage(), stackTrace, clause, offset);
            resp.setContentType("application/json");
            final ObjectMapper jsonMapper = new ObjectMapper();
            final ServletOutputStream outputStream = resp.getOutputStream();
            jsonMapper.writerWithDefaultPrettyPrinter().writeValue(outputStream, error);
            outputStream.close();
        }
    }

    // Logging code below

    // trying to not cause the logentry to overflow from being larger than 2^16
    // this is the pre URL-encoded limit and encoding can make it about twice longer
    private static final int QUERY_LENGTH_LIMIT = 27000;

    private void logQuery(HttpServletRequest req,
                          String query,
                          String userName,
                          long queryStartTimestamp,
                          IQLStatement parsedQuery,
                          SelectExecutionStats selectExecutionStats,
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
        logEntry.setProperty("hostname", hostname);
        logEntry.setProperty("starttime", Long.toString(queryStartTimestamp));
        logEntry.setProperty("tottime", (int)timeTaken);

        final List<String> params = Lists.newArrayList();
        final Enumeration<String> paramsEnum = req.getParameterNames();
        while(paramsEnum.hasMoreElements()) {
            final String param = paramsEnum.nextElement();
            if(USED_PARAMS.contains(param)) {
                params.add(param);
            }
        }
        logEntry.setProperty("params", Joiner.on(' ').join(params));
        final String queryToLog = query.length() > QUERY_LENGTH_LIMIT ? query.substring(0, QUERY_LENGTH_LIMIT) : query;
        logEntry.setProperty("q", queryToLog);
        logEntry.setProperty("qlen", query.length());
        logEntry.setProperty("error", errorOccurred != null ? "1" : "0");
        if(errorOccurred != null) {
            logEntry.setProperty("exceptiontype", errorOccurred.getClass().getSimpleName());
            logEntry.setProperty("exceptionmsg", errorOccurred.getMessage());
        }

        final String queryType = logStatementData(parsedQuery, selectExecutionStats, logEntry);
        logEntry.setProperty("statement", queryType);

        for (Map.Entry<String, Long> phase: selectExecutionStats.phases.entrySet()) {
            logEntry.setProperty(phase.getKey(), phase.getValue());
        }

        dataLog.info(logEntry);
    }

    // Log to logrepo
    private String logStatementData(IQLStatement parsedQuery,
                                    SelectExecutionStats selectExecutionStats,
                                    QueryLogEntry logEntry) {
        if(parsedQuery == null) {
            return "invalid";
        }
        final String queryType;
        if(parsedQuery instanceof SelectStatement) {
            queryType = "select";
            logSelectStatementData((SelectStatement) parsedQuery, selectExecutionStats, logEntry);
        } else if(parsedQuery instanceof DescribeStatement) {
            queryType = "describe";
            final DescribeStatement describeStatement = (DescribeStatement) parsedQuery;
            logEntry.setProperty("dataset", describeStatement.dataset);
            if(describeStatement.field != null) {
                logEntry.setProperty("field", describeStatement.field);
            }
        } else if(parsedQuery instanceof ShowStatement) {
            queryType = "show";
        } else {
            queryType = "invalid";
        }
        return queryType;
    }

    private void logSelectStatementData(SelectStatement selectStatement,
                                        SelectExecutionStats selectExecutionStats,
                                        QueryLogEntry logEntry) {
        final FromClause from = selectStatement.from;
        final GroupByClause groupBy = selectStatement.groupBy;
        final SelectClause select =  selectStatement.select;

        if(from != null) {
            logEntry.setProperty("dataset", from.getDataset());

            if(from.getStart() != null && from.getEnd() != null) {
                logEntry.setProperty("days", new Duration(from.getStart(), from.getEnd()).getStandardDays());
            }
        }

        final int selectCount;
        if(select != null && select.getProjections() != null) {
            selectCount = select.getProjections().size();
        } else {
            selectCount = 0;
        }
        logEntry.setProperty("selectcnt", selectCount);

        final int groupByCount;
        if(groupBy != null && groupBy.groupings != null) {
            groupByCount = groupBy.groupings.size();
        } else {
            groupByCount = 0;
        }
        logEntry.setProperty("groupbycnt", groupByCount);

        logEntry.setProperty("sessionid", selectExecutionStats.sessionId);
        logEntry.setProperty("cached", selectExecutionStats.cached ? "1" : "0");
        logEntry.setProperty("rows", selectExecutionStats.rowsWritten);
        logEntry.setProperty("shards", selectExecutionStats.shardCount);
        logEntry.setProperty("disk", selectExecutionStats.overflowedToDisk ? "1" : "0");
        logEntry.setProperty("hash", selectExecutionStats.hashForCaching);
        logEntry.setProperty("head", selectExecutionStats.headOnly ? "1" : "0");
        logEntry.setProperty("maxgroups", selectExecutionStats.maxImhotepGroups);
        // convert bytes to megabytes
        logEntry.setProperty("ftgsmb", selectExecutionStats.imhotepTempFilesBytesWritten / 1024 / 1024);
    }

    private void logQueryToLog4J(String query, String identification, long timeTaken) {
        if(query.length() > 500) {
            query = query.replaceAll("\\(([^\\)]{0,100}+)[^\\)]+\\)", "\\($1\\.\\.\\.\\)");
        }
        final String timeTakenStr = timeTaken >= 0 ? String.valueOf(timeTaken) : "";
        log.info((timeTaken < 0 ? "+" : "-") + identification + "\t" + timeTakenStr + "\t" + query);
    }

    public static class IdentificationRequiredException extends RuntimeException {
        public IdentificationRequiredException(String message) {
            super(message);
        }
    }

    private class SelectRequestArgs {
        public final boolean avoidFileSave;
        public final boolean asynchronous;
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

        public SelectRequestArgs(HttpServletRequest req, String userName) {
            asynchronous = req.getParameter("async") != null;
            avoidFileSave = req.getParameter("view") != null && !this.asynchronous;
            csv = req.getParameter("csv") != null;
            interactive = req.getParameter("interactive") != null;
            returnShardlist = req.getParameter("getshardlist") != null;
            returnNewestShardVersion = req.getParameter("getversion") != null;
            cacheReadDisabled = !queryCache.isEnabled() || req.getParameter("nocacheread") != null || req.getParameter("nocache") != null;
            cacheWriteDisabled = !queryCache.isEnabled() || req.getParameter("nocachewrite") != null || req.getParameter("nocache") != null;
            headOnly = "HEAD".equals(req.getMethod()) || req.getParameter("head") != null;
            progress = req.getParameter("progress") != null;
            getTotals = req.getParameter("totals") != null;
            final String clientName = Strings.nullToEmpty(req.getParameter("client"));
            imhotepUserName = "IQL:" + (!Strings.isNullOrEmpty(userName) ? userName : clientName);
            requestURL = req.getRequestURL().toString();
        }
    }
}
