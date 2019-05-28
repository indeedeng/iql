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

import au.com.bytecode.opencsv.CSVParser;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.DynamicIndexSubshardDirnameUtil;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.StrictCloser;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.exceptions.UserSessionCountLimitExceededException;
import com.indeed.iql.cache.CompletableOutputStream;
import com.indeed.iql.cache.QueryCache;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.io.TruncatingBufferedOutputStream;
import com.indeed.iql.metadata.DatasetMetadata;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql.metadata.FieldType;
import com.indeed.iql.web.AccessControl;
import com.indeed.iql.web.ClientInfo;
import com.indeed.iql.web.Limits;
import com.indeed.iql.web.QueryInfo;
import com.indeed.iql.web.QueryMetadata;
import com.indeed.iql.web.QueryServlet;
import com.indeed.iql.web.RunningQueriesManager;
import com.indeed.iql.web.SelectQuery;
import com.indeed.iql2.execution.QueryOptions;
import com.indeed.iql2.execution.ResultFormat;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.progress.CompositeProgressCallback;
import com.indeed.iql2.execution.progress.ProgressCallback;
import com.indeed.iql2.execution.progress.SessionOpenedOnlyProgressCallback;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateFilters;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.Term;
import com.indeed.iql2.language.cachekeys.CacheKey;
import com.indeed.iql2.language.query.Dataset;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.query.shardresolution.CachingShardResolver;
import com.indeed.iql2.language.query.shardresolution.ImhotepClientShardResolver;
import com.indeed.iql2.language.query.shardresolution.ShardResolver;
import com.indeed.iql2.language.util.FieldExtractor;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.core.time.WallClock;
import com.indeed.util.logging.TracingTreeTimer;
import org.antlr.v4.runtime.CharStream;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SelectQueryExecution {
    private static final Logger log = Logger.getLogger(SelectQueryExecution.class);

    @VisibleForTesting
    public static final String METADATA_FILE_SUFFIX = ".meta";

    // System configuration
    @Nullable
    private final File tmpDir;
    private final AccessControl accessControl;

    // IQL2 server systems
    private final QueryCache queryCache;
    private final QueryMetadata queryMetadata;
    private final ExecutorService cacheUploadExecutorService;
    private final Set<String> defaultIQL2Options;

    // Query sanity limits
    private final Limits limits;

    // IQL2 Imhotep-based state
    private final ImhotepClient imhotepClient;
    private final ShardResolver shardResolver;

    private final DatasetsMetadata datasetsMetadata;

    @Nullable
    private final Long maxCachedQuerySizeLimitBytes;
    // Query output state
    private final PrintWriter outputStream;
    private final QueryInfo queryInfo;
    private final TracingTreeTimer timer;
    private final ClientInfo clientInfo;

    // Query inputs
    private final String query;
    private final boolean headOnly;
    private final int version;
    private final boolean isStream;
    private final boolean returnNewestShardVersionHeader;
    private final boolean skipValidation;
    private final ResultFormat resultFormat;
    private final boolean getTotals;
    private final WallClock clock;

    private boolean ran = false;

    public SelectQueryExecution(
            @Nullable final File tmpDir,
            final QueryCache queryCache,
            final Limits limits,
            @Nullable final Long maxCachedQuerySizeLimitBytes,
            final ImhotepClient imhotepClient,
            final DatasetsMetadata datasetsMetadata,
            final PrintWriter outputStream,
            final QueryInfo queryInfo,
            final ClientInfo clientInfo,
            final TracingTreeTimer timer,
            final String query,
            final boolean headOnly,
            final int version,
            final boolean isStream,
            final boolean returnNewestShardVersionHeader,
            final boolean skipValidation,
            final boolean getTotals,
            final boolean csv,
            final WallClock clock,
            final QueryMetadata queryMetadata,
            final ExecutorService cacheUploadExecutorService,
            final ImmutableSet<String> defaultIQL2Options,
            final AccessControl accessControl) {
        this.outputStream = outputStream;
        this.queryInfo = queryInfo;
        this.clientInfo = clientInfo;
        this.query = query;
        this.headOnly = headOnly;
        this.version = version;
        this.timer = timer;
        this.isStream = isStream;
        this.limits = limits;
        this.maxCachedQuerySizeLimitBytes = maxCachedQuerySizeLimitBytes;
        this.returnNewestShardVersionHeader = returnNewestShardVersionHeader;
        this.skipValidation = skipValidation;
        this.resultFormat = csv ? ResultFormat.CSV : ResultFormat.TSV;
        this.getTotals = getTotals;
        this.clock = clock;
        this.imhotepClient = imhotepClient;
        this.datasetsMetadata = datasetsMetadata;
        this.queryCache = queryCache;
        this.queryMetadata = queryMetadata;
        this.cacheUploadExecutorService = cacheUploadExecutorService;
        this.defaultIQL2Options = defaultIQL2Options;
        this.tmpDir = tmpDir;
        this.accessControl = accessControl;
        this.shardResolver = new CachingShardResolver(new ImhotepClientShardResolver(imhotepClient));

        Preconditions.checkArgument(!(csv && isStream), "Cannot use csv and event-stream output formats at the same time");
    }

    public void processSelect(final RunningQueriesManager runningQueriesManager) throws IOException {
        // .. just in case.
        synchronized (this) {
            if (ran) {
                throw new IllegalArgumentException("Cannot run multiple times!");
            }
            ran = true;
        }

        if (isStream) {
            outputStream.println(": This is the start of the IQL Query Stream");
            outputStream.println();
        }

        final Consumer<String> out = new Consumer<String>() {
            int count = 1;

            @Override
            public void accept(final String s) {
                if (isStream) {
                    outputStream.print("data: ");
                }
                outputStream.println(s);

                count += 1;
                // Only check for errors every 10k rows because checkError
                // also flushes, which we do not want to do every row.
                if (((count % 10000) == 0) && outputStream.checkError()) {
                    throw new IqlKnownException.ClientHungUpException("OutputStream in error state");
                }
            }
        };

        final EventStreamProgressCallback eventStreamProgressCallback = new EventStreamProgressCallback(isStream, outputStream);
        ProgressCallback progressCallback;

        //Check query document count limit
        Integer numDocLimitBillion = limits.queryDocumentCountLimitBillions;
        NumDocLimitingProgressCallback numDocLimitingProgressCallback;
        if (numDocLimitBillion != null) {
            numDocLimitingProgressCallback = new NumDocLimitingProgressCallback(numDocLimitBillion * 1_000_000_000L);
            progressCallback = CompositeProgressCallback.create(numDocLimitingProgressCallback, eventStreamProgressCallback);
        } else {
            progressCallback = CompositeProgressCallback.create(eventStreamProgressCallback);
        }

        executeSelect(runningQueriesManager, version == 1, out, progressCallback);
    }

    private void extractCompletedQueryInfoData(SelectExecutionInformation execInfo, Set<String> warnings, final int rows) {
        int shardCount = 0;
        Duration totalShardPeriod = Duration.ZERO;
        final Set<String> hostHashSet = Sets.newHashSet();
        for (final List<Shard> shardList : execInfo.datasetToShards.values()) {
            shardCount += shardList.size();
            for (final Shard shardInfo : shardList) {
                hostHashSet.add(shardInfo.getServer().toString());
                ShardInfo.DateTimeRange range = shardInfo.getRange();
                totalShardPeriod = totalShardPeriod.plus(new Duration(range.start, range.end));
            }
        }
        queryInfo.numShards = shardCount;
        queryInfo.imhotepServers = hostHashSet;
        queryInfo.numImhotepServers = hostHashSet.size();
        queryInfo.totalShardPeriodHours = totalShardPeriod.toStandardHours().getHours();
        queryInfo.cached = execInfo.cached;
        queryInfo.ftgsMB = execInfo.imhotepTempBytesWritten / 1024 / 1024;
        queryInfo.sessionIDs = execInfo.sessionIds;
        queryInfo.numDocs = execInfo.totalNumDocs;
        queryInfo.rows = rows;
        queryInfo.maxGroups = execInfo.maxNumGroups;
        queryInfo.maxConcurrentSessions = execInfo.maxConcurrentSessions;

        queryInfo.resultBytes = execInfo.resultBytes;
        queryInfo.cacheUploadSkipped = execInfo.cacheUploadSkipped;

        queryInfo.setFromPerformanceStats(execInfo.imhotepPerformanceStats);

        if (execInfo.hasMoreRows) {
            warnings.add(String.format("Only first %d rows returned sorted on the last group by column", queryInfo.rows));
        }
    }

    private SelectExecutionInformation executeSelect(
            final RunningQueriesManager runningQueriesManager,
            final boolean useLegacy,
            final Consumer<String> out,
            final ProgressCallback progressCallback
    ) throws IOException {
        final Set<String> warnings = new HashSet<>();
        timer.push("Select query execution");

        timer.push("parse query");
        final Queries.ParseResult parseResult = Queries.parseQuery(query, useLegacy, datasetsMetadata, defaultIQL2Options, warnings::add, clock, timer, shardResolver);
        final Query query = parseResult.query;
        timer.pop();

        if (!skipValidation) {
            timer.push("validate query");
            final Set<String> errors = new HashSet<>();
            CommandValidator.validate(query, limits, datasetsMetadata, new ErrorCollector(errors, warnings));
            if (!errors.isEmpty()) {
                throw new IqlKnownException.ParseErrorException("Errors found when validating query: " + errors);
            }
            timer.pop();
        }

        if (query.options.contains(QueryOptions.PARANOID)) {
            timer.push("reparse query (paranoid mode)");
            final Query paranoidQuery = Queries.parseQuery(this.query, useLegacy, datasetsMetadata, defaultIQL2Options, x -> {}, clock, timer, shardResolver).query;
            timer.pop();

            timer.push("check query equals() and hashCode() (paranoid mode)");
            if (!paranoidQuery.equals(query)) {
                log.error("parseResult.query = " + query);
                log.error("paranoidQuery = " + paranoidQuery);
                throw new IllegalStateException("Paranoid mode encountered re-parsed query equals() failure!");
            }
            if (paranoidQuery.hashCode() != query.hashCode()) {
                log.error("parseResult.query = " + query);
                log.error("paranoidQuery = " + paranoidQuery);
                throw new IllegalStateException("Paranoid mode encountered re-parsed query hashCode() failure!");
            }
            timer.pop();

            timer.push("extractHeaders (paranoid mode)");
            Queries.extractHeaders(query);
            timer.pop();
        }

        {
            queryInfo.statementType = "select";

            final List<Dataset> allDatasets = Queries.findAllDatasets(query);
            Duration datasetRangeSum = Duration.ZERO;
            queryInfo.datasets = new HashSet<>();
            for (final Dataset dataset : allDatasets) {
                final String actualDataset = dataset.dataset.unwrap();
                if (actualDataset == null) {
                    continue;
                }
                accessControl.checkAllowedDatasetAccess(clientInfo.username, actualDataset);
                final Optional<DatasetMetadata> metadata = datasetsMetadata.getMetadata(actualDataset);
                if (metadata.isPresent() && metadata.get().isDeprecatedOrDescriptionDeprecated()) {
                    warnings.add("Dataset '" + actualDataset + "' is deprecated. Check the dataset description for alternative data sources.");
                }
                queryInfo.datasets.add(actualDataset);
                datasetRangeSum = datasetRangeSum.plus(new Duration(dataset.startInclusive.unwrap(), dataset.endExclusive.unwrap()));
            }
            queryInfo.totalDatasetRangeDays = datasetRangeSum.toStandardDays().getDays();

            final Set<FieldExtractor.DatasetField> datasetFields = FieldExtractor.getDatasetFields(query);
            queryInfo.datasetFields = Sets.newHashSet();
            queryInfo.datasetFieldsNoDescription = Sets.newHashSet();

            for (final FieldExtractor.DatasetField datasetField : datasetFields) {
                final DatasetInfo datasetInfo = imhotepClient.getDatasetToDatasetInfo().get(datasetField.dataset);
                final Collection<String> intFields = datasetInfo.getIntFields();
                final Collection<String> stringFields = datasetInfo.getStringFields();
                String field = intFields.stream().filter(intField -> intField.compareToIgnoreCase(datasetField.field) == 0).findFirst().orElse(null);
                if (field == null) {
                    field = stringFields.stream().filter(stringField -> stringField.compareToIgnoreCase(datasetField.field) == 0).findFirst().orElse(null);
                }
                if (field != null) {
                    queryInfo.datasetFields.add(datasetField.dataset + "." + field);
                    if (!datasetsMetadata.fieldHasDescription(datasetField.dataset, field, useLegacy)) {
                        queryInfo.datasetFieldsNoDescription.add(datasetField.dataset + "." + field);
                    }
                }
            }
        }

        final int sessions = query.datasets.size();
        if (!limits.satisfiesConcurrentImhotepSessionsLimit(sessions)) {
            throw new UserSessionCountLimitExceededException("User is creating more concurrent imhotep sessions than the limit: " + limits.concurrentImhotepSessionsLimit);
        }

        final Set<String> conflictFieldsUsed = Sets.intersection(queryInfo.datasetFields, datasetsMetadata.getTypeConflictDatasetFieldNames());
        if (!conflictFieldsUsed.isEmpty()) {
            final String conflictWarning = "Fields with type conflicts used in query: " + String.join(", ", conflictFieldsUsed);
            warnings.add(conflictWarning);
        }

        final ListMultimap<String, List<Shard>> allShardsUsed = query.allShardsUsed();
        final List<DatasetWithMissingShards> datasetsWithMissingShards = query.allMissingShards();
        queryMetadata.addItem("IQL-Newest-Shard", ISODateTimeFormat.dateTime().print(newestStaticShard(allShardsUsed).orElse(-1L)), returnNewestShardVersionHeader);
        for (DatasetWithMissingShards datasetWithMissingShards : datasetsWithMissingShards) {
            warnings.addAll(QueryServlet.missingShardsToWarnings(datasetWithMissingShards.dataset,
                    datasetWithMissingShards.start, datasetWithMissingShards.end, datasetWithMissingShards.timeIntervalsMissingShards));
        }

        final List<Interval> missingIntervals = datasetsWithMissingShards.stream()
                .flatMap(x -> x.getTimeIntervalsMissingShards().stream())
                .collect(Collectors.toList());
        if (!missingIntervals.isEmpty()) {
            final String missingIntervalsString = QueryServlet.intervalListToString(missingIntervals);
            queryMetadata.addItem("IQL-Missing-Shards", missingIntervalsString, true);
        }

        timer.push("compute hashes");
        queryInfo.cacheHashes = query.allCacheKeys(resultFormat);
        timer.pop();

        final StrictCloser strictCloser = new StrictCloser();
        // SelectQuery can be closed after all cache has been uploaded
        final SharedReference<SelectQuery> selectQuery = SharedReference.create(
                new SelectQuery(queryInfo, runningQueriesManager, this.query, clientInfo, limits, new DateTime(queryInfo.queryStartTimestamp),
                        null, (byte) sessions, queryMetadata, strictCloser, progressCallback)
        );
        try {
            final ParsedQueryExecution pqe = new ParsedQueryExecution(
                    true, parseResult.inputStream, out, warnings, resultFormat, progressCallback,
                    query, limits.queryInMemoryRowsLimit, selectQuery, strictCloser
            );

            final SelectExecutionInformation cacheReadResult = pqe.tryHeadAndCacheRead();
            if (cacheReadResult != null) {
                return cacheReadResult;
            }

            timer.push("Acquire concurrent query lock");
            selectQuery.get().lock();
            timer.pop();
            queryInfo.queryStartTimestamp = selectQuery.get().getQueryStartTimestamp().getMillis();
            return pqe.executeParsedQuery();
        } catch (final Exception e) {
            selectQuery.get().checkCancelled();
            throw e;
        } finally {
            Closeables2.closeQuietly(selectQuery, log);
        }
    }

    private class ParsedQueryExecution {
        private final boolean isTopLevelQuery;
        private final CharStream inputStream;
        private final Consumer<String> externalOutput;
        private final Set<String> warnings;
        private final ResultFormat resultFormat;

        private final CacheKey cacheKey;
        private final int groupLimit;
        private final SharedReference<SelectQuery> selectQuery;
        private final StrictCloser strictCloser;

        private final Query query;

        private final ProgressCallback progressCallback;

        // Indicates if the cache has been checked yet or not
        private boolean cacheChecked = false;

        private boolean cacheEnabled; // state computed by the cache check potentially for later use

        private ParsedQueryExecution(
                final boolean isTopLevelQuery,
                final CharStream inputStream,
                final Consumer<String> out,
                final Set<String> warnings,
                final ResultFormat resultFormat,
                final ProgressCallback progressCallback,
                final Query query,
                @Nullable final Integer groupLimit,
                final SharedReference<SelectQuery> selectQuery,
                final StrictCloser strictCloser) {
            this.isTopLevelQuery = isTopLevelQuery;
            this.inputStream = inputStream;
            this.externalOutput = out;
            this.warnings = warnings;
            this.resultFormat = resultFormat;
            this.progressCallback = progressCallback;
            this.query = query;
            this.cacheKey = query.cacheKey(resultFormat);
            this.groupLimit = groupLimit == null ? 1000000 : groupLimit;
            this.selectQuery = selectQuery;
            this.strictCloser = strictCloser;
        }

        /**
         * If in headOnly mode, finish execution and return the HEAD result.
         * Otherwise, if cache enabled and query is cached, finish execution and return the cached result.
         * Otherwise, return null.
         * <p>
         * Work that is not heavy and does not use Imhotep goes into this method.
         *
         * @return non-null execution information if query execution finished. null otherwise.
         */
        @Nullable
        private SelectExecutionInformation tryHeadAndCacheRead() throws IOException {
            final boolean skipCache = query.options.contains(QueryOptions.NO_CACHE);
            cacheEnabled = queryCache.isEnabled() && !skipCache;

            final InputStream cacheInputStream;
            if (cacheEnabled) {
                timer.push("cache check");
                cacheInputStream = strictCloser.registerOrClose(queryCache.getInputStream(cacheKey.cacheFileName));
                timer.pop();
            } else {
                cacheInputStream = null;
            }

            final boolean isCached = cacheInputStream != null;

            if (headOnly) {
                final SelectExecutionInformation selectExecutionInformation = new SelectExecutionInformation(
                        query.allShardsUsed(),
                        query.allMissingShards(),
                        isCached,
                        0L,
                        null,
                        Collections.emptyList(),
                        query.totalNumDocs(),
                        0,
                        0,
                        false,
                        null,
                        null
                );

                finalizeQueryExecution(selectExecutionInformation, 0);

                return selectExecutionInformation;
            }

            if (cacheEnabled && isCached) {
                timer.push("read cache");
                if (isTopLevelQuery) {
                    queryMetadata.addItem("IQL-Cached", true, true);
                    // read metadata from cache
                    try {
                        final InputStream metadataCacheStream = queryCache.getInputStream(cacheKey.cacheFileName + METADATA_FILE_SUFFIX);
                        if (metadataCacheStream != null) {
                            final QueryMetadata cachedMetadata = QueryMetadata.fromStream(metadataCacheStream);
                            queryMetadata.mergeIn(cachedMetadata);
                            queryMetadata.renameItem("IQL-Query-Info", "IQL-Cached-Query-Info");
                        }
                    } catch (final Exception e) {
                        log.warn("Failed to load metadata cache from " + cacheKey.cacheFileName + METADATA_FILE_SUFFIX, e);
                    }
                    queryMetadata.setPendingHeaders();
                }

                final CountingConsumer<String> countingExternalOutput = new CountingConsumer<>(externalOutput);

                // TODO: Don't have this hack
                progressCallback.startCommand(null, null, true);
                final boolean hasMoreRows = sendCachedQuery(countingExternalOutput, query.rowLimit, cacheInputStream);
                timer.pop();
                final SelectExecutionInformation selectExecutionInformation = new SelectExecutionInformation(query.allShardsUsed(), query.allMissingShards(),
                        true, 0L, null,
                        Collections.emptyList(), 0, 0, 0, hasMoreRows, null, null);

                finalizeQueryExecution(selectExecutionInformation, countingExternalOutput.getCount());
                return selectExecutionInformation;
            }

            cacheChecked = true;

            return null;
        }

        /**
         * Work that is heavy and/or uses Imhotep goes into this method.
         */
        private SelectExecutionInformation executeParsedQuery() throws IOException {
            if (!cacheChecked) {
                final SelectExecutionInformation cachedResult = tryHeadAndCacheRead();
                if (cachedResult != null) {
                    return cachedResult;
                }
            }

            final int[] totalBytesWritten = {0};

            final CountingConsumer<String> countingExternalOutput = new CountingConsumer<>(externalOutput);
            Consumer<String> out = countingExternalOutput;

            final File cacheFile;
            final TruncatingBufferedOutputStream cacheWriter;

            try (final StrictCloser innerStrictCloser = new StrictCloser()) {
                strictCloser.registerOrClose(innerStrictCloser);
                if (cacheEnabled) {
                    final Consumer<String> oldOut = out;
                    cacheFile = File.createTempFile("query", ".cache.tmp", tmpDir);
                    cacheWriter = new TruncatingBufferedOutputStream(new FileOutputStream(cacheFile), maxCachedQuerySizeLimitBytes);

                    out = s -> {
                        oldOut.accept(s);
                        try {
                            if (!cacheWriter.isOverflowed()) {
                                cacheWriter.write(s.getBytes(Charsets.UTF_8));
                                cacheWriter.write('\n');
                            }
                        } catch (IOException e) {
                            throw Throwables.propagate(e);
                        }
                    };
                } else {
                    cacheFile = null;
                    cacheWriter = null;
                }
                if (isTopLevelQuery) {
                    queryMetadata.addItem("IQL-Cached", "false", true);
                    queryMetadata.setPendingHeaders();
                }
                final AtomicBoolean hasMoreRows = new AtomicBoolean(false);
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
                            } else if (rowsWritten == rowLimit) {
                                hasMoreRows.set(true);
                            }
                        }
                    };
                }

                final Map<Query, Set<Term>> queryToResults = new HashMap<>();
                final Query substitutedQuery = executeAndSubstituteSubQueries(query, totalBytesWritten, queryToResults);

                if (query.options.contains(QueryOptions.PARANOID)) {
                    timer.push("re-validate substituted query (paranoid mode)");
                    final HashSet<String> errors = new HashSet<>();
                    CommandValidator.validate(substitutedQuery, limits, datasetsMetadata, new ErrorCollector(errors, warnings));
                    if (!errors.isEmpty()) {
                        throw new IqlKnownException.ParseErrorException("Errors found when (re-)validating query: " + errors);
                    }
                    timer.pop();
                }

                final List<Queries.QueryDataset> datasets = Queries.createDatasetMap(substitutedQuery);

                final InfoCollectingProgressCallback infoCollectingProgressCallback = new InfoCollectingProgressCallback();
                final ProgressCallback compositeProgressCallback = CompositeProgressCallback.create(progressCallback, infoCollectingProgressCallback);
                try {
                    final Session.CreateSessionResult createResult = Session.createSession(
                            imhotepClient,
                            groupLimit,
                            Sets.newHashSet(query.options),
                            substitutedQuery.commands(),
                            substitutedQuery.getTotals(),
                            datasets,
                            innerStrictCloser,
                            out,
                            timer,
                            compositeProgressCallback,
                            mbToBytes(limits.queryFTGSIQLLimitMB),
                            mbToBytes(limits.queryFTGSImhotepDaemonLimitMB),
                            limits.priority,
                            clientInfo.username,
                            (version == 2) ? FieldType.Integer : FieldType.String,
                            resultFormat,
                            version
                    );

                    final SelectExecutionInformation selectExecutionInformation = new SelectExecutionInformation(
                            query.allShardsUsed(),
                            query.allMissingShards(),
                            false,
                            createResult.tempFileBytesWritten + totalBytesWritten[0],
                            createResult.imhotepPerformanceStats,
                            infoCollectingProgressCallback.getSessionIds(),
                            query.totalNumDocs(),
                            infoCollectingProgressCallback.getMaxNumGroups(),
                            query.maxConcurrentSessions(),
                            hasMoreRows.get(),
                            (cacheWriter != null) ? cacheWriter.getAttemptedTotalWriteBytes() : null,
                            (cacheWriter != null) ? cacheWriter.isOverflowed() : null
                    );

                    if (createResult.totals.isPresent()) {
                        queryMetadata.addItem("IQL-Totals", Arrays.toString(createResult.totals.get()), getTotals);
                    }

                    finalizeQueryExecution(selectExecutionInformation, countingExternalOutput.getCount());

                    if (cacheEnabled) {
                        // Cache upload
                        final SharedReference<SelectQuery> selectQueryRef = selectQuery.copy(); // going to be closed asynchronously after cache is uploaded
                        cacheUploadExecutorService.submit(new Callable<Void>() {
                            @Override
                            public Void call() {
                                try {
                                    if (cacheWriter.isOverflowed()) {
                                        // If the results were too big, we do not want to write to the cache.
                                        log.warn("Skipping cache upload due to overflow");
                                        Closeables2.closeQuietly(cacheWriter, log);
                                    } else {
                                        final String cacheFileName = cacheKey.cacheFileName;
                                        if (isTopLevelQuery) {
                                            try {
                                                final CompletableOutputStream metadataCacheStream = queryCache.getOutputStream(cacheFileName + METADATA_FILE_SUFFIX);
                                                queryMetadata.toOutputStream(metadataCacheStream);
                                            } catch (Exception e) {
                                                log.warn("Failed to upload metadata cache: " + cacheFileName, e);
                                            }
                                        }
                                        try {
                                            cacheWriter.close();
                                            queryCache.writeFromFile(cacheFileName, cacheFile);
                                        } catch (Exception e) {
                                            log.warn("Failed to upload cache: " + cacheFileName, e);
                                        }
                                    }
                                } finally {
                                    Closeables2.closeQuietly(selectQueryRef, log);
                                    if (!cacheFile.delete()) {
                                        log.warn("Failed to delete " + cacheFile);
                                    }
                                }
                                return null;
                            }
                        });
                    }

                    return selectExecutionInformation;
                } catch (final Exception e) {
                    if (cacheFile != null) {
                        if (!cacheFile.delete()) {
                            log.info("Failed to delete: " + cacheFile.getPath());
                        }
                    }
                    throw Throwables.propagate(e);
                }
            }
        }

        private Query executeAndSubstituteSubQueries(final Query query, final int[] totalBytesWritten, final Map<Query, Set<Term>> queryToResults) {
            timer.push("execute subqueries");
            final Query result = query.transform(
                    new Function<GroupBy, GroupBy>() {
                        @Nullable
                        @Override
                        public GroupBy apply(final GroupBy input) {
                            if (input instanceof GroupBy.GroupByFieldInQuery) {
                                final GroupBy.GroupByFieldInQuery fieldInQuery = (GroupBy.GroupByFieldInQuery) input;
                                final Query q = fieldInQuery.query;
                                if (!queryToResults.containsKey(q)) {
                                    final Set<Term> subqueryResult =
                                            executeSubquery(q, totalBytesWritten);
                                    queryToResults.put(q, subqueryResult);
                                }
                                final Set<Term> terms = queryToResults.get(q);
                                if (fieldInQuery.isNegated) {
                                    // if negated then we have to iterate all terms and filter out result of subquery.
                                    final Optional<AggregateFilter> filter;
                                    if (terms.isEmpty()) {
                                        filter = Optional.empty();
                                    } else {
                                        filter = Optional.of(AggregateFilters.aggregateInHelper(terms.iterator(), true));
                                    }
                                    return new GroupBy.GroupByField(fieldInQuery.field, filter, Optional.empty(), fieldInQuery.withDefault);
                                } else {
                                    // if not-negated then we do group by field in terms-set.
                                    if (terms.isEmpty()) {
                                        // TODO: error here or warning?
                                    }
                                    final ImmutableSet<Term> immutableTerms = ImmutableSet.copyOf(terms);
                                    // Once terms are converted to immutable, store it to cache to prevent convertion next time.
                                    queryToResults.put(q, immutableTerms);
                                    return new GroupBy.GroupByFieldIn(fieldInQuery.field, immutableTerms, fieldInQuery.withDefault, false);
                                }
                            }
                            return input;
                        }
                    },
                    Function.identity(),
                    Function.identity(),
                    Function.identity(),
                    new Function<DocFilter, DocFilter>() {
                        @Nullable
                        @Override
                        public DocFilter apply(final DocFilter input) {
                            if (input instanceof DocFilter.FieldInQuery) {
                                final DocFilter.FieldInQuery fieldInQuery = (DocFilter.FieldInQuery) input;
                                final Query q = fieldInQuery.query;
                                if (!queryToResults.containsKey(q)) {
                                    final Set<Term> subqueryResult =
                                            executeSubquery(q, totalBytesWritten);
                                    queryToResults.put(q, subqueryResult);
                                }
                                final Set<Term> terms = queryToResults.get(q);
                                final FieldSet field = fieldInQuery.field;

                                final DocFilter maybeNegated;
                                final DocFilter filter = DocFilter.FieldInTermsSet.create(field, ImmutableSet.copyOf(terms));
                                if (fieldInQuery.isNegated) {
                                    maybeNegated = new DocFilter.Not(filter);
                                } else {
                                    maybeNegated = filter;
                                }
                                return maybeNegated;
                            }
                            return input;
                        }
                    }
            );
            timer.pop();
            return result;
        }

        private Set<Term> executeSubquery(
                final Query q,
                final int[] totalBytesWritten) {
            final Set<Term> terms = new HashSet<>();
            timer.push("Execute sub-query", "Execute sub-query: \"" + QueryInfo.truncateQuery(q.getRawInput()) + "\"");
            try {
                final CSVParser csvParser = new CSVParser(',', '"', '\0');
                // TODO: This use of ProgressCallbacks looks wrong.
                final SelectExecutionInformation execInfo = new ParsedQueryExecution(false, inputStream, new Consumer<String>() {
                    @Override
                    public void accept(final String s) {
                        if ((limits.queryInMemoryRowsLimit > 0) && (terms.size() >= limits.queryInMemoryRowsLimit)) {
                            throw new IqlKnownException.GroupLimitExceededException("Sub query cannot have more than [" + limits.queryInMemoryRowsLimit + "] terms!");
                        }
                        final String term;
                        try {
                            term = csvParser.parseLineMulti(s)[0];
                        } catch (final IOException e) {
                            throw Throwables.propagate(e);
                        }
                        terms.add(Term.term(term));
                    }
                }, warnings, ResultFormat.CSV, new SessionOpenedOnlyProgressCallback(progressCallback), q, groupLimit, selectQuery, strictCloser).executeParsedQuery();
                totalBytesWritten[0] += execInfo.imhotepTempBytesWritten;
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }

            timer.pop();
            return terms;
        }

        private void finalizeQueryExecution(SelectExecutionInformation selectExecutionInformation, final int rowsWritten) {
            if (!isTopLevelQuery) {
                return;
            }
            extractCompletedQueryInfoData(selectExecutionInformation, warnings, rowsWritten);
            timer.pop(); // "Select query execution" from executeSelect
            queryInfo.totalTime = System.currentTimeMillis() - queryInfo.queryStartTimestamp;
            queryInfo.timingTreeReport = timer.toString();

            if (isStream) {
                outputStream.println();
                outputStream.println("event: header");

                if (!warnings.isEmpty()) {
                    String warning = "[\"" + StringUtils.join(warnings, "\",\"") + "\"]";
                    queryMetadata.addItem("IQL-Warning", warning, false);
                }
                queryMetadata.renameItem("IQL-Query-Info", "IQL-Cached-Query-Info");
                queryMetadata.addItem("IQL-Query-Info", queryInfo.toJSON(), false);

                outputStream.println("data: " + queryMetadata.toJSONForClients());
                outputStream.println();

                outputStream.println("event: complete");
                outputStream.println("data: :)");
                outputStream.println();
            }
            outputStream.close(); // only close on success because on error the stack trace is printed
        }
    }

    private static Long mbToBytes(Integer megabytes) {
        if (megabytes == null) {
            return 0L;
        }
        return megabytes <= 0 ? (long) megabytes : (long) megabytes * 1024 * 1024;
    }

    // increment query limit so that we know that whether it filters the response data size
    public static Query incrementQueryLimit(final Query query) {
        final Optional<Integer> newRowLimit = query.rowLimit.map(limit -> limit + 1);
        return new Query(query.datasets, query.filter, query.groupBys, query.selects, query.formatStrings, query.options, newRowLimit, query.useLegacy).copyPosition(query);
    }

    public static class DatasetWithMissingShards {
        public final String dataset;
        public final DateTime start;
        public final DateTime end;
        public final List<Interval> timeIntervalsMissingShards;

        public DatasetWithMissingShards(String dataset, DateTime start, DateTime end, List<Interval> timeIntervalsMissingShards) {
            this.dataset = dataset;
            this.start = start;
            this.end = end;
            this.timeIntervalsMissingShards = timeIntervalsMissingShards;
        }

        public List<Interval> getTimeIntervalsMissingShards() {
            return timeIntervalsMissingShards;
        }
    }

    private static boolean sendCachedQuery(Consumer<String> out, Optional<Integer> rowLimit, InputStream cacheInputStream) throws IOException {
        final int limit = rowLimit.orElse(Integer.MAX_VALUE);
        int rowsWritten = 0;
        boolean hasMoreRows = false;
        try (final BufferedReader stream = new BufferedReader(new InputStreamReader(cacheInputStream))) {
            String line;
            while ((line = stream.readLine()) != null) {
                out.accept(line);
                rowsWritten += 1;
                if (rowsWritten >= limit) {
                    hasMoreRows = (stream.readLine() == null);
                    break;
                }
            }
        }
        return hasMoreRows;
    }

    public static Optional<Long> newestStaticShard(Multimap<String, List<Shard>> datasetToShards) {
        long newestStatic = -1;
        for (final List<Shard> shardset : datasetToShards.values()) {
            for (final Shard shard : shardset) {
                if (!DynamicIndexSubshardDirnameUtil.isValidDynamicShardId(shard.getShardId())) {
                    newestStatic = Math.max(newestStatic, shard.getVersion());
                }
            }
        }
        if (newestStatic == -1) {
            return Optional.empty();
        } else {
            return Optional.of(DateTimeFormat.forPattern("yyyyMMddHHmmss").parseMillis(String.valueOf(newestStatic)));
        }
    }

    private static class SelectExecutionInformation {
        public final Multimap<String, List<Shard>> datasetToShards;
        public final List<DatasetWithMissingShards> datasetsWithMissingShards;
        public final boolean cached;
        public final long imhotepTempBytesWritten;
        @Nullable
        public final PerformanceStats imhotepPerformanceStats;

        public final Set<String> sessionIds;
        public final long totalNumDocs;
        public final int maxNumGroups;
        public final int maxConcurrentSessions;
        public final boolean hasMoreRows;

        @Nullable
        public final Long resultBytes;
        @Nullable
        public final Boolean cacheUploadSkipped;

        private SelectExecutionInformation(Multimap<String, List<Shard>> datasetToShards, List<DatasetWithMissingShards> datasetsWithMissingShards,
                                           final boolean cached, long imhotepTempBytesWritten, PerformanceStats imhotepPerformanceStats,
                                           List<String> sessionIds, long totalNumDocs, int maxNumGroups, int maxConcurrentSessions,
                                           final boolean hasMoreRows, @Nullable final Long resultBytes, @Nullable final Boolean cacheUploadSkipped) {
            this.datasetToShards = datasetToShards;
            this.datasetsWithMissingShards = datasetsWithMissingShards;
            this.cached = cached;
            this.imhotepTempBytesWritten = imhotepTempBytesWritten;
            this.imhotepPerformanceStats = imhotepPerformanceStats;
            this.sessionIds = ImmutableSet.copyOf(sessionIds);
            this.totalNumDocs = totalNumDocs;
            this.maxNumGroups = maxNumGroups;
            this.maxConcurrentSessions = maxConcurrentSessions;
            this.hasMoreRows = hasMoreRows;
            this.resultBytes = resultBytes;
            this.cacheUploadSkipped = cacheUploadSkipped;
        }
    }
}
