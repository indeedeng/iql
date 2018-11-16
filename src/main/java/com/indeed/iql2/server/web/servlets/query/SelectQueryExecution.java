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
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.DynamicIndexSubshardDirnameUtil;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.exceptions.UserSessionCountLimitExceededException;
import com.indeed.iql.StrictCloser;
import com.indeed.iql.cache.CompletableOutputStream;
import com.indeed.iql.cache.QueryCache;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql.web.ClientInfo;
import com.indeed.iql.web.Limits;
import com.indeed.iql.web.QueryInfo;
import com.indeed.iql.web.QueryMetadata;
import com.indeed.iql.web.QueryServlet;
import com.indeed.iql.web.RunningQueriesManager;
import com.indeed.iql.web.SelectQuery;
import com.indeed.iql2.execution.QueryOptions;
import com.indeed.iql2.execution.Session;
import com.indeed.iql2.execution.progress.CompositeProgressCallback;
import com.indeed.iql2.execution.progress.ProgressCallback;
import com.indeed.iql2.execution.progress.SessionOpenedOnlyProgressCallback;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.Positioned;
import com.indeed.iql2.language.ScopedField;
import com.indeed.iql2.language.commands.Command;
import com.indeed.iql2.language.query.Dataset;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.util.FieldExtractor;
import com.indeed.util.core.Pair;
import com.indeed.util.core.TreeTimer;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.time.WallClock;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.antlr.v4.runtime.CharStream;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class SelectQueryExecution {
    private static final Logger log = Logger.getLogger(SelectQueryExecution.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String METADATA_FILE_SUFFIX = ".meta";

    // System configuration
    @Nullable
    private final File tmpDir;

    // IQL2 server systems
    private final QueryCache queryCache;
    private final QueryMetadata queryMetadata;
    private final ExecutorService cacheUploadExecutorService;
    private final Set<String> defaultIQL2Options;

    // Query sanity limits
    public final Limits limits;

    // IQL2 Imhotep-based state
    private final ImhotepClient imhotepClient;

    private final DatasetsMetadata datasetsMetadata;

    // Query output state
    private final PrintWriter outputStream;
    private final QueryInfo queryInfo;
    private final TreeTimer timer;
    public final ClientInfo clientInfo;

    // Query inputs
    public final String query;
    public final int version;
    public final boolean isStream;
    public final boolean skipValidation;
    private final WallClock clock;

    public boolean ran = false;

    public SelectQueryExecution(
            @Nullable final File tmpDir,
            final QueryCache queryCache,
            final Limits limits,
            final ImhotepClient imhotepClient,
            final DatasetsMetadata datasetsMetadata,
            final PrintWriter outputStream,
            final QueryInfo queryInfo,
            final ClientInfo clientInfo,
            final TreeTimer timer,
            final String query,
            final int version,
            final boolean isStream,
            final boolean skipValidation,
            final WallClock clock,
            final QueryMetadata queryMetadata,
            final ExecutorService cacheUploadExecutorService,
            final ImmutableSet<String> defaultIQL2Options) {
        this.outputStream = outputStream;
        this.queryInfo = queryInfo;
        this.clientInfo = clientInfo;
        this.query = query;
        this.version = version;
        this.timer = timer;
        this.isStream = isStream;
        this.limits = limits;
        this.skipValidation = skipValidation;
        this.clock = clock;
        this.imhotepClient = imhotepClient;
        this.datasetsMetadata = datasetsMetadata;
        this.queryCache = queryCache;
        this.queryMetadata = queryMetadata;
        this.cacheUploadExecutorService = cacheUploadExecutorService;
        this.defaultIQL2Options = defaultIQL2Options;
        this.tmpDir = tmpDir;
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
        final Consumer<String> out;
        if (isStream) {
            out = s -> {
                outputStream.print("data: ");
                outputStream.println(s);
            };
        } else {
            out = outputStream::println;
        }

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

        executeSelect(runningQueriesManager, queryInfo, query, version == 1, out, progressCallback);
    }

    private void extractCompletedQueryInfoData(SelectExecutionInformation execInfo, Set<String> warnings, CountingConsumer<String> countingOut) {
        int shardCount = 0;
        Duration totalShardPeriod = Duration.ZERO;
        for (final List<Shard> shardList : execInfo.datasetToShards.values()) {
            shardCount += shardList.size();
            for (final Shard shardInfo : shardList) {
                ShardInfo.DateTimeRange range = shardInfo.getRange();
                totalShardPeriod = totalShardPeriod.plus(new Duration(range.start, range.end));
            }
        }
        queryInfo.numShards = shardCount;
        queryInfo.totalShardPeriodHours = totalShardPeriod.toStandardHours().getHours();
        queryInfo.cached = execInfo.allCached();
        queryInfo.ftgsMB = execInfo.imhotepTempBytesWritten / 1024 / 1024;
        queryInfo.sessionIDs = execInfo.sessionIds;
        queryInfo.numDocs = execInfo.totalNumDocs;
        queryInfo.rows = countingOut.getCount();
        queryInfo.cacheHashes = ImmutableSet.copyOf(execInfo.cacheKeys);
        queryInfo.maxGroups = execInfo.maxNumGroups;
        queryInfo.maxConcurrentSessions = execInfo.maxConcurrentSessions;

        queryInfo.setFromPerformanceStats(execInfo.imhotepPerformanceStats);

        if (execInfo.hasMoreRows) {
            warnings.add(String.format("Only first %d rows returned sorted on the last group by column", queryInfo.rows));
        }
    }

    private SelectExecutionInformation executeSelect(
            final RunningQueriesManager runningQueriesManager,
            final QueryInfo queryInfo,
            final String q,
            final boolean useLegacy,
            final Consumer<String> out,
            final ProgressCallback progressCallback
    ) throws IOException {
        final Set<String> warnings = new HashSet<>();
        timer.push("Select query execution");

        timer.push("parse query");
        final Queries.ParseResult parseResult = Queries.parseQuery(q, useLegacy, datasetsMetadata, defaultIQL2Options, warnings::add, clock);
        timer.pop();

        {
            queryInfo.statementType = "select";

            final Map<String, String> upperCaseToActualDataset = new HashMap<>();
            for (final String dataset : imhotepClient.getDatasetNames()) {
                upperCaseToActualDataset.put(dataset.toUpperCase(), dataset);
            }

            final List<Dataset> allDatasets = Queries.findAllDatasets(parseResult.query);
            Duration datasetRangeSum = Duration.ZERO;
            queryInfo.datasets = new HashSet<>();
            for (final Dataset dataset : allDatasets) {
                final String actualDataset = upperCaseToActualDataset.get(dataset.dataset.unwrap().toUpperCase());
                if (actualDataset == null) {
                    continue;
                }
                queryInfo.datasets.add(actualDataset);
                datasetRangeSum = datasetRangeSum.plus(new Duration(dataset.startInclusive.unwrap(), dataset.endExclusive.unwrap()));
            }
            queryInfo.totalDatasetRangeDays = datasetRangeSum.toStandardDays().getDays();

            final Set<FieldExtractor.DatasetField> datasetFields = FieldExtractor.getDatasetFields(parseResult.query);
            queryInfo.datasetFields = Sets.newHashSet();

            for (final FieldExtractor.DatasetField datasetField : datasetFields) {
                final String actualDataset = upperCaseToActualDataset.get(datasetField.dataset.toUpperCase());
                if (actualDataset == null) {
                    continue;
                }
                datasetField.dataset = actualDataset;
                final DatasetInfo datasetInfo = imhotepClient.getDatasetToDatasetInfo().get(datasetField.dataset);
                final Collection<String> intFields = datasetInfo.getIntFields();
                final Collection<String> stringFields = datasetInfo.getStringFields();
                String field = intFields.stream().filter(intField -> intField.compareToIgnoreCase(datasetField.field) == 0).findFirst().orElse(null);
                if (field == null) {
                    field = stringFields.stream().filter(stringField -> stringField.compareToIgnoreCase(datasetField.field) == 0).findFirst().orElse(null);
                }
                if (field != null) {
                    queryInfo.datasetFields.add(datasetField.dataset + "." + field);
                }
            }
        }

        final int sessions = parseResult.query.datasets.size();
        if (sessions > limits.concurrentImhotepSessionsLimit) {
            throw new UserSessionCountLimitExceededException("User is creating more concurrent imhotep sessions than the limit: " + limits.concurrentImhotepSessionsLimit);
        }
        final StrictCloser strictCloser = new StrictCloser();
        final SelectQuery selectQuery = new SelectQuery(queryInfo, runningQueriesManager, query, clientInfo, limits, new DateTime(queryInfo.queryStartTimestamp),
                null, (byte) sessions, queryMetadata, strictCloser, progressCallback);

        try {
            timer.push("Acquire concurrent query lock");
            selectQuery.lock();
            timer.pop();
            queryInfo.queryStartTimestamp = selectQuery.getQueryStartTimestamp().getMillis();
            return new ParsedQueryExecution(true, parseResult.inputStream, out, warnings, progressCallback,
                    parseResult.query, limits.queryInMemoryRowsLimit, selectQuery, strictCloser).executeParsedQuery();
        } catch (final Exception e) {
            selectQuery.checkCancelled();
            throw e;
        } finally {
            if (!selectQuery.isAsynchronousRelease()) {
                Closeables2.closeQuietly(selectQuery, log);
            }
        }
    }

    private class ParsedQueryExecution {
        private final boolean isTopLevelQuery;
        private final CharStream inputStream;
        private final Consumer<String> externalOutput;
        private final Set<String> warnings;

        private final int groupLimit;
        private final SelectQuery selectQuery;
        private final StrictCloser strictCloser;

        private final Query originalQuery;

        private final ProgressCallback progressCallback;
        private final Map<Query, Boolean> queryCached = new HashMap<>();

        private final AtomicInteger cacheUploadingCounter = new AtomicInteger(0);

        private ParsedQueryExecution(
                boolean isTopLevelQuery,
                CharStream inputStream,
                Consumer<String> out,
                Set<String> warnings,
                ProgressCallback progressCallback,
                Query query,
                @Nullable Integer groupLimit,
                SelectQuery selectQuery,
                StrictCloser strictCloser
        ) {
            this.isTopLevelQuery = isTopLevelQuery;
            this.inputStream = inputStream;
            this.externalOutput = out;
            this.warnings = warnings;
            this.progressCallback = progressCallback;
            this.originalQuery = query;
            this.groupLimit = groupLimit == null ? 1000000 : groupLimit;
            this.selectQuery = selectQuery;
            this.strictCloser = strictCloser;
        }

        private SelectExecutionInformation executeParsedQuery() throws IOException {
            final int[] totalBytesWritten = {0};
            final Set<String> cacheKeys = new HashSet<>();
            final ListMultimap<String, List<Shard>> allShardsUsed = ArrayListMultimap.create();
            final List<DatasetWithMissingShards> datasetsWithMissingShards = new ArrayList<>();

            // TODO: subqueries should be executed at a later stage after the query hash is calculated
            // to support "head only" requests for shard related headers
            final Query query = originalQuery.transform(
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
                                        // TODO: This use of ProgressCallbacks looks wrong.
                                        final SelectExecutionInformation execInfo = new ParsedQueryExecution(false, inputStream, new Consumer<String>() {
                                            @Override
                                            public void accept(String s) {
                                                if ((limits.queryInMemoryRowsLimit > 0) && ((terms.size() + stringTerms.size()) >= limits.queryInMemoryRowsLimit)) {
                                                    throw new IqlKnownException.GroupLimitExceededException("Sub query cannot have more than [" + limits.queryInMemoryRowsLimit + "] terms!");
                                                }
                                                final String term = s.split("\t")[0];
                                                try {
                                                    terms.add(Long.parseLong(term));
                                                } catch (NumberFormatException e) {
                                                    stringTerms.add(term);
                                                }
                                            }
                                        }, warnings, new SessionOpenedOnlyProgressCallback(progressCallback), q, groupLimit, selectQuery, strictCloser).executeParsedQuery();
                                        totalBytesWritten[0] += execInfo.imhotepTempBytesWritten;
                                        cacheKeys.addAll(execInfo.cacheKeys);
                                        allShardsUsed.putAll(execInfo.datasetToShards);
                                        datasetsWithMissingShards.addAll(execInfo.datasetsWithMissingShards);
                                    } catch (IOException e) {
                                        throw Throwables.propagate(e);
                                    }
                                    timer.pop();
                                    queryToResults.put(q, Pair.of(terms, stringTerms));
                                }
                                final Pair<Set<Long>, Set<String>> p = queryToResults.get(q);
                                final ScopedField scopedField = fieldInQuery.field;

                                final List<DocFilter> filters = new ArrayList<>();
                                if (!p.getSecond().isEmpty()) {
                                    final Set<String> terms = Sets.newHashSet(p.getSecond());
                                    for (final long v : p.getFirst()) {
                                        terms.add(String.valueOf(v));
                                    }
                                    filters.add(new DocFilter.StringFieldIn(datasetsMetadata, scopedField.field, terms));
                                } else if (!p.getFirst().isEmpty()) {
                                    filters.add(new DocFilter.IntFieldIn(datasetsMetadata, scopedField.field, p.getFirst()));
                                }
                                final DocFilter.Ors orred = new DocFilter.Ors(filters);
                                final DocFilter maybeNegated;
                                if (fieldInQuery.isNegated) {
                                    maybeNegated = new DocFilter.Not(orred);
                                } else {
                                    maybeNegated = orred;
                                }
                                return scopedField.wrap(maybeNegated);
                            }
                            return input;
                        }
                    }
            );

            timer.push("compute commands");
            final List<Command> commands = Queries.queryCommands(incrementQueryLimit(query), datasetsMetadata);
            timer.pop();

            if (!skipValidation) {
                timer.push("validate commands");
                final Set<String> errors = new HashSet<>();
                final Set<String> warnings = new HashSet<>();
                CommandValidator.validate(commands, query, datasetsMetadata, errors, warnings);

                if (errors.size() != 0) {
                    throw new IqlKnownException.ParseErrorException("Errors found when validating query: " + errors);
                }
                if (warnings.size() != 0) {
                    this.warnings.addAll(warnings);
                }
                timer.pop();
            }

            final Set<String> conflictFieldsUsed = Sets.intersection(queryInfo.datasetFields, datasetsMetadata.getTypeConflictDatasetFieldNames());
            if (conflictFieldsUsed.size() > 0) {
                final String conflictWarning = "Fields with type conflicts used in query: " + String.join(", ", conflictFieldsUsed);
                warnings.add(conflictWarning);
            }

            final ComputeCacheKey computeCacheKey = computeCacheKey(timer, query, commands, imhotepClient);
            final Map<String, List<Shard>> datasetToChosenShards = Collections.unmodifiableMap(computeCacheKey.datasetToChosenShards);
            allShardsUsed.putAll(Multimaps.forMap(datasetToChosenShards));
            datasetsWithMissingShards.addAll(computeCacheKey.datasetsWithMissingShards);
            if (isTopLevelQuery) {
                queryMetadata.addItem("IQL-Newest-Shard", ISODateTimeFormat.dateTime().print(newestStaticShard(allShardsUsed).or(-1L)), true);

                for(DatasetWithMissingShards datasetWithMissingShards : datasetsWithMissingShards) {
                    warnings.addAll(QueryServlet.missingShardsToWarnings(datasetWithMissingShards.dataset,
                            datasetWithMissingShards.start, datasetWithMissingShards.end, datasetWithMissingShards.timeIntervalsMissingShards));
                }

                final List<Interval> missingIntervals = datasetsWithMissingShards.stream()
                        .map(DatasetWithMissingShards::getTimeIntervalsMissingShards).collect(ArrayList::new, List::addAll, List::addAll);
                if(missingIntervals.size() > 0) {
                    final String missingIntervalsString = QueryServlet.intervalListToString(missingIntervals);
                    queryMetadata.addItem("IQL-Missing-Shards", missingIntervalsString, true);
                }
            }

            // TODO: if HEAD query, return here

            cacheKeys.add(computeCacheKey.rawHash);

            final CountingConsumer<String> countingExternalOutput = new CountingConsumer<>(externalOutput);

            Consumer<String> out = countingExternalOutput;

            final boolean skipCache = query.options.contains(QueryOptions.NO_CACHE);

            final boolean cacheEnabled = queryCache.isEnabled() && !skipCache;
            final File cacheFile;
            final BufferedWriter cacheWriter;

            try (final StrictCloser innerStrictCloser = new StrictCloser()) {
                strictCloser.registerOrClose(innerStrictCloser);
                final String cacheFileName = computeCacheKey.cacheFileName;
                if (cacheEnabled) {
                    timer.push("cache check");
                    final InputStream cacheInputStream = queryCache.getInputStream(cacheFileName);
                    final boolean isCached = cacheInputStream != null;
                    timer.pop();

                    queryCached.put(query, isCached);

                    if (isCached) {
                        timer.push("read cache");
                        if (isTopLevelQuery) {
                            boolean allQueriesCached = queryCached.values().stream().allMatch((queryIsCached) -> queryIsCached);
                            queryMetadata.addItem("IQL-Cached", allQueriesCached, true);
                            // read metadata from cache
                            try {
                                final InputStream metadataCacheStream = queryCache.getInputStream(cacheFileName + METADATA_FILE_SUFFIX);
                                final QueryMetadata cachedMetadata = QueryMetadata.fromStream(metadataCacheStream);
                                queryMetadata.mergeIn(cachedMetadata);
                                queryMetadata.renameItem("IQL-Query-Info", "IQL-Cached-Query-Info");
                            } catch (Exception e) {
                                log.info("Failed to load metadata cache from " + cacheFileName + METADATA_FILE_SUFFIX, e);
                            }
                            queryMetadata.setPendingHeaders();
                        }
                        // TODO: Don't have this hack
                        progressCallback.startCommand(null, null, true);
                        final boolean hasMoreRows = sendCachedQuery(out, query.rowLimit, cacheInputStream);
                        timer.pop();
                        final SelectExecutionInformation selectExecutionInformation = new SelectExecutionInformation(allShardsUsed, datasetsWithMissingShards,
                                queryCached, totalBytesWritten[0], null, cacheKeys,
                                Collections.<String>emptyList(), 0, 0, 0, hasMoreRows);

                        finalizeQueryExecution(countingExternalOutput, selectExecutionInformation);
                        return selectExecutionInformation;
                    }

                    final Consumer<String> oldOut = out;
                    cacheFile = File.createTempFile("query", ".cache.tmp", tmpDir);
                    // TODO: Use LimitedBufferedOutputStream or mark as skipped on limit
                    cacheWriter = new BufferedWriter(new FileWriter(cacheFile));
                    out = s -> {
                        oldOut.accept(s);
                        try {
                            cacheWriter.write(s);
                            cacheWriter.newLine();
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

                final List<Queries.QueryDataset> datasets = Queries.createDatasetMap(inputStream, query, datasetsMetadata.getDatasetToDimensionAliasFields());

                final InfoCollectingProgressCallback infoCollectingProgressCallback = new InfoCollectingProgressCallback();
                final ProgressCallback compositeProgressCallback = CompositeProgressCallback.create(progressCallback, infoCollectingProgressCallback);
                try {
                    final Session.CreateSessionResult createResult = Session.createSession(
                            imhotepClient,
                            datasetToChosenShards,
                            groupLimit,
                            Sets.newHashSet(query.options),
                            commands,
                            datasets,
                            innerStrictCloser,
                            out,
                            timer,
                            compositeProgressCallback,
                            mbToBytes(limits.queryFTGSIQLLimitMB),
                            mbToBytes(limits.queryFTGSImhotepDaemonLimitMB),
                            clientInfo.username
                    );

                    final SelectExecutionInformation selectExecutionInformation = new SelectExecutionInformation(
                            allShardsUsed,
                            datasetsWithMissingShards,
                            queryCached,
                            createResult.tempFileBytesWritten + totalBytesWritten[0],
                            createResult.imhotepPerformanceStats,
                            cacheKeys,
                            infoCollectingProgressCallback.getSessionIds(),
                            infoCollectingProgressCallback.getTotalNumDocs(),
                            infoCollectingProgressCallback.getMaxNumGroups(),
                            infoCollectingProgressCallback.getMaxConcurrentSessions(),
                            hasMoreRows.get());

                    finalizeQueryExecution(countingExternalOutput, selectExecutionInformation);

                    if (cacheEnabled) {
                        // Cache upload
                        cacheUploadingCounter.incrementAndGet();

                        cacheUploadExecutorService.submit(new Callable<Void>() {
                            @Override
                            public Void call() {
                                try {
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
                                } finally {
                                    if (cacheUploadingCounter.decrementAndGet() == 0) {
                                        Closeables2.closeQuietly(selectQuery, log);
                                    }
                                    if (!cacheFile.delete()) {
                                        log.warn("Failed to delete " + cacheFile);
                                    }
                                }
                                return null;
                            }
                        });
                        selectQuery.markAsynchronousRelease(); // going to be closed asynchronously after cache is uploaded
                    }

                    return selectExecutionInformation;
                } catch (final Exception e) {
                    if (cacheFile != null) {
                        if(!cacheFile.delete()) {
                            log.info("Failed to delete: " + cacheFile.getPath());
                        }
                    }
                    throw Throwables.propagate(e);
                }
            }
        }

        private void finalizeQueryExecution(CountingConsumer<String> countingExternalOutput, SelectExecutionInformation selectExecutionInformation) {
            if(!isTopLevelQuery) {
                return;
            }
            extractCompletedQueryInfoData(selectExecutionInformation, warnings, countingExternalOutput);
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
        if(megabytes == null) {
            return 0L;
        }
        return megabytes <= 0 ? (long)megabytes : (long)megabytes * 1024 * 1024;
    }

    // increment query limit so that we know that whether it filters the response data size
    private Query incrementQueryLimit(final Query query) {
        final Optional<Integer> newRowLimit = query.rowLimit.transform(new Function<Integer, Integer>() {
            @Nullable
            @Override
            public Integer apply(@Nullable final Integer integer) {
                return (integer == null) ? integer : integer + 1;
            }
        });
        return new Query(query.datasets, query.filter, query.groupBys, query.selects, query.formatStrings, query.options, newRowLimit, query.useLegacy);
    }

    private static class DatasetWithMissingShards {
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

    public static ComputeCacheKey computeCacheKey(TreeTimer timer, Query query, List<Command> commands, ImhotepClient imhotepClient) {
        timer.push("compute dataset normalization");
        final List<String> datasets = imhotepClient.getDatasetNames();
        final Map<String, String> upperCaseToActualDataset = Maps.newHashMapWithExpectedSize(datasets.size());
        for (final String dataset : datasets) {
            upperCaseToActualDataset.put(dataset.toUpperCase(), dataset);
        }
        timer.pop();

        timer.push("compute hash");
        final Set<Pair<String, String>> shards = Sets.newHashSet();
        final Set<DatasetWithTimeRangeAndAliases> datasetsWithTimeRange = Sets.newHashSet();
        final Map<String, List<Shard>> datasetToChosenShards = Maps.newHashMap();
        final List<DatasetWithMissingShards> datasetsWithMissingShards = new ArrayList<>();
        for (final Dataset dataset : query.datasets) {
            timer.push("get chosen shards");
            final String actualDataset = upperCaseToActualDataset.get(dataset.dataset.unwrap());
            if (actualDataset == null) {
                throw new IqlKnownException.UnknownDatasetException("Unknown dataset: " + dataset.dataset.unwrap());
            }
            final String sessionName = dataset.alias.or(dataset.dataset).unwrap();
            final DateTime startTime = dataset.startInclusive.unwrap();
            final DateTime endTime = dataset.endExclusive.unwrap();
            ImhotepClient.SessionBuilder imhotepSessionBuilder =
                    imhotepClient.sessionBuilder(actualDataset, startTime, endTime);
            final List<Shard> chosenShards = imhotepSessionBuilder.getChosenShards();
            datasetsWithMissingShards.add(new DatasetWithMissingShards(sessionName, startTime, endTime, imhotepSessionBuilder.getTimeIntervalsMissingShards()));
            timer.pop();
            for (final Shard chosenShard : chosenShards) {
                // This needs to be associated with the session name, not just the actualDataset.
                shards.add(Pair.of(sessionName, chosenShard.getShardId() + "-" + chosenShard.getVersion()));
            }
            final Set<FieldAlias> fieldAliases = Sets.newHashSet();
            for (final Map.Entry<Positioned<String>, Positioned<String>> e : dataset.fieldAliases.entrySet()) {
                fieldAliases.add(new FieldAlias(e.getValue().unwrap(), e.getKey().unwrap()));
            }
            datasetsWithTimeRange.add(new DatasetWithTimeRangeAndAliases(actualDataset, dataset.startInclusive.unwrap().getMillis(), dataset.endExclusive.unwrap().getMillis(), fieldAliases));
            final List<Shard> oldShards = datasetToChosenShards.put(sessionName, chosenShards);
            if (oldShards != null) {
                throw new IllegalArgumentException("Overwrote shard list for " + sessionName);
            }
        }
        final TreeSet<String> sortedOptions = Sets.newTreeSet(query.options);
        final String queryHash = computeQueryHash(commands, query.rowLimit, sortedOptions, shards, datasetsWithTimeRange, SelectQuery.VERSION_FOR_HASHING);
        final String cacheFileName = "IQL2-" + queryHash + ".tsv";
        timer.pop();

        return new ComputeCacheKey(datasetToChosenShards, datasetsWithMissingShards, queryHash, cacheFileName);
    }

    private static boolean sendCachedQuery(Consumer<String> out, Optional<Integer> rowLimit, InputStream cacheInputStream) throws IOException {
        final int limit = rowLimit.or(Integer.MAX_VALUE);
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

    private static String computeQueryHash(List<Command> commands, Optional<Integer> rowLimit, final Collection<String> sortedOptions, Set<Pair<String, String>> shards, Set<DatasetWithTimeRangeAndAliases> datasets, int version) {
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
        for (final DatasetWithTimeRangeAndAliases dataset : datasets) {
            sha1.update(dataset.dataset.getBytes(Charsets.UTF_8));
            sha1.update(Longs.toByteArray(dataset.start));
            sha1.update(Longs.toByteArray(dataset.end));
            final List<FieldAlias> sortedFieldAliases = Lists.newArrayList(dataset.fieldAliases);
            Collections.sort(sortedFieldAliases, new Comparator<FieldAlias>() {
                @Override
                public int compare(FieldAlias o1, FieldAlias o2) {
                    return o1.newName.compareTo(o2.newName);
                }
            });
            for (final FieldAlias fieldAlias : sortedFieldAliases) {
                sha1.update(fieldAlias.toString().getBytes(Charsets.UTF_8));
            }
        }
        for (final String option : sortedOptions) {
            if (QueryOptions.includeInCacheKey(option)) {
                sha1.update(option.getBytes(Charsets.UTF_8));
            }
        }
        sha1.update(Ints.toByteArray(rowLimit.or(-1)));
        for (final Pair<String, String> pair : shards) {
            sha1.update(pair.getFirst().getBytes(Charsets.UTF_8));
            sha1.update(pair.getSecond().getBytes(Charsets.UTF_8));
        }
        return Base64.encodeBase64URLSafeString(sha1.digest());
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
            return Optional.absent();
        } else {
            return Optional.of(DateTimeFormat.forPattern("yyyyMMddHHmmss").parseMillis(String.valueOf(newestStatic)));
        }
    }

    private static class SelectExecutionInformation {
        public final Multimap<String, List<Shard>> datasetToShards;
        public final List<DatasetWithMissingShards> datasetsWithMissingShards;
        public final Map<Query, Boolean> queryCached;
        public final long imhotepTempBytesWritten;
        @Nullable
        public final PerformanceStats imhotepPerformanceStats;
        public final Set<String> cacheKeys;

        public final Set<String> sessionIds;
        public final long totalNumDocs;
        public final int maxNumGroups;
        public final int maxConcurrentSessions;
        public final boolean hasMoreRows;

        private SelectExecutionInformation(Multimap<String, List<Shard>> datasetToShards, List<DatasetWithMissingShards> datasetsWithMissingShards,
                                           Map<Query, Boolean> queryCached, long imhotepTempBytesWritten, PerformanceStats imhotepPerformanceStats,
                                           Set<String> cacheKeys, List<String> sessionIds, long totalNumDocs, int maxNumGroups, int maxConcurrentSessions,
                                           final boolean hasMoreRows) {
            this.datasetToShards = datasetToShards;
            this.datasetsWithMissingShards = datasetsWithMissingShards;
            this.queryCached = queryCached;
            this.imhotepTempBytesWritten = imhotepTempBytesWritten;
            this.imhotepPerformanceStats = imhotepPerformanceStats;
            this.cacheKeys = ImmutableSet.copyOf(cacheKeys);
            this.sessionIds = ImmutableSet.copyOf(sessionIds);
            this.totalNumDocs = totalNumDocs;
            this.maxNumGroups = maxNumGroups;
            this.maxConcurrentSessions = maxConcurrentSessions;
            this.hasMoreRows = hasMoreRows;
        }

        public boolean allCached() {
            for (final boolean b : queryCached.values()) {
                if (!b) {
                    return false;
                }
            }
            return true;
        }
    }

    private static class DatasetWithTimeRangeAndAliases {
        public final String dataset;
        public final long start;
        public final long end;
        public final Set<FieldAlias> fieldAliases;

        private DatasetWithTimeRangeAndAliases(String dataset, long start, long end, Set<FieldAlias> fieldAliases) {
            this.dataset = dataset;
            this.start = start;
            this.end = end;
            this.fieldAliases = fieldAliases;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DatasetWithTimeRangeAndAliases that = (DatasetWithTimeRangeAndAliases) o;
            return start == that.start &&
                    end == that.end &&
                    Objects.equals(dataset, that.dataset) &&
                    Objects.equals(fieldAliases, that.fieldAliases);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataset, start, end, fieldAliases);
        }

        @Override
        public String toString() {
            return "DatasetWithTimeRangeAndAliases{" +
                    "dataset='" + dataset + '\'' +
                    ", start=" + start +
                    ", end=" + end +
                    ", fieldAliases=" + fieldAliases +
                    '}';
        }
    }

    public static class ComputeCacheKey {
        public final Map<String, List<Shard>> datasetToChosenShards;
        public final List<DatasetWithMissingShards> datasetsWithMissingShards;
        public final String rawHash;
        public final String cacheFileName;

        private ComputeCacheKey(Map<String, List<Shard>> datasetToChosenShards, List<DatasetWithMissingShards> datasetsWithMissingShards, String rawHash, String cacheFileName) {
            this.datasetToChosenShards = datasetToChosenShards;
            this.datasetsWithMissingShards = datasetsWithMissingShards;
            this.rawHash = rawHash;
            this.cacheFileName = cacheFileName;
        }
    }

    private static class FieldAlias {
        public final String originalName;
        public final String newName;

        private FieldAlias(String originalName, String newName) {
            this.originalName = originalName;
            this.newName = newName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if ((o == null) || (getClass() != o.getClass())) {
                return false;
            }
            FieldAlias that = (FieldAlias) o;
            return Objects.equals(originalName, that.originalName) &&
                    Objects.equals(newName, that.newName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(originalName, newName);
        }

        @Override
        public String toString() {
            return "FieldAlias{" +
                    "originalName='" + originalName + '\'' +
                    ", newName='" + newName + '\'' +
                    '}';
        }
    }
}
