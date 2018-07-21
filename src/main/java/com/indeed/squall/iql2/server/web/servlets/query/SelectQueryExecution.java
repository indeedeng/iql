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
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.DynamicIndexSubshardDirnameUtil;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.exceptions.ImhotepKnownException;
import com.indeed.imhotep.exceptions.UserSessionCountLimitExceededException;
import com.indeed.imhotep.iql.cache.QueryCache;
import com.indeed.imhotep.web.ClientInfo;
import com.indeed.imhotep.web.Limits;
import com.indeed.imhotep.web.RunningQueriesManager;
import com.indeed.imhotep.web.SelectQuery;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.squall.iql2.execution.QueryOptions;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.progress.CompositeProgressCallback;
import com.indeed.squall.iql2.execution.progress.ProgressCallback;
import com.indeed.squall.iql2.execution.progress.SessionOpenedOnlyProgressCallback;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.Positioned;
import com.indeed.squall.iql2.language.ScopedField;
import com.indeed.squall.iql2.language.commands.Command;
import com.indeed.squall.iql2.language.metadata.DatasetsMetadata;
import com.indeed.squall.iql2.language.query.Dataset;
import com.indeed.squall.iql2.language.query.GroupBy;
import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.squall.iql2.language.query.Query;
import com.indeed.squall.iql2.language.util.FieldExtractor;
import com.indeed.squall.iql2.language.util.FieldExtractor.DatasetField;
import com.indeed.util.core.Pair;
import com.indeed.util.core.TreeTimer;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.time.WallClock;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.antlr.v4.runtime.CharStream;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class SelectQueryExecution implements Closeable {
    private static final Logger log = Logger.getLogger(SelectQueryExecution.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // IQL2 server systems
    private final QueryCache queryCache;

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
    private final Closer closer = Closer.create();

    public boolean ran = false;
    public long queryStartTimestamp = -1L;

    public SelectQueryExecution(
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
            final WallClock clock
    ) {
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
    }

    @Override
    public void close() {
        Closeables2.closeQuietly(closer, log);
    }

    public void processSelect(final RunningQueriesManager runningQueriesManager) throws TimeoutException, IOException, ImhotepOutOfMemoryException {
        // .. just in case.
        synchronized (this) {
            if (ran) {
                throw new IllegalArgumentException("Cannot run multiple times!");
            }
            ran = true;
        }

        final long startTime = clock.currentTimeMillis();
        queryStartTimestamp = System.currentTimeMillis(); // ignore time spent waiting
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

        final CountingConsumer<String> countingOut = new CountingConsumer<>(out);
        final Set<String> warnings = new HashSet<>();
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

        final SelectExecutionInformation execInfo = executeSelect(runningQueriesManager, queryInfo, query, version == 1, countingOut, progressCallback, new com.indeed.squall.iql2.language.compat.Consumer<String>() {
            @Override
            public void accept(String s) {
                warnings.add(s);
            }
        });
        extractCompletedQueryInfoData(execInfo, warnings, countingOut);
        if (isStream) {
            outputStream.println();
            outputStream.println("event: header");

            // TODO: Fix these headers
            final Map<String, Object> headerMap = new HashMap<>();
            headerMap.put("IQL-Cached", execInfo.allCached());
            headerMap.put("IQL-Timings", timer.toString().replaceAll("\n", "\t"));
            headerMap.put("IQL-Shard-Lists", execInfo.perDatasetShardIds().toString());
            headerMap.put("IQL-Newest-Shard", ISODateTimeFormat.dateTime().print(execInfo.newestStaticShard().or(-1L)));
            headerMap.put("IQL-Imhotep-Temp-Bytes-Written", execInfo.imhotepTempBytesWritten);
            headerMap.put("Imhotep-Session-IDs", execInfo.sessionIds);
            headerMap.put("IQL-Execution-Time", ISODateTimeFormat.dateTime().print(startTime));
            if (!warnings.isEmpty()) {
                headerMap.put("IQL-Warning", Joiner.on('\n').join(warnings));
            }
            if (queryInfo.cpuSlotsExecTimeMs != null) {
                headerMap.put("Imhotep-CPU-Slots-Execution-Time-MS", queryInfo.cpuSlotsExecTimeMs);
            }
            outputStream.println("data: " + OBJECT_MAPPER.writeValueAsString(headerMap));
            outputStream.println();

            outputStream.println("event: complete");
            outputStream.println("data: :)");
            outputStream.println();
        }
    }

    private void extractCompletedQueryInfoData(SelectExecutionInformation execInfo, Set<String> warnings, CountingConsumer<String> countingOut) {
        int shardCount = 0;
        Duration totalShardPeriod = Duration.ZERO;
        for (final List<Shard> shardList : execInfo.shards.values()) {
            shardCount += shardList.size();
            for (final Shard shardInfo : shardList) {
                ShardInfo.DateTimeRange range = shardInfo.getRange();
                totalShardPeriod = totalShardPeriod.plus(new Duration(range.start, range.end));
            }
        }
        queryInfo.numShards = shardCount;
        queryInfo.totalShardPeriod = totalShardPeriod;
        queryInfo.cached = execInfo.allCached();
        queryInfo.ftgsMB = execInfo.imhotepTempBytesWritten / 1024 / 1024;
        queryInfo.sessionIDs = execInfo.sessionIds;
        queryInfo.numDocs = execInfo.totalNumDocs;
        queryInfo.rows = countingOut.getCount();
        queryInfo.cacheHashes = ImmutableSet.copyOf(execInfo.cacheKeys);
        queryInfo.maxGroups = execInfo.maxNumGroups;
        queryInfo.maxConcurrentSessions = execInfo.maxConcurrentSessions;

        final PerformanceStats imhotepPerfStats = execInfo.imhotepPerformanceStats;
        if (imhotepPerfStats != null) {
            queryInfo.imhotepcputimems = imhotepPerfStats.cpuTime / 1000000;   // nanoseconds to ms;
            queryInfo.imhoteprammb = imhotepPerfStats.maxMemoryUsage / 1024 / 1024;
            queryInfo.imhotepftgsmb = imhotepPerfStats.ftgsTempFileSize / 1024 / 1024;
            queryInfo.imhotepfieldfilesmb = imhotepPerfStats.fieldFilesReadSize / 1024 / 1024;
            queryInfo.cpuSlotsExecTimeMs = imhotepPerfStats.cpuSlotsExecTimeMs;
            queryInfo.cpuSlotsWaitTimeMs = imhotepPerfStats.cpuSlotsWaitTimeMs;
            queryInfo.ioSlotsExecTimeMs = imhotepPerfStats.ioSlotsExecTimeMs;
            queryInfo.ioSlotsWaitTimeMs = imhotepPerfStats.ioSlotsWaitTimeMs;
        }

        if (execInfo.hasMoreRows) {
            warnings.add(String.format("Only first %d rows returned sorted on the last group by column", queryInfo.rows));
        }
    }

    // TODO: These parameters are nuts
    private SelectExecutionInformation executeSelect(
            final RunningQueriesManager runningQueriesManager,
            final QueryInfo queryInfo,
            final String q,
            final boolean useLegacy,
            final Consumer<String> out,
            final ProgressCallback progressCallback,
            final com.indeed.squall.iql2.language.compat.Consumer<String> warn
    ) throws IOException, ImhotepOutOfMemoryException, ImhotepKnownException {
        timer.push(q.replaceAll("\\s+", " "));

        timer.push("parse query");
        final Queries.ParseResult parseResult = Queries.parseQuery(q, useLegacy, datasetsMetadata, warn, clock);
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
                queryInfo.datasets.add(upperCaseToActualDataset.get(dataset.dataset.unwrap().toUpperCase()));
                datasetRangeSum = datasetRangeSum.plus(new Duration(dataset.startInclusive.unwrap(), dataset.endExclusive.unwrap()));
            }
            queryInfo.totalDatasetRange = datasetRangeSum;

            final Set<DatasetField> datasetFields = FieldExtractor.getDatasetFields(parseResult.query);
            queryInfo.datasetFields = Sets.newHashSet();

            for (final DatasetField datasetField : datasetFields) {
                datasetField.dataset = upperCaseToActualDataset.get(datasetField.dataset.toUpperCase());
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
        final SelectQuery selectQuery = new SelectQuery(runningQueriesManager, query, clientInfo, limits, new DateTime(queryStartTimestamp),
                null, (byte) sessions, this);

        try {
            timer.push("Acquire concurrent query lock");
            selectQuery.lock();
            timer.pop();
            queryStartTimestamp = selectQuery.getQueryStartTimestamp().getMillis();
            return new ParsedQueryExecution(parseResult.inputStream, out, warn, progressCallback, parseResult.query, limits.queryInMemoryRowsLimit).executeParsedQuery();
        } finally {
            if (!selectQuery.isAsynchronousRelease()) {
                Closeables2.closeQuietly(selectQuery, log);
            }
            timer.pop();
        }
    }

    private class ParsedQueryExecution {
        private final CharStream inputStream;
        private final Consumer<String> externalOutput;
        private final com.indeed.squall.iql2.language.compat.Consumer warn;

        private final int groupLimit;

        private final Query originalQuery;

        private final ProgressCallback progressCallback;
        private final Map<Query, Boolean> queryCached = new HashMap<>();

        private ParsedQueryExecution(CharStream inputStream, Consumer<String> out, com.indeed.squall.iql2.language.compat.Consumer warn, ProgressCallback progressCallback, Query query, @Nullable Integer groupLimit) {
            this.inputStream = inputStream;
            this.externalOutput = out;
            this.warn = warn;
            this.progressCallback = progressCallback;
            this.originalQuery = query;
            this.groupLimit = groupLimit == null ? 1000000 : groupLimit;
        }

        private SelectExecutionInformation executeParsedQuery() throws IOException {
            final int[] totalBytesWritten = {0};
            final Set<String> cacheKeys = new HashSet<>();
            final ListMultimap<String, List<Shard>> allShardsUsed = ArrayListMultimap.create();

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
                                        final SelectExecutionInformation execInfo = new ParsedQueryExecution(inputStream, new Consumer<String>() {
                                            @Override
                                            public void accept(String s) {
                                                if ((limits.queryInMemoryRowsLimit > 0) && ((terms.size() + stringTerms.size()) >= limits.queryInMemoryRowsLimit)) {
                                                    throw new IqlKnownException.LimitExceededException("Sub query cannot have more than [" + limits.queryInMemoryRowsLimit + "] terms!");
                                                }
                                                final String term = s.split("\t")[0];
                                                try {
                                                    terms.add(Long.parseLong(term));
                                                } catch (NumberFormatException e) {
                                                    stringTerms.add(term);
                                                }
                                            }
                                        }, warn, new SessionOpenedOnlyProgressCallback(progressCallback), q, groupLimit).executeParsedQuery();
                                        totalBytesWritten[0] += execInfo.imhotepTempBytesWritten;
                                        cacheKeys.addAll(execInfo.cacheKeys);
                                        allShardsUsed.putAll(execInfo.shards);
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
                    for (String warning : warnings) {
                        warn.accept(warning);
                    }
                }
                timer.pop();
            }

            final ComputeCacheKey computeCacheKey = computeCacheKey(timer, query, commands, imhotepClient);
            final Map<String, List<Shard>> datasetToChosenShards = Collections.unmodifiableMap(computeCacheKey.datasetToChosenShards);
            allShardsUsed.putAll(Multimaps.forMap(datasetToChosenShards));

            final AtomicBoolean errorOccurred = new AtomicBoolean(false);

            cacheKeys.add(computeCacheKey.rawHash);

            Consumer<String> out = externalOutput;

            final boolean skipCache = query.options.contains(QueryOptions.NO_CACHE);

            try (final Closer closer = Closer.create()) {
                if (queryCache.isEnabled() && !skipCache) {
                    timer.push("cache check");
                    final boolean isCached = queryCache.isFileCached(computeCacheKey.cacheFileName);
                    timer.pop();

                    queryCached.put(query, isCached);

                    if (isCached) {
                        timer.push("read cache");
                        // TODO: Don't have this hack
                        progressCallback.startCommand(null, null, true);
                        final boolean hasMoreRows = sendCachedQuery(computeCacheKey.cacheFileName, out, query.rowLimit, queryCache);
                        timer.pop();
                        return new SelectExecutionInformation(allShardsUsed, queryCached, totalBytesWritten[0], null, cacheKeys,
                                Collections.<String>emptyList(), 0, 0, 0, hasMoreRows);
                    } else {
                        final Consumer<String> oldOut = out;
                        final Path tmpFile = Files.createTempFile("query", ".cache.tmp");
                        final File cacheFile = tmpFile.toFile();
                        final BufferedWriter cacheWriter = new BufferedWriter(new FileWriter(cacheFile));
                        closer.register(new Closeable() {
                            @Override
                            public void close() throws IOException {
                                // TODO: Do this stuff asynchronously
                                timer.push("Cache upload");
                                cacheWriter.close();
                                if (!errorOccurred.get()) {
                                    queryCache.writeFromFile(computeCacheKey.cacheFileName, cacheFile);
                                }
                                if (!cacheFile.delete()) {
                                    log.warn("Failed to delete  " + cacheFile);
                                }
                                timer.pop();
                            }
                        });
                        out = new Consumer<String>() {
                            @Override
                            public void accept(String s) {
                                oldOut.accept(s);
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
                            closer,
                            out,
                            timer,
                            compositeProgressCallback,
                            mbToBytes(limits.queryFTGSIQLLimitMB),
                            mbToBytes(limits.queryFTGSImhotepDaemonLimitMB),
                            clientInfo.username
                    );
                    return new SelectExecutionInformation(
                            allShardsUsed,
                            queryCached,
                            createResult.tempFileBytesWritten + totalBytesWritten[0],
                            createResult.imhotepPerformanceStats,
                            cacheKeys,
                            infoCollectingProgressCallback.getSessionIds(),
                            infoCollectingProgressCallback.getTotalNumDocs(),
                            infoCollectingProgressCallback.getMaxNumGroups(),
                            infoCollectingProgressCallback.getMaxConcurrentSessions(),
                            hasMoreRows.get());
                } catch (Exception e) {
                    errorOccurred.set(true);
                    throw Throwables.propagate(e);
                }
            }
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
        for (final Dataset dataset : query.datasets) {
            timer.push("get chosen shards");
            final String actualDataset = upperCaseToActualDataset.get(dataset.dataset.unwrap());
            if (actualDataset == null) {
                throw new IqlKnownException.ParseErrorException("Unknown dataset: " + dataset.dataset.unwrap());
            }
            final String sessionName = dataset.alias.or(dataset.dataset).unwrap();
            final List<Shard> chosenShards = imhotepClient.findShardsForTimeRange(actualDataset, dataset.startInclusive.unwrap(), dataset.endExclusive.unwrap());
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
        final String queryHash = computeQueryHash(commands, query.rowLimit, shards, datasetsWithTimeRange, SelectQuery.VERSION_FOR_HASHING);
        final String cacheFileName = "IQL2-" + queryHash + ".tsv";
        timer.pop();

        return new ComputeCacheKey(datasetToChosenShards, queryHash, cacheFileName);
    }

    private static boolean sendCachedQuery(String cacheFile, Consumer<String> out, Optional<Integer> rowLimit, QueryCache queryCache) throws IOException {
        final int limit = rowLimit.or(Integer.MAX_VALUE);
        int rowsWritten = 0;
        boolean hasMoreRows = false;
        try (final BufferedReader stream = new BufferedReader(new InputStreamReader(queryCache.getInputStream(cacheFile)))) {
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

    private static String computeQueryHash(List<Command> commands, Optional<Integer> rowLimit, Set<Pair<String, String>> shards, Set<DatasetWithTimeRangeAndAliases> datasets, int version) {
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
        sha1.update(Ints.toByteArray(rowLimit.or(-1)));
        for (final Pair<String, String> pair : shards) {
            sha1.update(pair.getFirst().getBytes(Charsets.UTF_8));
            sha1.update(pair.getSecond().getBytes(Charsets.UTF_8));
        }
        return Base64.encodeBase64URLSafeString(sha1.digest());
    }

    static class QueryInfo {
        @Nullable String statementType;
        @Nullable Set<String> datasets;
        @Nullable Duration totalDatasetRange; // SUM(dataset (End - Start))
        @Nullable Duration totalShardPeriod; // SUM(shard (end-start))
        @Nullable Long ftgsMB;
        @Nullable Long imhotepcputimems;
        @Nullable Long imhoteprammb;
        @Nullable Long imhotepftgsmb;
        @Nullable Long imhotepfieldfilesmb;
        @Nullable Long cpuSlotsExecTimeMs;
        @Nullable Long cpuSlotsWaitTimeMs;
        @Nullable Long ioSlotsExecTimeMs;
        @Nullable Long ioSlotsWaitTimeMs;
        @Nullable Collection<String> sessionIDs;
        @Nullable Integer numShards;
        @Nullable Long numDocs;
        @Nullable Boolean cached;
        @Nullable Integer rows;
        @Nullable Set<String> cacheHashes;
        @Nullable Integer maxGroups;
        @Nullable Integer maxConcurrentSessions;
        @Nullable Set<String> datasetFields;

    }

    private static class SelectExecutionInformation {
        public final Multimap<String, List<Shard>> shards;
        public final Map<Query, Boolean> queryCached;
        public final long imhotepTempBytesWritten;
        @Nullable
        public final PerformanceStats imhotepPerformanceStats;
        public final Set<String> cacheKeys;

        public final List<String> sessionIds;
        public final long totalNumDocs;
        public final int maxNumGroups;
        public final int maxConcurrentSessions;
        public final boolean hasMoreRows;

        private SelectExecutionInformation(Multimap<String, List<Shard>> shards, Map<Query, Boolean> queryCached, long imhotepTempBytesWritten, PerformanceStats imhotepPerformanceStats, Set<String> cacheKeys, List<String> sessionIds,
                                           long totalNumDocs, int maxNumGroups, int maxConcurrentSessions, final boolean hasMoreRows) {
            this.shards = shards;
            this.queryCached = queryCached;
            this.imhotepTempBytesWritten = imhotepTempBytesWritten;
            this.imhotepPerformanceStats = imhotepPerformanceStats;
            this.cacheKeys = ImmutableSet.copyOf(cacheKeys);
            this.sessionIds = ImmutableList.copyOf(sessionIds);
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

        public Multimap<String, List<String>> perDatasetShardIds() {
            return Multimaps.transformValues(shards, new Function<List<Shard>, List<String>>() {
                public List<String> apply(List<Shard> shardsForDataset) {
                    return Shard.keepShardIds(shardsForDataset);
                }
            });
        }

        public Optional<Long> newestStaticShard() {
            long newestStatic = -1;
            for (final List<Shard> shardset : shards.values()) {
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
        public final String rawHash;
        public final String cacheFileName;

        private ComputeCacheKey(Map<String, List<Shard>> datasetToChosenShards, String rawHash, String cacheFileName) {
            this.datasetToChosenShards = datasetToChosenShards;
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
