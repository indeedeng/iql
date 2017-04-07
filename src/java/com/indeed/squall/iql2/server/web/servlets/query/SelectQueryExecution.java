package com.indeed.squall.iql2.server.web.servlets.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.indeed.common.util.time.WallClock;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.ShardIdWithVersion;
import com.indeed.squall.iql2.execution.Session;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.dimensions.DatasetDimensions;
import com.indeed.squall.iql2.execution.progress.CompositeProgressCallback;
import com.indeed.squall.iql2.execution.progress.NoOpProgressCallback;
import com.indeed.squall.iql2.execution.progress.ProgressCallback;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.Positioned;
import com.indeed.squall.iql2.language.ScopedField;
import com.indeed.squall.iql2.language.commands.Command;
import com.indeed.squall.iql2.language.query.Dataset;
import com.indeed.squall.iql2.language.query.GroupBy;
import com.indeed.squall.iql2.language.query.Queries;
import com.indeed.squall.iql2.language.query.Query;
import com.indeed.squall.iql2.server.web.ExecutionManager;
import com.indeed.squall.iql2.server.web.cache.QueryCache;
import com.indeed.util.core.Pair;
import com.indeed.util.core.TreeTimer;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.antlr.v4.runtime.CharStream;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.Charsets;
import org.apache.log4j.Logger;
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

public class SelectQueryExecution {
    private static final Logger log = Logger.getLogger(SelectQueryExecution.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // IQL2 server systems
    private final QueryCache queryCache;
    private final ExecutionManager executionManager;

    // Query sanity limits
    private final Long subQueryTermLimit;
    private final Long imhotepLocalTempFileSizeLimit;
    private final Long imhotepDaemonTempFileSizeLimit;
    private final Integer groupLimit;

    // IQL2 Imhotep-based state
    private final ImhotepClient imhotepClient;
    private final Map<String, Set<String>> keywordAnalyzerWhitelist;
    private final Map<String, Set<String>> datasetToIntFields;
    private final Map<String, DatasetDimensions> dimensions;

    // Query output state
    private final PrintWriter outputStream;
    private final QueryInfo queryInfo;
    private final TreeTimer timer;

    // Query inputs
    private final String username;
    private final String query;
    private final int version;
    private final boolean isStream;
    private final boolean skipValidation;
    private final WallClock clock;

    private boolean ran = false;
    private long queryStartTimestamp = -1L;

    public SelectQueryExecution(
            final QueryCache queryCache,
            final ExecutionManager executionManager,
            final Long subQueryTermLimit,
            final Long imhotepLocalTempFileSizeLimit,
            final Long imhotepDaemonTempFileSizeLimit,
            final Integer groupLimit, ImhotepClient imhotepClient,
            final Map<String, Set<String>> keywordAnalyzerWhitelist,
            final Map<String, Set<String>> datasetToIntFields,
            final Map<String, DatasetDimensions> dimensions,
            final PrintWriter outputStream,
            final QueryInfo queryInfo,
            final TreeTimer timer,
            final String username,
            final String query,
            final int version,
            final boolean isStream,
            final boolean skipValidation,
            final WallClock clock
    ) {
        this.outputStream = outputStream;
        this.queryInfo = queryInfo;
        this.query = query;
        this.version = version;
        this.username = username;
        this.timer = timer;
        this.isStream = isStream;
        this.skipValidation = skipValidation;
        this.groupLimit = groupLimit;
        this.clock = clock;
        this.keywordAnalyzerWhitelist = keywordAnalyzerWhitelist;
        this.datasetToIntFields = datasetToIntFields;
        this.imhotepClient = imhotepClient;
        this.executionManager = executionManager;
        this.subQueryTermLimit = subQueryTermLimit;
        this.dimensions = dimensions;
        this.queryCache = queryCache;
        this.imhotepLocalTempFileSizeLimit = imhotepLocalTempFileSizeLimit;
        this.imhotepDaemonTempFileSizeLimit = imhotepDaemonTempFileSizeLimit;
    }

    public long getQueryStartTimestamp() {
        return queryStartTimestamp;
    }

    public void processSelect() throws TimeoutException, IOException, ImhotepOutOfMemoryException {
        // .. just in case.
        synchronized (this) {
            if (ran) {
                throw new IllegalArgumentException("Cannot run multiple times!");
            }
            ran = true;
        }

        final ExecutionManager.QueryTracker queryTracker = executionManager.queryStarted(query, username);
        try {
            timer.push("Acquire concurrent query lock");
            queryTracker.acquireLocks(); // blocks and waits if necessary
            final long startTime = clock.currentTimeMillis();
            timer.pop();
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

            final NumDocLimitingProgressCallback numDocLimitingProgressCallback = new NumDocLimitingProgressCallback(50000000000L);
            final EventStreamProgressCallback eventStreamProgressCallback = new EventStreamProgressCallback(isStream, outputStream);
            final ProgressCallback progressCallback = CompositeProgressCallback.create(numDocLimitingProgressCallback, eventStreamProgressCallback);

            final SelectExecutionInformation execInfo = executeSelect(queryInfo, query, version == 1, countingOut, progressCallback, new com.indeed.squall.iql2.language.compat.Consumer<String>() {
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
                headerMap.put("IQL-Newest-Shard", ISODateTimeFormat.dateTime().print(execInfo.newestShard()));
                headerMap.put("IQL-Imhotep-Temp-Bytes-Written", execInfo.imhotepTempBytesWritten);
                headerMap.put("Imhotep-Session-IDs", execInfo.sessionIds);
                headerMap.put("IQL-Execution-Time", ISODateTimeFormat.dateTime().print(startTime));
                if (!warnings.isEmpty()) {
                    headerMap.put("IQL-Warning", Joiner.on('\n').join(warnings));
                }
                outputStream.println("data: " + OBJECT_MAPPER.writeValueAsString(headerMap));
                outputStream.println();


                outputStream.println("event: complete");
                outputStream.println("data: :)");
                outputStream.println();
            }
        } finally {
            if (!queryTracker.isAsynchronousRelease()) {
                queryTracker.close();
            }
        }
    }

    private void extractCompletedQueryInfoData(SelectExecutionInformation execInfo, Set<String> warnings, CountingConsumer<String> countingOut) {
        int shardCount = 0;
        Duration totalShardPeriod = Duration.ZERO;
        for (final List<String> shardList : execInfo.perDatasetShardIds().values()) {
            shardCount += shardList.size();
            for (final String shardID : shardList) {
                final ShardInfo.DateTimeRange shardInfo = ShardInfo.parseDateTime(shardID);
                totalShardPeriod = totalShardPeriod.plus(new Duration(shardInfo.start, shardInfo.end));
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

        if (execInfo.rowLimit == queryInfo.rows) {
            warnings.add(String.format("Only first %d rows returned sorted on the last group by column", execInfo.rowLimit));
        }
    }

    // TODO: These parameters are nuts
    private SelectExecutionInformation executeSelect(
            final QueryInfo queryInfo,
            final String q,
            final boolean useLegacy,
            final Consumer<String> out,
            final ProgressCallback progressCallback,
            final com.indeed.squall.iql2.language.compat.Consumer<String> warn
    ) throws IOException, ImhotepOutOfMemoryException {
        timer.push(q);

        timer.push("parse query");
        final Queries.ParseResult parseResult = Queries.parseQuery(q, useLegacy, keywordAnalyzerWhitelist, datasetToIntFields, warn, clock);
        timer.pop();

        {
            queryInfo.statementType = "select";

            final Map<String, String> upperCaseToActualDataset = new HashMap<>();
            for (final String dataset : Session.getDatasets(imhotepClient)) {
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
        }

        final SelectExecutionInformation result = new ParsedQueryExecution(parseResult.inputStream, out, warn, progressCallback, parseResult.query, groupLimit).executeParsedQuery();
        timer.pop();

        return result;
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
            final ListMultimap<String, List<ShardIdWithVersion>> allShardsUsed = ArrayListMultimap.create();

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
                                                if (subQueryTermLimit > 0 && terms.size() + stringTerms.size() >= subQueryTermLimit) {
                                                    throw new IllegalStateException("Sub query cannot have more than [" + subQueryTermLimit + "] terms!");
                                                }
                                                final String term = s.split("\t")[0];
                                                try {
                                                    terms.add(Long.parseLong(term));
                                                } catch (NumberFormatException e) {
                                                    stringTerms.add(term);
                                                }
                                            }
                                        }, warn, new NoOpProgressCallback(), q, groupLimit).executeParsedQuery();
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
                                    filters.add(new DocFilter.StringFieldIn(keywordAnalyzerWhitelist, scopedField.field, terms));
                                } else if (!p.getFirst().isEmpty()) {
                                    filters.add(new DocFilter.IntFieldIn(scopedField.field, p.getFirst()));
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
            final List<Command> commands = Queries.queryCommands(query);
            timer.pop();

            if (!skipValidation) {
                timer.push("validate commands");
                final Set<String> errors = new HashSet<>();
                final Set<String> warnings = new HashSet<>();
                CommandValidator.validate(commands, imhotepClient, query, dimensions, datasetToIntFields, errors, warnings);

                if (errors.size() != 0) {
                    throw new IllegalArgumentException("Errors found when validating query: " + errors);
                }
                if (warnings.size() != 0) {
                    for (String warning : warnings) {
                        warn.accept(warning);
                    }
                }
                timer.pop();
            }

            final ComputeCacheKey computeCacheKey = computeCacheKey(timer, query, commands, imhotepClient);
            final Map<String, List<ShardIdWithVersion>> datasetToChosenShards = Collections.unmodifiableMap(computeCacheKey.datasetToChosenShards);
            allShardsUsed.putAll(Multimaps.forMap(datasetToChosenShards));

            final AtomicBoolean errorOccurred = new AtomicBoolean(false);

            cacheKeys.add(computeCacheKey.rawHash);

            Consumer<String> out = externalOutput;

            try (final Closer closer = Closer.create()) {
                if (queryCache.isEnabled()) {
                    timer.push("cache check");
                    final boolean isCached = queryCache.isFileCached(computeCacheKey.cacheFileName);
                    timer.pop();

                    queryCached.put(query, isCached);

                    if (isCached) {
                        timer.push("read cache");
                        // TODO: Don't have this hack
                        progressCallback.startCommand(null, null, true);
                        sendCachedQuery(computeCacheKey.cacheFileName, out, query.rowLimit, queryCache);
                        timer.pop();
                        return new SelectExecutionInformation(allShardsUsed, queryCached, totalBytesWritten[0], cacheKeys, Collections.<String>emptyList(), 0, 0, 0,
                                originalQuery.rowLimit.isPresent() ? originalQuery.rowLimit.get() : -1);
                    } else {
                        final Consumer<String> oldOut = out;
                        final Path tmpFile = Files.createTempFile("query", ".cache.tmp");
                        final File cacheFile = tmpFile.toFile();
                        final BufferedWriter cacheWriter = new BufferedWriter(new FileWriter(cacheFile));
                        closer.register(new Closeable() {
                            @Override
                            public void close() throws IOException {
                                // TODO: Do this stuff asynchronously
                                cacheWriter.close();
                                if (!errorOccurred.get()) {
                                    queryCache.writeFromFile(computeCacheKey.cacheFileName, cacheFile);
                                }
                                if (!cacheFile.delete()) {
                                    log.warn("Failed to delete  " + cacheFile);
                                }
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
                            }
                        }
                    };
                }

                final ObjectMapper objectMapper = new ObjectMapper();
                if (log.isDebugEnabled()) {
                    log.debug("commands = " + commands);
                    for (final Command command : commands) {
                        log.debug("command = " + command);
                        final String s = objectMapper.writeValueAsString(command);
                        log.debug("s = " + s);
                    }
                    final String commandList = objectMapper.writeValueAsString(commands);
                    log.debug("commandList = " + commandList);
                }

                final Map<String, Object> request = new HashMap<>();
                request.put("datasets", Queries.createDatasetMap(inputStream, query));
                request.put("commands", commands);

                request.put("groupLimit", groupLimit);

                final JsonNode requestJson = OBJECT_MAPPER.valueToTree(request);

                final InfoCollectingProgressCallback infoCollectingProgressCallback = new InfoCollectingProgressCallback();
                final ProgressCallback compositeProgressCallback = CompositeProgressCallback.create(progressCallback, infoCollectingProgressCallback);
                try {
                    final Session.CreateSessionResult createResult = Session.createSession(imhotepClient, datasetToChosenShards, requestJson, closer, out, dimensions, timer, compositeProgressCallback, imhotepLocalTempFileSizeLimit, imhotepDaemonTempFileSizeLimit, clock, username);
                    final SelectExecutionInformation selectExecutionInformation = new SelectExecutionInformation(
                            allShardsUsed,
                            queryCached,
                            createResult.tempFileBytesWritten + totalBytesWritten[0],
                            cacheKeys,
                            infoCollectingProgressCallback.getSessionIds(),
                            infoCollectingProgressCallback.getTotalNumDocs(),
                            infoCollectingProgressCallback.getMaxNumGroups(),
                            infoCollectingProgressCallback.getMaxConcurrentSessions(),
                            originalQuery.rowLimit.isPresent() ? originalQuery.rowLimit.get() : -1
                    );
                    return selectExecutionInformation;
                } catch (Exception e) {
                    errorOccurred.set(true);
                    throw Throwables.propagate(e);
                }
            }
        }
    }

    public static ComputeCacheKey computeCacheKey(TreeTimer timer, Query query, List<Command> commands, ImhotepClient imhotepClient) {
        timer.push("compute dataset normalization");
        final Set<String> datasets = Session.getDatasets(imhotepClient);
        final Map<String, String> upperCaseToActualDataset = Maps.newHashMapWithExpectedSize(datasets.size());
        for (final String dataset : datasets) {
            upperCaseToActualDataset.put(dataset.toUpperCase(), dataset);
        }
        timer.pop();

        timer.push("compute hash");
        final Set<Pair<String, String>> shards = Sets.newHashSet();
        final Set<DatasetWithTimeRangeAndAliases> datasetsWithTimeRange = Sets.newHashSet();
        final Map<String, List<ShardIdWithVersion>> datasetToChosenShards = Maps.newHashMap();
        for (final Dataset dataset : query.datasets) {
            timer.push("get chosen shards");
            final String actualDataset = upperCaseToActualDataset.get(dataset.dataset.unwrap());
            final String sessionName = dataset.alias.or(dataset.dataset).unwrap();
            final List<ShardIdWithVersion> chosenShards = imhotepClient.sessionBuilder(actualDataset, dataset.startInclusive.unwrap(), dataset.endExclusive.unwrap()).getChosenShards();
            timer.pop();
            for (final ShardIdWithVersion chosenShard : chosenShards) {
                // This needs to be associated with the session name, not just the actualDataset.
                shards.add(Pair.of(sessionName, chosenShard.getShardId() + "-" + chosenShard.getVersion()));
            }
            final Set<FieldAlias> fieldAliases = Sets.newHashSet();
            for (final Map.Entry<Positioned<String>, Positioned<String>> e : dataset.fieldAliases.entrySet()) {
                fieldAliases.add(new FieldAlias(e.getValue().unwrap(), e.getKey().unwrap()));
            }
            datasetsWithTimeRange.add(new DatasetWithTimeRangeAndAliases(actualDataset, dataset.startInclusive.unwrap().getMillis(), dataset.endExclusive.unwrap().getMillis(), fieldAliases));
            final List<ShardIdWithVersion> oldShards = datasetToChosenShards.put(sessionName, chosenShards);
            if (oldShards != null) {
                throw new IllegalArgumentException("Overwrote shard list for " + sessionName);
            }
        }
        final String queryHash = computeQueryHash(commands, query.rowLimit, shards, datasetsWithTimeRange, 11);
        final String cacheFileName = "IQL2-" + queryHash + ".tsv";
        timer.pop();

        return new ComputeCacheKey(datasetToChosenShards, queryHash, cacheFileName);
    }

    private static void sendCachedQuery(String cacheFile, Consumer<String> out, Optional<Integer> rowLimit, QueryCache queryCache) throws IOException {
        final int limit = rowLimit.or(Integer.MAX_VALUE);
        int rowsWritten = 0;
        try (final BufferedReader stream = new BufferedReader(new InputStreamReader(queryCache.getInputStream(cacheFile)))) {
            String line;
            while ((line = stream.readLine()) != null) {
                out.accept(line);
                rowsWritten += 1;
                if (rowsWritten >= limit) {
                    break;
                }
            }
        }
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
        public @Nullable
        String statementType;
        public @Nullable
        Set<String> datasets;
        public @Nullable
        Duration totalDatasetRange; // SUM(dataset (End - Start))
        public @Nullable Duration totalShardPeriod; // SUM(shard (end-start))
        public @Nullable Long ftgsMB;
        public @Nullable
        Collection<String> sessionIDs;
        public @Nullable Integer numShards;
        public @Nullable Long numDocs;
        public @Nullable Boolean cached;
        public @Nullable Integer rows;
        public @Nullable Set<String> cacheHashes;
        public @Nullable Integer maxGroups;
        public @Nullable Integer maxConcurrentSessions;
    }

    private static class SelectExecutionInformation {
        public final Multimap<String, List<ShardIdWithVersion>> shards;
        public final Map<Query, Boolean> queryCached;
        public final long imhotepTempBytesWritten;
        public final Set<String> cacheKeys;

        public final List<String> sessionIds;
        public final long totalNumDocs;
        public final int maxNumGroups;
        public final int maxConcurrentSessions;
        public final int rowLimit;

        private SelectExecutionInformation(Multimap<String, List<ShardIdWithVersion>> shards, Map<Query, Boolean> queryCached, long imhotepTempBytesWritten, Set<String> cacheKeys, List<String> sessionIds,
                                           long totalNumDocs, int maxNumGroups, int maxConcurrentSessions, int rowLimit) {
            this.shards = shards;
            this.queryCached = queryCached;
            this.imhotepTempBytesWritten = imhotepTempBytesWritten;
            this.cacheKeys = ImmutableSet.copyOf(cacheKeys);
            this.sessionIds = ImmutableList.copyOf(sessionIds);
            this.totalNumDocs = totalNumDocs;
            this.maxNumGroups = maxNumGroups;
            this.maxConcurrentSessions = maxConcurrentSessions;
            this.rowLimit = rowLimit;
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
            return Multimaps.transformValues(shards, new Function<List<ShardIdWithVersion>, List<String>>() {
                public List<String> apply(List<ShardIdWithVersion> shardIdWithVersions) {
                    return ShardIdWithVersion.keepShardIds(shardIdWithVersions);
                }
            });
        }

        public long newestShard() {
            long newest = -1;
            for (final List<ShardIdWithVersion> shardset : shards.values()) {
                for (final ShardIdWithVersion shard : shardset) {
                    newest = Math.max(newest, shard.getVersion());
                }
            }
            if (newest == -1) {
                throw new IllegalArgumentException("No shards!");
            }
            return DateTimeFormat.forPattern("yyyyMMddHHmmss").parseMillis(String.valueOf(newest));
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
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
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
        public final Map<String, List<ShardIdWithVersion>> datasetToChosenShards;
        public final String rawHash;
        public final String cacheFileName;

        private ComputeCacheKey(Map<String, List<ShardIdWithVersion>> datasetToChosenShards, String rawHash, String cacheFileName) {
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
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
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
