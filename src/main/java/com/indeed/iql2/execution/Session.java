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

package com.indeed.iql2.execution;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.StrictCloser;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.io.RequestTools;
import com.indeed.imhotep.io.SingleFieldRegroupTools;
import com.indeed.imhotep.metrics.aggregate.AggregateStatTree;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.metadata.DatasetMetadata;
import com.indeed.iql.metadata.FieldType;
import com.indeed.iql2.Formatter;
import com.indeed.iql2.MathUtils;
import com.indeed.iql2.execution.commands.Command;
import com.indeed.iql2.execution.commands.GetGroupStats;
import com.indeed.iql2.execution.commands.SimpleIterate;
import com.indeed.iql2.execution.groupkeys.GroupKeySets;
import com.indeed.iql2.execution.groupkeys.sets.DumbGroupKeySet;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.groupkeys.sets.MaskingGroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.execution.progress.ProgressCallback;
import com.indeed.iql2.language.query.Queries;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.util.logging.TracingTreeTimer;
import io.opentracing.ActiveSpan;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author jwolfe
 */
public class Session {

    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    private static final Logger log = Logger.getLogger(Session.class);

    public GroupKeySet groupKeySet = DumbGroupKeySet.empty();
    public final Map<String, SavedGroupStats> savedGroupStats = Maps.newHashMap();
    public int currentDepth = 0;
    // Here metric totals will be saved in case of no-regroup query.
    // Saving it to not calculate this stats twice (as totals and as query results)
    private double[] statsTotals;

    public final Map<String, ImhotepSessionInfo> sessions;
    public final TracingTreeTimer timer;
    private final ProgressCallback progressCallback;
    public final int groupLimit;
    private final long firstStartTimeMillis;
    public final Set<String> options;
    private final FieldType defaultFieldType;
    public final ResultFormat resultFormat;
    public final Formatter formatter;
    public final int iqlVersion;

    // Does not count group zero.
    // Exactly equivalent to maxGroup.
    public int numGroups = 1;

    public static final String INFINITY_SYMBOL = "∞";
    public static final String DEFAULT_FORMAT_STRING = "#.#######";
    public static final ThreadLocal<DecimalFormat> DEFAULT_DECIMAL_FORMAT = new ThreadLocal<DecimalFormat>() {
        @Override
        protected DecimalFormat initialValue() {
            return new DecimalFormat(DEFAULT_FORMAT_STRING);
        }
    };

    public Session(
            Map<String, ImhotepSessionInfo> sessions,
            TracingTreeTimer timer,
            ProgressCallback progressCallback,
            @Nullable Integer groupLimit,
            long firstStartTimeMillis,
            final Set<String> options,
            final FieldType defaultFieldType,
            final ResultFormat resultFormat,
            final int iqlVersion
    ) {
        this.sessions = sessions;
        this.timer = timer;
        this.progressCallback = progressCallback;
        this.groupLimit = groupLimit == null ? -1 : groupLimit;
        this.firstStartTimeMillis = firstStartTimeMillis;
        this.options = options;
        this.defaultFieldType = defaultFieldType;
        this.resultFormat = resultFormat;
        this.iqlVersion = iqlVersion;
        this.formatter = Formatter.forFormat(resultFormat);
    }

    public static class CreateSessionResult {
        public final long tempFileBytesWritten;
        @Nullable
        public final PerformanceStats imhotepPerformanceStats;
        public final Optional<double[]> totals;

        CreateSessionResult(
                final long tempFileBytesWritten,
                final PerformanceStats imhotepPerformanceStats,
                final Optional<double[]> totals) {
            this.tempFileBytesWritten = tempFileBytesWritten;
            this.imhotepPerformanceStats = imhotepPerformanceStats;
            this.totals = totals;
        }
    }

    public static CreateSessionResult createSession(
            final ImhotepClient client,
            final Integer groupLimit,
            final Set<String> optionsSet,
            final List<com.indeed.iql2.language.commands.Command> commands,
            final Optional<List<com.indeed.iql2.language.AggregateMetric>> totals,
            final List<Queries.QueryDataset> datasets,
            final StrictCloser strictCloser,
            final Consumer<String> out,
            final TracingTreeTimer treeTimer,
            final ProgressCallback progressCallback,
            final Long imhotepLocalTempFileSizeLimit,
            final Long imhotepDaemonTempFileSizeLimit,
            final String username,
            final FieldType defaultFieldType,
            final ResultFormat resultFormat,
            final int iqlVersion
    ) throws ImhotepOutOfMemoryException, IOException {
        final Map<String, ImhotepSessionInfo> sessions = Maps.newLinkedHashMap();

        final List<String> optionsList = new ArrayList<>(optionsSet);

        final boolean requestRust = optionsSet.contains(QueryOptions.USE_RUST_DAEMON);
        final boolean useAsync = optionsSet.contains(QueryOptions.Experimental.ASYNC);

        progressCallback.startSession(Optional.of(commands.size()));
        progressCallback.preSessionOpen(datasets);

        treeTimer.push("createSubSessions");
        final long firstStartTimeMillis = createSubSessions(client, requestRust, useAsync, datasets,
                strictCloser, sessions, treeTimer, imhotepLocalTempFileSizeLimit, imhotepDaemonTempFileSizeLimit, username, progressCallback);
        progressCallback.sessionsOpened(sessions);
        treeTimer.pop();

        final Session session = new Session(sessions, treeTimer, progressCallback, groupLimit, firstStartTimeMillis, optionsSet, defaultFieldType, resultFormat, iqlVersion);
        for (int i = 0; i < commands.size(); i++) {
            final com.indeed.iql2.language.commands.Command command = commands.get(i);
            final Tracer tracer = GlobalTracer.get();
            try (final ActiveSpan activeSpan = tracer.buildSpan(command.getClass().getSimpleName()).withTag("details", command.toString()).startActive()) {
                final boolean isLast = i == commands.size() - 1;
                if (isLast) {
                    session.evaluateCommandToOutput(command, out, optionsList);
                } else {
                    session.evaluateCommand(command, optionsList);
                }
                activeSpan.setTag("numgroups", session.numGroups);
                if (session.numGroups == 0) {
                    break;
                }
            }

            // Move to beginning of the loop if we ever start to allow the last
            // command to leave the session in whatever state it wants to, to shave
            // off some unnecessary operations.
            if (optionsSet.contains(QueryOptions.PARANOID)) {
                session.ensureNoStats();
            }
        }

        if (optionsList.contains(QueryOptions.DIE_AT_END)) {
            throw new RuntimeException("You have requested me to fail.");
        }

        final double[] totalValues;
        if (totals.isPresent()) {
            totalValues = new double[totals.get().size()];
            for (int i = 0; i < totals.get().size(); i++) {
                final AggregateMetric metric = totals.get().get(i).toExecutionMetric(session::namedMetricLookup, session.groupKeySet);
                if (metric.needStats()) {
                    // Something went wrong!
                    // Everything must be precalculated on during query execution.
                    throw new IllegalStateException("Total stats for query expected to be precalculated");
                }
                // first param in getGroupStats can be any since metric.needStats() == false.
                final double[] groupStats = metric.getGroupStats(null, 2);
                totalValues[i] = groupStats[1];
            }
        } else {
            totalValues = session.statsTotals;
        }

        long tempFileBytesWritten = 0L;
        for (final ImhotepSessionInfo sessionInfo : session.sessions.values()) {
            tempFileBytesWritten += sessionInfo.session.getTempFilesBytesWritten();
        }

        treeTimer.push("get performance stats");
        final PerformanceStats.Builder performanceStats = PerformanceStats.builder();
        try {
            // Close sessions and get performance stats
            for (final ImhotepSessionInfo sessionInfo : session.sessions.values()) {
                final PerformanceStats sessionPerformanceStats = sessionInfo.session.closeAndGetPerformanceStats();
                if (sessionPerformanceStats != null) {
                    performanceStats.add(sessionPerformanceStats);
                }
            }
        } catch (Exception e) {
            log.error("Exception trying to close Imhotep sessions", e);
        }
        treeTimer.pop();

        return new CreateSessionResult(tempFileBytesWritten, performanceStats.build(), Optional.fromNullable(totalValues));
    }

    private void ensureNoStats() {
        for (final ImhotepSessionInfo session : sessions.values()) {
            if (session.session.getNumStats() != 0) {
                throw new IllegalStateException("At start of command execution, session " + session.displayName + " does not have zero stats!");
            }
        }
    }

    private static long createSubSessions(
            final ImhotepClient client,
            final boolean requestRust,
            final boolean useAsync,
            final List<Queries.QueryDataset> sessionRequest,
            final StrictCloser strictCloser,
            final Map<String, ImhotepSessionInfo> sessions,
            final TracingTreeTimer treeTimer,
            final Long imhotepLocalTempFileSizeLimit,
            final Long imhotepDaemonTempFileSizeLimit,
            final String username,
            final ProgressCallback progressCallback
    ) throws ImhotepOutOfMemoryException {
        long firstStartTimeMillis = 0;
        for (int i = 0; i < sessionRequest.size(); i++) {
            final Queries.QueryDataset dataset = sessionRequest.get(i);
            final String imhotepDataset = dataset.dataset;
            Preconditions.checkNotNull(imhotepDataset, "Dataset does not exist: %s", dataset.name);
            treeTimer.push("session", "session:" + dataset.displayName);

            treeTimer.push("get dataset info");
            treeTimer.push("getDatasetShardInfo");
            final DatasetInfo datasetInfo = client.getDatasetInfo(imhotepDataset);
            treeTimer.pop();
            final Set<String> sessionIntFields = Sets.newHashSet(datasetInfo.getIntFields());
            final Set<String> sessionStringFields = Sets.newHashSet(datasetInfo.getStringFields());

            final DateTime startDateTime = parseDateTime(dataset.start);
            final DateTime endDateTime = parseDateTime(dataset.end);
            treeTimer.pop();
            treeTimer.push("build session");
            treeTimer.push("create session builder");
            final List<Shard> chosenShards = dataset.shards;
            if ((chosenShards == null) || chosenShards.isEmpty()) {
                throw new IqlKnownException.NoDataException("No shards: no data available for the requested dataset and time range."
                + " Dataset: " + dataset.name + ", start: " + startDateTime + ", end: " + endDateTime);
            }
            final ImhotepClient.SessionBuilder sessionBuilder = client
                .sessionBuilder(imhotepDataset, startDateTime, endDateTime)
                .username("IQL2:" + username)
                .shardsOverride(chosenShards)
                .localTempFileSizeLimit(imhotepLocalTempFileSizeLimit)
                .daemonTempFileSizeLimit(imhotepDaemonTempFileSizeLimit)
                .allowSessionForwarding(requestRust);
            treeTimer.pop();
            // TODO: message should be "build session builder (xxx shards on yyy daemons)"
            // but we can't get information about daemons count now
            // need to add method to RemoteImhotepMultiSession or to session builder.
            treeTimer.push("build session builder", "build session builder (" + chosenShards.size() + " shards)");
            ImhotepSession build = strictCloser.registerOrClose(sessionBuilder.build());
            treeTimer.pop();

            if (useAsync) {
                build = ((RemoteImhotepMultiSession) build).toAsync();
            }

            // Just in case they have resources, registerOrClose the wrapped session as well.
            // Double close() is supposed to be safe.
            final ImhotepSessionHolder session = strictCloser.registerOrClose(wrapSession(dataset.displayName, build));
            treeTimer.pop();

            progressCallback.sessionOpened(session);

            treeTimer.push("determine time range");
            final DateTime earliestStart = Ordering.natural().min(Iterables.transform(chosenShards, new Function<Shard, DateTime>() {
                public DateTime apply(Shard input) {
                    return input.getStart();
                }
            }));
            final DateTime latestEnd = Ordering.natural().max(Iterables.transform(chosenShards, new Function<Shard, DateTime>() {
                public DateTime apply(@Nullable Shard input) {
                    return input.getEnd();
                }
            }));
            treeTimer.pop();
            final String timeField = DatasetMetadata.TIME_FIELD_NAME;
            if (earliestStart.isBefore(startDateTime) || latestEnd.isAfter(endDateTime)) {
                treeTimer.push("regroup time range");
                session.pushStat(timeField);
                session.metricFilter(0, (int) (startDateTime.getMillis() / 1000), (int)((endDateTime.getMillis() - 1) / 1000), false);
                session.popStat();
                treeTimer.pop();
            }
            sessions.put(dataset.name, new ImhotepSessionInfo(session, dataset.displayName, sessionIntFields, sessionStringFields, startDateTime, endDateTime, timeField));
            if (i == 0) {
                firstStartTimeMillis = startDateTime.getMillis();
            }

            treeTimer.pop();
        }
        return firstStartTimeMillis;
    }

    private static ImhotepSessionHolder wrapSession(final String datasetName, final ImhotepSession build) {
        return new ImhotepSessionHolder(datasetName, build);
    }

    // this datetime is serialized by standard Datetime by iql2-language
    private static DateTime parseDateTime(String datetime) {
        try {
            return DateTime.parse(datetime);
        } catch (final IllegalArgumentException e) {
            throw Throwables.propagate(e);
        }
    }

    public void evaluateCommand(final com.indeed.iql2.language.commands.Command lCommand,
                                final List<String> options) throws ImhotepOutOfMemoryException, IOException {
        final String commandTreeString = lCommand.toString();
        timer.push("evaluateCommand " + lCommand.getClass().getSimpleName(), "evaluateCommand " + (commandTreeString.length() > 500 ? (commandTreeString.substring(0, 500) + "[...](log truncated)") : commandTreeString));
        try {
            final Command command = lCommand.toExecutionCommand(this::namedMetricLookup, groupKeySet, options);
            progressCallback.startCommand(this, command, false);
            try {
                command.execute(this);
            } finally {
                progressCallback.endCommand(this, command);
            }
        } finally {
            timer.pop();
        }
    }

    public void evaluateCommandToOutput(final com.indeed.iql2.language.commands.Command lCommand,
                                        final Consumer<String> out,
                                        final List<String> options) throws ImhotepOutOfMemoryException, IOException {
        timer.push("evaluateCommandToOutput " + lCommand.getClass().getSimpleName(), "evaluateCommandToOutput " + lCommand);
        try {
            final Command command = lCommand.toExecutionCommand(this::namedMetricLookup, groupKeySet, options);
            try {
                progressCallback.startCommand(this, command, true);
                if (command instanceof SimpleIterate) {
                    final SimpleIterate simpleIterate = (SimpleIterate) command;
                    final String[] formats = simpleIterate.formFormatStrings();
                    final SimpleIterate.ResultCollector collector =
                            new SimpleIterate.ResultCollector.Streaming(out, groupKeySet, formats, formatter);
                    simpleIterate.evaluate(this, collector);
                } else if (command instanceof GetGroupStats) {
                    final GetGroupStats getGroupStats = (GetGroupStats) command;
                    final double[][] results = getGroupStats.evaluate(this);
                    final StringBuilder sb = new StringBuilder();

                    final String[] formatStrings = new String[getGroupStats.metrics.size()];
                    for (int i = 0; i < formatStrings.length; i++) {
                        final Optional<String> opt = getGroupStats.formatStrings.get(i);
                        formatStrings[i] = opt.isPresent() ? opt.get() : null;
                    }

                    for (int group = 1; group <= numGroups; group++) {
                        if (!groupKeySet.isPresent(group)) {
                            continue;
                        }
                        GroupKeySets.appendTo(sb, groupKeySet, group, formatter.getSeparator());
                        if (sb.length() == 0) {
                            sb.append(formatter.getSeparator());
                        }
                        writeDoubleStatsWithFormatString(results, group, formatStrings, sb, formatter.getSeparator());
                        sb.setLength(sb.length() - 1);
                        out.accept(sb.toString());
                        sb.setLength(0);
                    }

                    if (groupKeySet.previous() == null) {
                        // It's query without regroup.
                        // Saving totalStats since it's not calculated otherwise.
                        statsTotals = new double[results.length];
                        for (int i = 0; i < statsTotals.length; i++) {
                            statsTotals[i] = results[i][numGroups];
                        }
                    }
                } else {
                    throw new IllegalArgumentException("Don't know how to evaluate [" + command + "] to TSV");
                }
            } finally {
                progressCallback.endCommand(this, command);
            }
        } finally {
            timer.pop();
        }
    }

    public static void writeDoubleStatWithFormatString(final double stat, final String formatString, final StringBuilder sb, final char separator) {
        if (formatString != null) {
            sb.append(String.format(formatString, stat));
        } else {
            final long value = MathUtils.integralDoubleAsLong(stat);
            if (value != Long.MAX_VALUE) {
                sb.append(value);
            } else {
                sb.append(Double.isNaN(stat) ? "NaN" : DEFAULT_DECIMAL_FORMAT.get().format(stat));
            }
        }
        sb.append(separator);
    }

    public static void writeDoubleStatsWithFormatString(final double[] stats, final String[] formatStrings, final StringBuilder sb, final char separator) {
        for (int i = 0; i < stats.length; i++) {
            writeDoubleStatWithFormatString(stats[i], formatStrings[i], sb, separator);
        }
    }

    public static void writeDoubleStatsWithFormatString(
            final double[][] stats,
            final int group,
            final String[] formatStrings,
            final StringBuilder sb,
            final char separator) {
        for (int i = 0; i < stats.length; i++) {
            writeDoubleStatWithFormatString(stats[i][group], formatStrings[i], sb, separator);
        }
    }

    public void registerMetrics(Map<QualifiedPush, Integer> metricIndexes, Iterable<AggregateMetric> metrics, Iterable<AggregateFilter> filters) {
        for (final AggregateMetric metric : metrics) {
            metric.register(metricIndexes, groupKeySet);
        }
        for (final AggregateFilter filter : filters) {
            filter.register(metricIndexes, groupKeySet);
        }
    }

    public void pushMetrics(Set<QualifiedPush> allPushes, Map<QualifiedPush, Integer> metricIndexes, Map<String, IntList> sessionMetricIndexes) throws ImhotepOutOfMemoryException {
        pushMetrics(allPushes, metricIndexes, sessionMetricIndexes, false);
    }

    public void pushMetrics(Set<QualifiedPush> allPushes, Map<QualifiedPush, Integer> metricIndexes, Map<String, IntList> sessionMetricIndexes, boolean dryRun) throws ImhotepOutOfMemoryException {
        int numStats = 0;
        for (final QualifiedPush push : allPushes) {
            final int index = numStats++;
            List<String> pushes = new ArrayList<>(push.pushes);
            metricIndexes.put(push, index);
            final String sessionName = push.sessionName;
            if (!dryRun) {
                sessions.get(sessionName).session.pushStats(pushes);
            }
            // TODO: Terrible variable names.
            IntList sessionMetricIndex = sessionMetricIndexes.get(sessionName);
            if (sessionMetricIndex == null) {
                sessionMetricIndex = new IntArrayList();
                sessionMetricIndexes.put(sessionName, sessionMetricIndex);
            }
            sessionMetricIndex.add(index);
        }
    }

    public Map<QualifiedPush, AggregateStatTree> pushMetrics(final Set<QualifiedPush> allPushes) throws ImhotepOutOfMemoryException {
        timer.push("pushing metrics");
        final Map<QualifiedPush, AggregateStatTree> statResults = new HashMap<>();
        final Object2IntOpenHashMap<String> sessionToPushCount = new Object2IntOpenHashMap<>();
        // TODO: Parallelize across sessions
        for (final QualifiedPush push : allPushes) {
            final ImhotepSessionHolder session = sessions.get(push.sessionName).session;
            pushStatsWithTimer(session, push.pushes, timer);
            statResults.put(push, session.aggregateStat(sessionToPushCount.add(push.sessionName, 1)));
        }
        timer.pop();
        return statResults;
    }

    public static void pushStatsWithTimer(final ImhotepSessionHolder session, final List<String> pushes, final TracingTreeTimer timer) throws ImhotepOutOfMemoryException {
        timer.push("pushStats", "pushStats ('" + String.join("', '", pushes) + "')");
        session.pushStats(pushes);
        timer.pop();
    }

    public long getFirstStartTimeMillis() {
        return firstStartTimeMillis;
    }

    public long getLongestSessionDistance() {
        long distance = 0;
        for (final ImhotepSessionInfo sessionInfo : sessions.values()) {
            distance = Math.max(distance, sessionInfo.endTime.getMillis()-sessionInfo.startTime.getMillis());
        }
        return distance;
    }

    public long getLatestEnd() {
        return Ordering.natural().max(Iterables.transform(sessions.values(), new Function<ImhotepSessionInfo, Long>() {
            public Long apply(ImhotepSessionInfo input) {
                return input.endTime.getMillis();
            }
        }));
    }

    public long getEarliestStart() {
        return Ordering.natural().min(Iterables.transform(sessions.values(), new Function<ImhotepSessionInfo, Long>() {
            public Long apply(ImhotepSessionInfo input) {
                return input.startTime.getMillis();
            }
        }));
    }

    public int performTimeRegroup(
            final long start,
            final long end,
            final long unitSize,
            final Optional<FieldSet> fieldOverride,
            final boolean isRelative,
            final boolean deleteEmptyGroups) throws ImhotepOutOfMemoryException {
        timer.push("performTimeRegroup");
        final int oldNumGroups = this.numGroups;
        // TODO: Parallelize
        final int maxPossibleGroups = (int) (oldNumGroups * Math.ceil(((double) end - start) / unitSize));
        int newNumGroups = deleteEmptyGroups ? 0 : maxPossibleGroups;
        for (final ImhotepSessionInfo sessionInfo : sessions.values()) {
            timer.push("session", "session:" + sessionInfo.displayName);

            final ImhotepSessionHolder session = sessionInfo.session;
            final String fieldName;
            if (fieldOverride.isPresent()) {
                fieldName = fieldOverride.get().datasetFieldName(sessionInfo.displayName);
            } else {
                fieldName = sessionInfo.timeFieldName;
            }

            timer.push("pushStat");
            session.pushStat(fieldName);
            timer.pop();

            timer.push("metricRegroup");
            if (isRelative) {
                final long realStart = sessionInfo.startTime.getMillis();
                final long realEnd = sessionInfo.endTime.getMillis();
                session.metricRegroup(0, realStart / 1000, realEnd / 1000, unitSize / 1000, true);
            } else {
                session.metricRegroup(0, start / 1000, end / 1000, unitSize / 1000, true);
            }
            timer.pop();

            // request group count from imhotep only if we have to.
            if (newNumGroups < maxPossibleGroups) {
                timer.push("getNumGroups");
                final int groups = session.getNumGroups() - 1; // imhtotep groups are with zero group.
                newNumGroups = Math.max(newNumGroups, groups);
                timer.pop();
            }

            timer.push("popStat");
            session.popStat();
            timer.pop();

            timer.pop();
        }
        timer.pop();
        return newNumGroups;
    }

    public void densify(GroupKeySet groupKeySet) throws ImhotepOutOfMemoryException {
        timer.push("densify");
        final BitSet anyPresent = new BitSet();
        // TODO: Parallelize?
        for (Map.Entry<String, ImhotepSessionHolder> imhotepSessionEntry : getSessionsMapRaw().entrySet()) {
            timer.push("session", "session:" + getSessionsMap().get(imhotepSessionEntry.getKey()).displayName);

            final ImhotepSessionHolder session = imhotepSessionEntry.getValue();
            timer.push("push counts");
            session.pushStat("count()");
            timer.pop();

            timer.push("get counts");
            final long[] counts = session.getGroupStats(0);
            timer.pop();
            for (int i = 0; i < counts.length; i++) {
                if (counts[i] > 0L) {
                    anyPresent.set(i);
                }
            }

            timer.push("pop counts");
            session.popStat();
            timer.pop();

            timer.pop();
        }

        numGroups = groupKeySet.numGroups();
        log.debug("numGroups = " + numGroups);
        this.groupKeySet = new MaskingGroupKeySet(groupKeySet, anyPresent);
        this.currentDepth += 1;
        timer.pop();
    }

    public void assumeDense(GroupKeySet groupKeySet) {
        timer.push("assumeDense");
        this.numGroups = groupKeySet.numGroups();
        log.debug("numGroups = " + numGroups);
        this.groupKeySet = groupKeySet;
        this.currentDepth += 1;
        timer.pop();
    }

    public Map<String, ImhotepSessionHolder> getSessionsMapRaw() {
        final Map<String, ImhotepSessionHolder> sessionMap = Maps.newHashMap();
        for (final Map.Entry<String, ImhotepSessionInfo> entry : sessions.entrySet()) {
            sessionMap.put(entry.getKey(), entry.getValue().session);
        }
        return sessionMap;
    }

    private Map<String, ImhotepSessionInfo> getSessionsMap() {
        return Maps.newHashMap(sessions);
    }

    private FieldType getFieldType(final FieldSet field) {
        boolean hasIntField = false;
        boolean hasStrField = false;
        for (final ImhotepSessionInfo session : sessions.values()) {
            final String dataset = session.displayName;
            if (!field.containsDataset(dataset)) {
                continue;
            }
            hasIntField |= session.intFields.contains(field.datasetFieldName(dataset));
            hasStrField |= session.stringFields.contains(field.datasetFieldName(dataset));
        }
        if (hasIntField && !hasStrField) {
            return FieldType.Integer;
        }
        if (hasStrField && !hasIntField) {
            return FieldType.String;
        }
        if (!hasIntField && !hasStrField) {
            return null;
        }
        return defaultFieldType;
    }

    public boolean isIntField(final FieldSet field) {
        return FieldType.Integer.equals(getFieldType(field));
    }

    public boolean isStringField(final FieldSet field) {
        return FieldType.String.equals(getFieldType(field));
    }

    private PerGroupConstant namedMetricLookup(String name) {
        final SavedGroupStats savedStat = savedGroupStats.get(name);
        final int depthChange = currentDepth - savedStat.depth;
        final double[] stats = new double[numGroups + 1];
        for (int group = 1; group <= numGroups; group++) {
            GroupKeySet groupKeySet = this.groupKeySet;
            int index = group;
            for (int i = 0; i < depthChange; i++) {
                index = groupKeySet.parentGroup(index);
                groupKeySet = groupKeySet.previous();
            }
            stats[group] = savedStat.stats[index];
        }
        return new PerGroupConstant(stats);
    }

    public void checkGroupLimitWithoutLog(int numGroups) {
        if (groupLimit > 0 && numGroups > groupLimit) {
            throw new IqlKnownException.GroupLimitExceededException("Number of groups [" + numGroups + "] exceeds the group limit [" + groupLimit + "]");
        }
    }

    public void checkGroupLimit(int numGroups) {
        if (groupLimit > 0 && numGroups > groupLimit) {
            throw new IqlKnownException.GroupLimitExceededException("Number of groups [" + numGroups + "] exceeds the group limit [" + groupLimit + "]");
        }
        log.debug("checkGroupLimit(" + numGroups + ")");
    }

    // TODO: Parallelize across sessions?
    public void popStats() {
        timer.push("popStats");
        for (final ImhotepSessionInfo sessionInfo : sessions.values()) {
            timer.push("session", "session:" + sessionInfo.displayName);
            final ImhotepSessionHolder session = sessionInfo.session;
            final int numStats = session.getNumStats();
            for (int i = 0; i < numStats; i++) {
                session.popStat();
            }
            timer.pop();
        }
        timer.pop();
    }

    public SingleFieldRegroupTools.SingleFieldRulesBuilder createRuleBuilder(
            final FieldSet field,
            final boolean intType,
            final boolean inequality) {

        final Set<String> realFields = new HashSet<>();

        for (final String dataset : field.datasets()) {
            realFields.add(field.datasetFieldName(dataset));
        }

        if (realFields.size() > 1) {
            // several datasets with different field names. Cannot create rules on-the-fly.
            // Collecting single field rules for futher conversion.
            return new SingleFieldRegroupTools.SingleFieldRulesBuilder.SingleField();
        }

        // real field name match in all sessions.
        // Rules (in memory or cached in data stream) could be created once.
        final SingleFieldRegroupTools.FieldOptions realField =
                new SingleFieldRegroupTools.FieldOptions(realFields.iterator().next(), intType, inequality);
        return new SingleFieldRegroupTools.SingleFieldRulesBuilder.Cached(realField);
    }

    public void regroupWithSingleFieldRules(
            final SingleFieldRegroupTools.SingleFieldRulesBuilder builder,
            final FieldSet field,
            final boolean intType,
            final boolean inequality,
            final boolean errorOnCollisions
    ) throws ImhotepOutOfMemoryException {

        if (builder instanceof SingleFieldRegroupTools.SingleFieldRulesBuilder.Cached) {
            timer.push("regroupOnSingleField(GroupMultiRemapRuleSender)");
            // one real field, all sessions are remote.
            final RequestTools.GroupMultiRemapRuleSender sender =
                    ((SingleFieldRegroupTools.SingleFieldRulesBuilder.Cached) builder).createSender();
            for (final ImhotepSessionInfo sessionInfo : sessions.values()) {
                timer.push("session", "session: " + sessionInfo.displayName);
                sessionInfo.session.regroupWithSender(sender, errorOnCollisions);
                timer.pop();
            }
            timer.pop();
            return;
        }

        if (builder instanceof SingleFieldRegroupTools.SingleFieldRulesBuilder.SingleField) {
            // several real fields
            timer.push("regroupOnSingleField(several real field names)");

            final SingleFieldRegroupTools.SingleFieldMultiRemapRule[] rules =
                    ((SingleFieldRegroupTools.SingleFieldRulesBuilder.SingleField) builder).getRules();

            // Gather sessions with same real fields name together
            // to convert messages only once for each unique real field name.
            final Map<String, List<ImhotepSessionInfo>> realFieldToSessions = new HashMap<>();
            for (final ImhotepSessionInfo sessionInfo : sessions.values()) {
                final String realField = field.datasetFieldName(sessionInfo.displayName);
                if (!realFieldToSessions.containsKey(realField)) {
                    realFieldToSessions.put(realField, new ArrayList<>());
                }
                realFieldToSessions.get(realField).add(sessionInfo);
            }

            for (final Map.Entry<String, List<ImhotepSessionInfo>> entry : realFieldToSessions.entrySet()) {
                final String realField = entry.getKey();
                timer.push("real field", "real field: " + realField);
                final SingleFieldRegroupTools.FieldOptions realFieldOptions = new SingleFieldRegroupTools.FieldOptions(realField, intType, inequality);
                final Iterator<GroupMultiRemapMessage> messages =
                        Iterators.transform(Arrays.asList(rules).iterator(),
                                rule -> SingleFieldRegroupTools.marshal(rule, realFieldOptions));
                final RequestTools.GroupMultiRemapRuleSender sender = RequestTools.GroupMultiRemapRuleSender.cacheMessages(messages);
                for (final ImhotepSessionInfo sessionInfo : entry.getValue()) {
                    timer.push("session", "session:" + sessionInfo.displayName);
                    sessionInfo.session.regroupWithSender(sender, errorOnCollisions);
                    timer.pop();
                }
                timer.pop();
            }
            timer.pop();
            return;
        }

        throw new IllegalStateException("Unexpected builder type");
    }

    public void remapGroups(final int[] fromGroups, final int[] toGroups) throws ImhotepOutOfMemoryException {

        if (fromGroups.length != toGroups.length) {
            throw new IllegalStateException();
        }

        // TODO: Parallelize
        timer.push("remapGroups");
        for (final ImhotepSessionInfo sessionInfo : sessions.values()) {
            timer.push("session","session:" + sessionInfo.displayName);
            sessionInfo.session.regroup(fromGroups, toGroups, true);
            timer.pop();
        }
        timer.pop();
    }

    public void popStat() {
        // TODO: Parallelize
        timer.push("popStat");
        for (final ImhotepSessionInfo sessionInfo : sessions.values()) {
            timer.push("session", "session:" + sessionInfo.displayName);
            sessionInfo.session.popStat();
            timer.pop();
        }
        timer.pop();
    }

    public void intOrRegroup(String field, long[] terms, int targetGroup, int negativeGroup, int positiveGroup, Set<String> scope) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        timer.push("intOrRegroup", "intOrRegroup(" + terms.length + " terms)");
        for (final String s : scope) {
            if (sessions.containsKey(s)) {
                final ImhotepSessionInfo sessionInfo = sessions.get(s);
                timer.push("session", "session:" + sessionInfo.displayName);
                sessionInfo.session.intOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
                timer.pop();
            }
        }
        timer.pop();
    }

    public void stringOrRegroup(FieldSet field, String[] terms, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        timer.push("stringOrRegroup", "stringOrRegroup(" + terms.length + " terms)");
        for (final String dataset : field.datasets()) {
            if (sessions.containsKey(dataset)) {
                final ImhotepSessionInfo sessionInfo = sessions.get(dataset);
                timer.push("session", "session:" + sessionInfo.displayName);
                sessionInfo.session.stringOrRegroup(field.datasetFieldName(dataset), terms, targetGroup, negativeGroup, positiveGroup);
                timer.pop();
            }
        }
        timer.pop();
    }

    public void remapGroup(final int fromGroup, final int toGroup, final Set<String> scope) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        timer.push("regroup");
        for (final String s : scope) {
            if (sessions.containsKey(s)) {
                timer.push("session", "session:" + sessions.get(s).displayName);
                // remapping just one group, leaving other groups as-is
                sessions.get(s).session.regroup(new int[]{fromGroup}, new int[]{toGroup}, false);
                timer.pop();
            }
        }
        timer.pop();
    }

    public void regexRegroup(FieldSet field, String regex, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        timer.push("regexRegroup");
        for (final Map.Entry<String, Session.ImhotepSessionInfo> entry : sessions.entrySet()) {
            final String dataset = entry.getKey();
            if (field.containsDataset(dataset)) {
                final Session.ImhotepSessionInfo v = entry.getValue();
                timer.push("session", "session:" + entry.getValue().displayName);
                v.session.regexRegroup(field.datasetFieldName(dataset), regex, targetGroup, negativeGroup, positiveGroup);
                timer.pop();
            }
        }
        timer.pop();
    }

    public void randomRegroup(FieldSet field, boolean isIntField, String seed, double probability, int targetGroup, int positiveGroup, int negativeGroup) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        timer.push("randomRegroup");
        for (final Map.Entry<String, ImhotepSessionInfo> entry : sessions.entrySet()) {
            final String dataset = entry.getKey();
            if (field.containsDataset(dataset)) {
                timer.push("session", "session:" + entry.getValue().displayName);
                entry.getValue().session.randomRegroup(field.datasetFieldName(dataset), isIntField, seed, probability, targetGroup, negativeGroup, positiveGroup);
                timer.pop();
            }
        }
        timer.pop();
    }

    public long[] getSimpleDistinct(final String field, final String scope) {
        final long[] result = new long[numGroups+1];
        if (!sessions.containsKey(scope)) {
            return result; // or error?
        }

        final ImhotepSessionInfo info = sessions.get(scope);
        final ImhotepSessionHolder session = info.session;
        final boolean isIntField;
        if (info.intFields.contains(field)) {
            isIntField = true;
        } else if (info.stringFields.contains(field)) {
            isIntField = false;
        } else {
            return result; // or error?
        }
        timer.push("getSimpleDistinct", "getSimpleDistinct session:" + info.displayName);
        try (final GroupStatsIterator iterator = session.getDistinct(field, isIntField)) {
            timer.pop();
            final int size = Math.min(iterator.getNumGroups(), result.length);
            for (int i = 0; i < size; i++) {
                result[i] += iterator.nextLong();
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
        return result;
    }

    public void process(SessionCallback sessionCallback) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize, use different timers per thread and somehow merge them back into the parent without
        //       overcounting parallel time?
        for (final Map.Entry<String, ImhotepSessionInfo> entry : sessions.entrySet()) {
            sessionCallback.handle(timer, entry.getKey(), entry.getValue().session);
        }
    }

    private static class SessionIntIterationState {
        public final FTGSIterator iterator;
        private final IntList metricIndexes;
        @Nullable
        private final Integer presenceIndex;
        public final long[] statsBuff;
        public long nextTerm;
        public int nextGroup;

        private SessionIntIterationState(FTGSIterator iterator, IntList metricIndexes, @Nullable Integer presenceIndex, long[] statsBuff, long nextTerm, int nextGroup) {
            this.iterator = iterator;
            this.metricIndexes = metricIndexes;
            this.presenceIndex = presenceIndex;
            this.statsBuff = statsBuff;
            this.nextTerm = nextTerm;
            this.nextGroup = nextGroup;
        }

        static Optional<SessionIntIterationState> construct(
                Closer closer, ImhotepSessionHolder session, FieldSet field, IntList sessionMetricIndexes, @Nullable Integer presenceIndex,
                Optional<RemoteTopKParams> topKParams, Optional<Integer> ftgsRowLimit, Optional<long[]> termSubset) {
            final FTGSIterator it = closer.register(getFTGSIterator(session, field, true, topKParams, ftgsRowLimit, termSubset, Optional.<String[]>absent()));
            final int numStats = session.getNumStats();
            final long[] statsBuff = new long[numStats];
            while (it.nextField()) {
                while (it.nextTerm()) {
                    while (it.nextGroup()) {
                        it.groupStats(statsBuff);
                        return Optional.of(new SessionIntIterationState(it, sessionMetricIndexes, presenceIndex, statsBuff, it.termIntVal(), it.group()));
                    }
                }
            }
            return Optional.absent();
        }
    }

    public interface IntIterateCallback {
        void term(long term, long[] stats, int group);
        // return true if terms are expected in sorted order
        boolean needSorted();
        // if needGroup() returns false then group value is ignored inside term(...) method
        // and it's valid to pass any value as group
        boolean needGroup();
        // if needStats() returns false then stats value is ignored inside term(...) method
        // and it's valid to pass any array or null as stats
        boolean needStats();
    }

    /**
     * {@code metricIndexes} must be disjoint across sessions.
     */
    public static void iterateMultiInt(Map<String, ImhotepSessionHolder> sessions, Map<String, IntList> metricIndexes, Map<String, Integer> presenceIndexes, FieldSet field, IntIterateCallback callback, TracingTreeTimer timer,
                                       final Set<String> options) throws IOException {
        iterateMultiInt(sessions, metricIndexes, presenceIndexes, field, Optional.<RemoteTopKParams>absent(), Optional.<Integer>absent(), Optional.<long[]>absent(), callback, timer, options);
    }

    /**
     * {@code metricIndexes} must be disjoint across sessions.
     */
    public static void iterateMultiInt(
            Map<String, ImhotepSessionHolder> sessions, Map<String, IntList> metricIndexes, Map<String, Integer> presenceIndexes,
            FieldSet field, Optional<RemoteTopKParams> topKParams, Optional<Integer> ftgsRowLimit,
            Optional<long[]> termSubset, IntIterateCallback callback, TracingTreeTimer timer,
            final Set<String> options) throws IOException
    {
        if (iterateSimpleInt(sessions, metricIndexes, presenceIndexes, field, topKParams, ftgsRowLimit, termSubset, callback, timer, options)) {
            return;
        }

        int numMetrics = 0;
        for (final IntList metrics : metricIndexes.values()) {
            numMetrics += metrics.size();
        }
        try (final Closer closer = Closer.create()) {
            final Comparator<SessionIntIterationState> comparator = new Comparator<SessionIntIterationState>() {
                @Override
                public int compare(SessionIntIterationState x, SessionIntIterationState y) {
                    int r = Longs.compare(x.nextTerm, y.nextTerm);
                    if (r != 0) return r;
                    return Ints.compare(x.nextGroup, y.nextGroup);
                }
            };
            // TODO: Parallelize
            final PriorityQueue<SessionIntIterationState> pq = new PriorityQueue<>(sessions.size(), comparator);
            timer.push("request remote FTGS iterator");
            for (final String sessionName : sessions.keySet()) {
                final ImhotepSessionHolder session = sessions.get(sessionName);
                timer.push("session", "session:" + sessionName + ", field:" + field.datasetFieldName(sessionName));
                final IntList sessionMetricIndexes = Objects.firstNonNull(metricIndexes.get(sessionName), new IntArrayList());
                final Integer presenceIndex = presenceIndexes.get(sessionName);
                final Optional<SessionIntIterationState> constructed = SessionIntIterationState.construct(
                        closer, session, field, sessionMetricIndexes, presenceIndex, topKParams, ftgsRowLimit,
                        termSubset
                );
                if (constructed.isPresent()) {
                    pq.add(constructed.get());
                }
                timer.pop();
            }
            timer.pop();

            timer.push("consume FTGS iterator");
            final long[] realBuffer = new long[numMetrics + presenceIndexes.size()];
            final List<SessionIntIterationState> toEnqueue = Lists.newArrayList();
            while (!pq.isEmpty()) {
                toEnqueue.clear();
                Arrays.fill(realBuffer, 0);
                final SessionIntIterationState state1 = pq.poll();
                final long term = state1.nextTerm;
                final int group = state1.nextGroup;
                copyStats(state1, realBuffer);
                toEnqueue.add(state1);
                while (!pq.isEmpty() && pq.peek().nextTerm == term && pq.peek().nextGroup == group) {
                    final SessionIntIterationState state = pq.poll();
                    copyStats(state, realBuffer);
                    toEnqueue.add(state);
                }
                callback.term(term, realBuffer, group);
                for (final SessionIntIterationState state : toEnqueue) {
                    advanceAndEnqueue(state, pq);
                }
            }
            timer.pop();
        }
    }

    private static void advanceAndEnqueue(SessionIntIterationState state, PriorityQueue<SessionIntIterationState> pq) {
        final FTGSIterator iterator = state.iterator;
        if (iterator.nextGroup()) {
            state.nextGroup = iterator.group();
            iterator.groupStats(state.statsBuff);
            pq.add(state);
        } else {
            while (iterator.nextTerm()) {
                while (iterator.nextGroup()) {
                    state.nextTerm = iterator.termIntVal();
                    state.nextGroup = iterator.group();
                    iterator.groupStats(state.statsBuff);
                    pq.add(state);
                    return;
                }
            }
        }
    }

    /**
     * {@code state.metricIndexes} must be disjoint across sessions.
     */
    private static void copyStats(SessionIntIterationState state, long[] dst) {
        for (int i = 0; i < state.metricIndexes.size(); i++) {
            dst[state.metricIndexes.getInt(i)] = state.statsBuff[i];
        }
        if (state.presenceIndex != null) {
            dst[state.presenceIndex] = 1;
        }
    }

    // do simple processing if possible
    public static boolean iterateSimpleInt(
            final Map<String, ImhotepSessionHolder> sessions,
            final Map<String, IntList> metricIndexes,
            final Map<String, Integer> presenceIndexes,
            final FieldSet field,
            final Optional<RemoteTopKParams> topKParams,
            final Optional<Integer> ftgsRowLimit,
            final Optional<long[]> termSubset,
            final IntIterateCallback callback,
            final TracingTreeTimer timer,
            final Set<String> options)
    {
        if (!isSimple(sessions, metricIndexes, presenceIndexes, options)) {
            return false;
        }

        final ImhotepSessionHolder session = Iterables.getOnlyElement(sessions.values());
        final String dataset = session.getDatasetName();
        timer.push("request remote FTGS iterator for single session", "request remote FTGS iterator for single session: " + dataset + "." + field.datasetFieldName(dataset));

        try (final FTGSIterator ftgs =
                     createFTGSIterator(session, field, true,
                             topKParams, ftgsRowLimit,
                             termSubset, Optional.absent(), callback.needSorted())) {
            timer.pop();

            timer.push("consume FTGS iterator");

            if (!ftgs.nextField()) {
                throw new IllegalStateException("At least one field expected");
            }

            if (!ftgs.fieldIsIntType()) {
                throw new IllegalStateException("Field type mismatch");
            }

            final boolean needStats = callback.needStats();
            final long[] stats = needStats ? new long[ftgs.getNumStats()] : null;
            while (ftgs.nextTerm()) {
                final long term = ftgs.termIntVal();
                while (ftgs.nextGroup()) {
                    final int group = ftgs.group();
                    if (needStats) {
                        ftgs.groupStats(stats);
                    }
                    callback.term(term, stats, group);
                }
            }

            if (ftgs.nextField()) {
                throw new IllegalStateException("Exactly one field expected");
            }

            timer.pop();
        }
        return true;
    }

    // check if simple processing is possible
    // simple means one ImhotepSession, no presence indexes and stat indexes are the same as in session's FTGSIterator
    private static boolean isSimple(
            final Map<String, ImhotepSessionHolder> sessions,
            final Map<String, IntList> metricIndexes,
            final Map<String, Integer> presenceIndexes,
            final Set<String> options
    ) {
        if ((sessions.size() != 1) || (metricIndexes.size() != 1) || !presenceIndexes.isEmpty()) {
            return false;
        }

        if (!Iterables.getOnlyElement(sessions.keySet()).equals(Iterables.getOnlyElement(metricIndexes.keySet()))) {
            throw new IllegalStateException("Names for session and metric indexes don't match");
        }

        final IntList metricIndex = Iterables.getOnlyElement(metricIndexes.values());
        for (int i = 0; i < metricIndex.size(); i++) {
            if (metricIndex.getInt(i) != i) {
                return false;
            }
        }

        return true;
    }

    private static FTGSIterator createFTGSIterator(
            final ImhotepSessionHolder session,
            final FieldSet field,
            final boolean isIntField,
            final Optional<RemoteTopKParams> topKParams,
            final Optional<Integer> ftgsRowLimit,
            final Optional<long[]> intTerms,
            final Optional<String[]> stringTerms,
            final boolean isSorted
    ) {
        if (isIntField && intTerms.isPresent()) {
            return session.getSubsetFTGSIterator(Collections.singletonMap(field, intTerms.get()), Collections.emptyMap());
        } else if(!isIntField && stringTerms.isPresent()) {
            return session.getSubsetFTGSIterator(Collections.emptyMap(), Collections.singletonMap(field, stringTerms.get()));
        }

        final String fieldName = field.datasetFieldName(session.getDatasetName());
        final String[] intFields = isIntField ? new String[]{fieldName} : new String[0];
        final String[] stringFields = isIntField ? new String[0] : new String[]{fieldName};
        final FTGSParams params;
        if (topKParams.isPresent()) {
            params = new FTGSParams(intFields, stringFields, topKParams.get().limit, topKParams.get().sortStatIndex, isSorted);
        } else if(ftgsRowLimit.isPresent()) {
            // TODO: can term limited request be unsorted?
            // Check if calling side expects first terms in sorted order.
            params = new FTGSParams(intFields, stringFields, ftgsRowLimit.get(), -1, true);
        } else {
            params = new FTGSParams(intFields, stringFields, 0, -1, isSorted);
        }

        return session.getFTGSIterator(params);
    }

    private static class SessionStringIterationState {
        public final FTGSIterator iterator;
        private final IntList metricIndexes;
        @Nullable
        private final Integer presenceIndex;
        public final long[] statsBuff;
        public String nextTerm;
        public int nextGroup;

        private SessionStringIterationState(FTGSIterator iterator, IntList metricIndexes, @Nullable Integer presenceIndex, long[] statsBuff, String nextTerm, int nextGroup) {
            this.iterator = iterator;
            this.metricIndexes = metricIndexes;
            this.presenceIndex = presenceIndex;
            this.statsBuff = statsBuff;
            this.nextTerm = nextTerm;
            this.nextGroup = nextGroup;
        }

        static Optional<SessionStringIterationState> construct(Closer closer, ImhotepSessionHolder session, FieldSet field, IntList sessionMetricIndexes, @Nullable Integer presenceIndex,
                                                               Optional<RemoteTopKParams> topKParams, Optional<Integer> ftgsRowLimit, Optional<String[]> termSubset) {
            final FTGSIterator it = closer.register(getFTGSIterator(session, field, false, topKParams, ftgsRowLimit, Optional.<long[]>absent(), termSubset));
            final int numStats = session.getNumStats();
            final long[] statsBuff = new long[numStats];
            while (it.nextField()) {
                while (it.nextTerm()) {
                    while (it.nextGroup()) {
                        it.groupStats(statsBuff);
                        return Optional.of(new SessionStringIterationState(it, sessionMetricIndexes, presenceIndex, statsBuff, it.termStringVal(), it.group()));
                    }
                }
            }
            return Optional.absent();
        }
    }

    public interface StringIterateCallback {
        void term(String term, long[] stats, int group);
        // return true if terms are expected in sorted order
        boolean needSorted();
        // if needGroup() returns false then group value is ignored inside term(...) method
        // and it's valid to pass any value as group
        boolean needGroup();
        // if needStats() returns false then stats value is ignored inside term(...) method
        // and it's valid to pass any array or null as stats
        boolean needStats();
    }

    /**
     * {@code metricIndexes} must be disjoint across sessions.
     */
    public static void iterateMultiString(Map<String, ImhotepSessionHolder> sessions, Map<String, IntList> metricIndexes, Map<String, Integer> presenceIndexes, FieldSet field, StringIterateCallback callback, TracingTreeTimer timer,
                                          final Set<String> options) throws IOException {
        iterateMultiString(sessions, metricIndexes, presenceIndexes, field, Optional.<RemoteTopKParams>absent(), Optional.<Integer>absent(), Optional.<String[]>absent(), callback, timer, options);
    }

    /**
     * {@code metricIndexes} must be disjoint across sessions.
     */
    public static void iterateMultiString(
            Map<String, ImhotepSessionHolder> sessions, Map<String, IntList> metricIndexes, Map<String, Integer> presenceIndexes, FieldSet field,
            Optional<RemoteTopKParams> topKParams, Optional<Integer> limit, Optional<String[]> termSubset, StringIterateCallback callback, TracingTreeTimer timer,
            final Set<String> options) throws IOException {

        if (iterateSimpleString(sessions, metricIndexes, presenceIndexes, field, topKParams, limit, termSubset, callback, timer, options)) {
            return;
        }

        int numMetrics = 0;
        for (final IntList metrics : metricIndexes.values()) {
            numMetrics += metrics.size();
        }
        try (final Closer closer = Closer.create()) {
            final Comparator<SessionStringIterationState> comparator = new Comparator<SessionStringIterationState>() {
                public int compare(SessionStringIterationState x, SessionStringIterationState y) {
                    int r = x.nextTerm.compareTo(y.nextTerm);
                    if (r != 0) return r;
                    return Ints.compare(x.nextGroup, y.nextGroup);
                }
            };
            timer.push("request remote FTGS iterator");
            // TODO: Parallelize
            final PriorityQueue<SessionStringIterationState> pq = new PriorityQueue<>(sessions.size(), comparator);
            for (final String sessionName : sessions.keySet()) {
                timer.push("session", "session:" + sessionName + ", field:" + field.datasetFieldName(sessionName));
                final ImhotepSessionHolder session = sessions.get(sessionName);
                final IntList sessionMetricIndexes = Objects.firstNonNull(metricIndexes.get(sessionName), new IntArrayList());
                final Integer presenceIndex = presenceIndexes.get(sessionName);
                final Optional<SessionStringIterationState> constructed = SessionStringIterationState.construct(closer, session, field, sessionMetricIndexes, presenceIndex, topKParams, limit, termSubset);
                if (constructed.isPresent()) {
                    pq.add(constructed.get());
                }
                timer.pop();
            }
            timer.pop();

            timer.push("consume FTGS iterator");
            final long[] realBuffer = new long[numMetrics + presenceIndexes.size()];
            final List<SessionStringIterationState> toEnqueue = Lists.newArrayList();
            while (!pq.isEmpty()) {
                toEnqueue.clear();
                Arrays.fill(realBuffer, 0);
                final SessionStringIterationState state1 = pq.poll();
                final String term = state1.nextTerm;
                final int group = state1.nextGroup;
                copyStats(state1, realBuffer);
                toEnqueue.add(state1);
                while (!pq.isEmpty() && pq.peek().nextTerm.equals(term) && pq.peek().nextGroup == group) {
                    final SessionStringIterationState state = pq.poll();
                    copyStats(state, realBuffer);
                    toEnqueue.add(state);
                }
                callback.term(term, realBuffer, group);
                for (final SessionStringIterationState state : toEnqueue) {
                    advanceAndEnqueue(state, pq);
                }
            }
            timer.pop();
        }
    }

    public static boolean iterateSimpleString(
            final Map<String, ImhotepSessionHolder> sessions,
            final Map<String, IntList> metricIndexes,
            final Map<String, Integer> presenceIndexes,
            final FieldSet field,
            final Optional<RemoteTopKParams> topKParams,
            final Optional<Integer> ftgsRowLimit,
            final Optional<String[]> termSubset,
            final StringIterateCallback callback,
            final TracingTreeTimer timer,
            final Set<String> options)
    {
        if (!isSimple(sessions, metricIndexes, presenceIndexes, options)) {
            return false;
        }

        final ImhotepSessionHolder session = Iterables.getOnlyElement(sessions.values());
        final String dataset = session.getDatasetName();
        timer.push("request remote FTGS iterator for single session", "request remote FTGS iterator for single session: " + dataset + "." + field.datasetFieldName(dataset));

        try (final FTGSIterator ftgs =
                     createFTGSIterator(session, field, false,
                             topKParams, ftgsRowLimit,
                             Optional.absent(), termSubset, callback.needSorted())) {
            timer.pop();

            timer.push("consume FTGS iterator");

            if (!ftgs.nextField()) {
                throw new IllegalStateException("At least one field expected");
            }

            if (ftgs.fieldIsIntType()) {
                throw new IllegalStateException("Field type mismatch");
            }

            final boolean needStats = callback.needStats();
            final long[] stats = needStats ? new long[ftgs.getNumStats()] : null;
            while (ftgs.nextTerm()) {
                final String term = ftgs.termStringVal();
                while (ftgs.nextGroup()) {
                    final int group = ftgs.group();
                    if (needStats) {
                        ftgs.groupStats(stats);
                    }
                    callback.term(term, stats, group);
                }
            }

            if (ftgs.nextField()) {
                throw new IllegalStateException("Exactly one field expected");
            }

            timer.pop();
        }
        return true;
    }

    private static FTGSIterator getFTGSIterator(
            final ImhotepSessionHolder session, final FieldSet field, final boolean isIntField,
            final Optional<RemoteTopKParams> topKParams, final Optional<Integer> limit,
            Optional<long[]> intTermSubset, Optional<String[]> stringTermSubset
    ) {

        if (isIntField && intTermSubset.isPresent()) {
            return session.getSubsetFTGSIterator(Collections.singletonMap(field, intTermSubset.get()), Collections.emptyMap());
        } else if (!isIntField && stringTermSubset.isPresent()) {
            return session.getSubsetFTGSIterator(Collections.emptyMap(), Collections.singletonMap(field, stringTermSubset.get()));
        }

        final String fieldName = field.datasetFieldName(session.getDatasetName());

        final String[] intFields, strFields;
        if (isIntField) {
            intFields = new String[]{fieldName};
            strFields = new String[0];
        } else {
            strFields = new String[]{fieldName};
            intFields = new String[0];
        }
        final FTGSIterator it;
        if (topKParams.isPresent()) {
            it = session.getFTGSIterator(intFields, strFields, topKParams.get().limit, topKParams.get().sortStatIndex);
        } else if (limit.isPresent()) {
            it = session.getFTGSIterator(intFields, strFields, limit.get());
        } else {
            it = session.getFTGSIterator(intFields, strFields);
        }
        return it;
    }

    private static void advanceAndEnqueue(SessionStringIterationState state, PriorityQueue<SessionStringIterationState> pq) {
        final FTGSIterator iterator = state.iterator;
        if (iterator.nextGroup()) {
            state.nextGroup = iterator.group();
            iterator.groupStats(state.statsBuff);
            pq.add(state);
        } else {
            while (iterator.nextTerm()) {
                while (iterator.nextGroup()) {
                    state.nextTerm = iterator.termStringVal();
                    state.nextGroup = iterator.group();
                    iterator.groupStats(state.statsBuff);
                    pq.add(state);
                    return;
                }
            }
        }
    }

    /**
     * {@code state.metricIndexes} must be disjoint across sessions.
     */
    private static void copyStats(SessionStringIterationState state, long[] dst) {
        for (int i = 0; i < state.metricIndexes.size(); i++) {
            dst[state.metricIndexes.getInt(i)] = state.statsBuff[i];
        }
        if (state.presenceIndex != null) {
            dst[state.presenceIndex] = 1;
        }
    }

    public static class SavedGroupStats {
        public final int depth;
        public final double[] stats;

        public SavedGroupStats(int depth, double[] stats) {
            this.depth = depth;
            this.stats = stats;
        }
    }

    public static class RemoteTopKParams {
        public final int limit;
        public final int sortStatIndex;

        public RemoteTopKParams(final int limit, final int sortStatIndex) {
            this.limit = limit;
            this.sortStatIndex = sortStatIndex;
        }
    }

    public static class ImhotepSessionInfo {
        public final ImhotepSessionHolder session;
        public final String displayName;
        public final Collection<String> intFields;
        public final Collection<String> stringFields;
        public final DateTime startTime;
        public final DateTime endTime;
        public final String timeFieldName;

        @VisibleForTesting
        ImhotepSessionInfo(ImhotepSessionHolder session, String displayName, Collection<String> intFields, Collection<String> stringFields, DateTime startTime, DateTime endTime, String timeFieldName) {
            this.session = session;
            this.displayName = displayName;
            this.intFields = Collections.unmodifiableCollection(intFields);
            this.stringFields = Collections.unmodifiableCollection(stringFields);
            this.startTime = startTime;
            this.endTime = endTime;
            this.timeFieldName = timeFieldName;
        }
    }
}
