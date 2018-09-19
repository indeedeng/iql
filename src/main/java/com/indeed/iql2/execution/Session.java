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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql.marshal.ImhotepMarshallerInIQL;
import com.indeed.iql.marshal.ImhotepMarshallerInIQL.FieldOptions;
import com.indeed.iql.marshal.ImhotepMarshallerInIQL.SingleFieldMultiRemapRule;
import com.indeed.iql.metadata.DatasetMetadata;
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
import com.indeed.util.core.TreeTimer;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleCollection;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author jwolfe
 */
public class Session {

    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    private static final Logger log = Logger.getLogger(Session.class);

    public GroupKeySet groupKeySet = DumbGroupKeySet.create();
    public final Map<String, SavedGroupStats> savedGroupStats = Maps.newHashMap();
    public int currentDepth = 0;

    public final Map<String, ImhotepSessionInfo> sessions;
    public final TreeTimer timer;
    private final ProgressCallback progressCallback;
    public final int groupLimit;
    private final long firstStartTimeMillis;
    public final Set<String> options;

    public int numGroups = 1;

    public static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    }

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
            TreeTimer timer,
            ProgressCallback progressCallback,
            @Nullable Integer groupLimit,
            long firstStartTimeMillis,
            final Set<String> options
    ) {
        this.sessions = sessions;
        this.timer = timer;
        this.progressCallback = progressCallback;
        this.groupLimit = groupLimit == null ? -1 : groupLimit;
        this.firstStartTimeMillis = firstStartTimeMillis;
        this.options = options;
    }

    public static class CreateSessionResult {
        public final Optional<Session> session;
        public final long tempFileBytesWritten;
        @Nullable
        public final PerformanceStats imhotepPerformanceStats;

        CreateSessionResult(Optional<Session> session, long tempFileBytesWritten, PerformanceStats imhotepPerformanceStats) {
            this.session = session;
            this.tempFileBytesWritten = tempFileBytesWritten;
            this.imhotepPerformanceStats = imhotepPerformanceStats;
        }
    }

    public static CreateSessionResult createSession(
            final ImhotepClient client,
            final Map<String, List<Shard>> datasetToChosenShards,
            final Integer groupLimit,
            final Set<String> optionsSet,
            final List<com.indeed.iql2.language.commands.Command> commands,
            final List<Queries.QueryDataset> datasets,
            final Closer closer,
            final Consumer<String> out,
            final TreeTimer treeTimer,
            final ProgressCallback progressCallback,
            final Long imhotepLocalTempFileSizeLimit,
            final Long imhotepDaemonTempFileSizeLimit,
            final String username
    ) throws ImhotepOutOfMemoryException, IOException {
        final Map<String, ImhotepSessionInfo> sessions = Maps.newLinkedHashMap();

        final List<String> optionsList = new ArrayList<>(optionsSet);

        final boolean requestRust = optionsSet.contains(QueryOptions.USE_RUST_DAEMON);

        progressCallback.startSession(Optional.of(commands.size()));
        progressCallback.preSessionOpen(datasetToChosenShards);

        treeTimer.push("createSubSessions");
        final long firstStartTimeMillis = createSubSessions(client, requestRust, datasets, datasetToChosenShards,
                closer, sessions, treeTimer, imhotepLocalTempFileSizeLimit, imhotepDaemonTempFileSizeLimit, username, progressCallback);
        progressCallback.sessionsOpened(sessions);
        treeTimer.pop();

        final Session session = new Session(sessions, treeTimer, progressCallback, groupLimit, firstStartTimeMillis, optionsSet);
        for (int i = 0; i < commands.size(); i++) {
            final com.indeed.iql2.language.commands.Command command = commands.get(i);
            final boolean isLast = i == commands.size() - 1;
            if (isLast) {
                session.evaluateCommandToTSV(command, out, optionsList);
            } else {
                session.evaluateCommand(command, s -> {}, optionsList);
            }
            if (session.numGroups == 0) {
                break;
            }
        }

        long tempFileBytesWritten = 0L;
        for (final ImhotepSessionInfo sessionInfo : session.sessions.values()) {
            tempFileBytesWritten += sessionInfo.session.getTempFilesBytesWritten();
        }

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

        return new CreateSessionResult(Optional.<Session>absent(), tempFileBytesWritten, performanceStats.build());
    }

    private static long createSubSessions(
            final ImhotepClient client,
            final boolean requestRust,
            final List<Queries.QueryDataset> sessionRequest,
            final Map<String, List<Shard>> datasetToChosenShards,
            final Closer closer,
            final Map<String, ImhotepSessionInfo> sessions,
            final TreeTimer treeTimer,
            final Long imhotepLocalTempFileSizeLimit,
            final Long imhotepDaemonTempFileSizeLimit,
            final String username,
            final ProgressCallback progressCallback
    ) throws ImhotepOutOfMemoryException, IOException {
        final Map<String, String> upperCaseToActualDataset = new HashMap<>();
        for (final String dataset : client.getDatasetNames()) {
            upperCaseToActualDataset.put(dataset.toUpperCase(), dataset);
        }

        long firstStartTimeMillis = 0;
        for (int i = 0; i < sessionRequest.size(); i++) {
            final Queries.QueryDataset dataset = sessionRequest.get(i);
            final String actualDataset = upperCaseToActualDataset.get(dataset.dataset.toUpperCase());
            Preconditions.checkNotNull(actualDataset, "Dataset does not exist: %s", dataset.name);
            final Map<String, String> uppercasedFieldAliases = upperCaseMap(dataset.fieldAliases);
            final Map<String, String> uppercasedDimensionAliases = upperCaseMap(dataset.dimensionAliases);
            final Map<String, String> uppercasedCombinedAliases = combineAliases(uppercasedFieldAliases, uppercasedDimensionAliases);
            treeTimer.push("session:" + dataset.displayName);

            treeTimer.push("get dataset info");
            treeTimer.push("getDatasetShardInfo");
            final DatasetInfo datasetInfo = client.getDatasetInfo(actualDataset);
            treeTimer.pop();
            final Set<String> sessionIntFields = Sets.newHashSet(datasetInfo.getIntFields());
            final Set<String> sessionStringFields = new HashSet<>();

            for (String stringField : datasetInfo.getStringFields()) {
                if (uppercasedDimensionAliases.containsKey(stringField.toUpperCase())) {
                    sessionIntFields.add(stringField);
                } else {
                    sessionStringFields.add(stringField);
                }
            }

            final Set<String> upperCasedIntFields = upperCase(sessionIntFields);
            final Set<String> upperCasedStringFields = upperCase(sessionStringFields);

            for (final Map.Entry<String, String> entry : uppercasedCombinedAliases.entrySet()) {
                final String uppercasedField = entry.getValue();
                if (upperCasedIntFields.contains(uppercasedField) || uppercasedDimensionAliases.containsKey(uppercasedField)) {
                    sessionIntFields.add(entry.getKey());
                    upperCasedIntFields.add(entry.getKey().toUpperCase());
                } else if (upperCasedStringFields.contains(uppercasedField)) {
                    sessionStringFields.add(entry.getKey());
                    upperCasedStringFields.add(entry.getKey().toUpperCase());
                } else {
                    throw new IllegalStateException("Field [" + uppercasedField + "] not found in index [" + dataset.dataset + "]");
                }
            }

            final DateTime startDateTime = parseDateTime(dataset.start);
            final DateTime endDateTime = parseDateTime(dataset.end);
            treeTimer.pop();
            treeTimer.push("build session");
            treeTimer.push("create session builder");
            final List<Shard> chosenShards = datasetToChosenShards.get(dataset.name);
            if ((chosenShards == null) || chosenShards.isEmpty()) {
                throw new IqlKnownException.NoDataException("No shards: no data available for the requested dataset and time range."
                + " Dataset: " + dataset.name + ", start: " + startDateTime + ", end: " + endDateTime);
            }
            final ImhotepClient.SessionBuilder sessionBuilder = client
                .sessionBuilder(actualDataset, startDateTime, endDateTime)
                .username("IQL2:" + username)
                .shardsOverride(chosenShards)
                .localTempFileSizeLimit(imhotepLocalTempFileSizeLimit)
                .daemonTempFileSizeLimit(imhotepDaemonTempFileSizeLimit)
                .allowSessionForwarding(requestRust);
            treeTimer.pop();
            // TODO: message should be "build session builder (xxx shards on yyy daemons)"
            // but we can't get information about daemons count now
            // need to add method to RemoteImhotepMultiSession or to session builder.
            treeTimer.push("build session builder (" + chosenShards.size() + " shards)");
            final ImhotepSession build = closer.register(sessionBuilder.build());
            treeTimer.pop();
            // Just in case they have resources, register the wrapped session as well.
            // Double close() is supposed to be safe.
            final ImhotepSessionHolder session = closer.register(wrapSession(uppercasedCombinedAliases, build, Sets.union(sessionIntFields, sessionStringFields)));
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
            sessions.put(dataset.name, new ImhotepSessionInfo(session, dataset.displayName, upperCasedIntFields, upperCasedStringFields, startDateTime, endDateTime, timeField.toUpperCase()));
            if (i == 0) {
                firstStartTimeMillis = startDateTime.getMillis();
            }

            treeTimer.pop();
        }
        return firstStartTimeMillis;
    }

    private static Map<String, String> combineAliases(final Map<String, String> fieldAliases, final Map<String, String> dimensionAliases) {
        final Map<String, String> combinedAliases = new HashMap<>();
        combinedAliases.putAll(dimensionAliases);
        combinedAliases.putAll(fieldAliases);
        final Map<String, String> resolvedAliasesFields = resolveAliasToRealField(combinedAliases);
        // alias target to the same field will cause the CaseInsensitiveSession fail
        // uppercased field may overwrite the origin field
        return resolvedAliasesFields.entrySet().stream().filter(
                e -> !e.getKey().equalsIgnoreCase(e.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }


    private static Map<String, String> resolveAliasToRealField(Map<String, String> fieldAliases) {
        ImmutableMap.Builder<String, String> resolvedAliasToRealFieldBuilder = new ImmutableMap.Builder<>();
        for (String originField : fieldAliases.keySet()) {
            String targetField = fieldAliases.get(originField);
            final Set<String> seenField = new LinkedHashSet<>();
            seenField.add(targetField);
            while (fieldAliases.containsKey(targetField)) {
                final String newTargetField = fieldAliases.get(targetField);
                // for the dimension: same -> same
                if (newTargetField.equals(targetField)) {
                    break;
                }
                if (seenField.contains(newTargetField)) {
                    throw new IllegalArgumentException(
                            String.format("field alias has circular reference: %s -> %s", originField,
                                    Joiner.on(" -> ").join(seenField.toArray())));
                }
                seenField.add(newTargetField);
                targetField = newTargetField;
            }
            resolvedAliasToRealFieldBuilder.put(originField, targetField);
        }
        return resolvedAliasToRealFieldBuilder.build();
    }


    private static Set<String> upperCase(Collection<String> collection) {
        final Set<String> result = new HashSet<>(collection.size());
        for (final String value : collection) {
            result.add(value.toUpperCase());
        }
        return result;
    }

    private static Map<String, String> upperCaseMap(final Map<String, String> map) {
        return map.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toUpperCase(), Map.Entry::getValue));
    }

    private static ImhotepSessionHolder wrapSession(Map<String, String> fieldAliases, ImhotepSession build, Set<String> fieldNames) {
        return new ImhotepSessionHolder(build, fieldAliases, fieldNames);
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
                                final Consumer<String> out,
                                final List<String> options) throws ImhotepOutOfMemoryException, IOException {
        final String commandTreeString = lCommand.toString();
        timer.push("evaluateCommand " + (commandTreeString.length() > 500 ? (commandTreeString.substring(0, 500) + "[...](log truncated)") : commandTreeString));
        try {
            final Command command = lCommand.toExecutionCommand(this::namedMetricLookup, groupKeySet, options);
            progressCallback.startCommand(this, command, false);
            try {
                command.execute(this, out);
            } finally {
                progressCallback.endCommand(this, command);
            }
        } finally {
            timer.pop();
        }
    }

    public static void appendGroupString(String groupString, StringBuilder sb) {
        for (int i = 0; i < groupString.length(); i++) {
            final char groupChar = groupString.charAt(i);
            if (groupChar != '\t' && groupChar != '\r' && groupChar != '\n') {
                sb.append(groupChar);
            } else {
                sb.append('\ufffd');
            }
        }
    }

    public void evaluateCommandToTSV(final com.indeed.iql2.language.commands.Command lCommand,
                                     final Consumer<String> out,
                                     final List<String> options) throws ImhotepOutOfMemoryException, IOException {
        timer.push("evaluateCommandToTSV " + lCommand);
        try {
            final Command command = lCommand.toExecutionCommand(this::namedMetricLookup, groupKeySet, options);
            try {
                progressCallback.startCommand(this, command, true);
                if (command instanceof SimpleIterate) {
                    final SimpleIterate simpleIterate = (SimpleIterate) command;
                    final List<List<List<TermSelects>>> result = simpleIterate.evaluate(this, out);
                    //noinspection StatementWithEmptyBody
                    if (simpleIterate.streamResult) {
                        // result already sent
                    } else {
                        final String[] formatStrings = new String[simpleIterate.selecting.size()];
                        for (int i = 0; i < formatStrings.length; i++) {
                            final Optional<String> opt = simpleIterate.formatStrings.get(i);
                            formatStrings[i] = opt.isPresent() ? opt.get() : null;
                        }

                        for (final List<List<TermSelects>> groupFieldTerms : result) {
                            final List<TermSelects> groupTerms = groupFieldTerms.get(0);
                            for (final TermSelects termSelect : groupTerms) {
                                if (!groupKeySet.isPresent(termSelect.group)) {
                                    continue;
                                }
                                // TODO: propagate PRINTF info
                                if (termSelect.isIntTerm) {
                                    out.accept(SimpleIterate.createRow(groupKeySet, termSelect.group, termSelect.intTerm, termSelect.selects, formatStrings));
                                } else {
                                    out.accept(SimpleIterate.createRow(groupKeySet, termSelect.group, termSelect.stringTerm, termSelect.selects, formatStrings));
                                }
                            }
                        }
                    }
                } else if (command instanceof GetGroupStats) {
                    final GetGroupStats getGroupStats = (GetGroupStats) command;
                    final List<GroupStats> results = getGroupStats.evaluate(this);
                    final StringBuilder sb = new StringBuilder();

                    final String[] formatStrings = new String[getGroupStats.metrics.size()];
                    for (int i = 0; i < formatStrings.length; i++) {
                        final Optional<String> opt = getGroupStats.formatStrings.get(i);
                        formatStrings[i] = opt.isPresent() ? opt.get() : null;
                    }

                    for (final GroupStats result : results) {
                        if (!groupKeySet.isPresent(result.group)) {
                            continue;
                        }
                        final List<String> keyColumns = GroupKeySets.asList(groupKeySet, result.group);
                        if (keyColumns.isEmpty()) {
                            sb.append("\t");
                        } else {
                            for (final String k : keyColumns) {
                                appendGroupString(k, sb);
                                sb.append('\t');
                            }
                        }
                        final double[] stats = result.stats;
                        writeDoubleStatsWithFormatString(stats, formatStrings, sb);
                        if (keyColumns.size() + result.stats.length > 0) {
                            sb.setLength(sb.length() - 1);
                        }
                        out.accept(sb.toString());
                        sb.setLength(0);
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

    public static void writeDoubleStatsWithFormatString(final double[] stats, final String[] formatStrings, final StringBuilder sb) {
        for (int i = 0; i < stats.length; i++) {
            final double stat = stats[i];
            if (i < formatStrings.length && formatStrings[i] != null) {
                sb.append(String.format(formatStrings[i], stat)).append('\t');
            } else if (DoubleMath.isMathematicalInteger(stat)) {
                sb.append(String.format("%.0f", stat)).append('\t');
            } else {
                sb.append(Double.isNaN(stat) ? "NaN" : DEFAULT_DECIMAL_FORMAT.get().format(stat)).append('\t');
            }
        }
    }

    public static void writeTermSelectsJson(GroupKeySet groupKeySet, List<List<List<TermSelects>>> results, StringBuilder sb) {
        for (final List<List<TermSelects>> groupFieldTerms : results) {
            final List<TermSelects> groupTerms = groupFieldTerms.get(0);
            for (final TermSelects termSelects : groupTerms) {
                if (!groupKeySet.isPresent(termSelects.group)) {
                    continue;
                }
                final List<String> keyColumns = GroupKeySets.asList(groupKeySet, termSelects.group);
                for (final String k : keyColumns) {
                    appendGroupString(k, sb);
                    sb.append('\t');
                }
                if (termSelects.isIntTerm) {
                    sb.append(termSelects.intTerm).append('\t');
                } else {
                    sb.append(termSelects.stringTerm).append('\t');
                }
                for (final double stat : termSelects.selects) {
                    if (DoubleMath.isMathematicalInteger(stat)) {
                        sb.append((long) stat).append('\t');
                    } else {
                        sb.append(stat).append('\t');
                    }
                }
                sb.setLength(sb.length() - 1);
                sb.append('\n');
            }
        }
        sb.setLength(sb.length() - 1);
    }

    public int findPercentile(double v, double[] percentiles) {
        for (int i = 0; i < percentiles.length - 1; i++) {
            if (v <= percentiles[i + 1]) {
                return i;
            }
        }
        return percentiles.length - 1;
    }

    // Returns the start of the bucket.
    // result[0] will always be 0
    // result[1] will be the minimum value required to be greater than 1/k values.
    public static double[] getPercentiles(DoubleCollection values, int k) {
        final DoubleArrayList list = new DoubleArrayList(values);
        // TODO: Will this be super slow?
        Collections.sort(list);
        final double[] result = new double[k];
        for (int i = 0; i < k; i++) {
            result[i] = list.get((int) Math.ceil((double) list.size() * i / k));
        }
        return result;
    }

    // TODO: Any call sites of this could be optimized.
    public static double[] prependZero(double[] in) {
        final double[] out = new double[in.length + 1];
        System.arraycopy(in, 0, out, 1, in.length);
        return out;
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

    public static int pushStatsWithTimer(final ImhotepSessionHolder session, final List<String> pushes, final TreeTimer timer) throws ImhotepOutOfMemoryException {
        timer.push("pushStats ('" + String.join("', '", pushes) + "')");
        final int result = session.pushStats(pushes);
        timer.pop();
        return result;
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

    public int performTimeRegroup(long start, long end, long unitSize, final Optional<String> fieldOverride, boolean isRelative) throws ImhotepOutOfMemoryException {
        timer.push("performTimeRegroup");
        final int oldNumGroups = this.numGroups;
        // TODO: Parallelize
        for (final ImhotepSessionInfo sessionInfo : sessions.values()) {
            timer.push("session:" + sessionInfo.displayName);

            final ImhotepSessionHolder session = sessionInfo.session;
            final String fieldName;
            if (fieldOverride.isPresent()) {
                fieldName = fieldOverride.get();
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

            timer.push("popStat");
            session.popStat();
            timer.pop();

            timer.pop();
        }
        final int result = (int) (oldNumGroups * Math.ceil(((double) end - start) / unitSize));
        timer.pop();
        return result;
    }

    public void densify(GroupKeySet groupKeySet) throws ImhotepOutOfMemoryException {
        timer.push("densify");
        final BitSet anyPresent = new BitSet();
        // TODO: Parallelize?
        for (Map.Entry<String, ImhotepSessionHolder> imhotepSessionEntry : getSessionsMapRaw().entrySet()) {
            timer.push("session:" + getSessionsMap().get(imhotepSessionEntry.getKey()).displayName);

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

    public boolean isIntField(String field) {
        for (final ImhotepSessionInfo x : sessions.values()) {
            if (x.intFields.contains(field)) {
                return true;
            }
        }
        return false;
    }

    public boolean isStringField(String field) {
        if (isIntField(field)) {
            return false;
        }
        for (final ImhotepSessionInfo x : sessions.values()) {
            if (x.stringFields.contains(field)) {
                return true;
            }
        }
        return false;
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

    public static void unchecked(RunnableWithException runnable) {
        try {
            runnable.run();
        } catch (final Throwable t) {
            log.error("unchecked error", t);
            throw Throwables.propagate(t);
        }
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
            timer.push("session:" + sessionInfo.displayName);
            final ImhotepSessionHolder session = sessionInfo.session;
            final int numStats = session.getNumStats();
            for (int i = 0; i < numStats; i++) {
                session.popStat();
            }
            timer.pop();
        }
        timer.pop();
    }

    public void regroupWithProtos(GroupMultiRemapMessage[] messages, boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        timer.push("regroupWithProtos(" + messages.length + " rules)");
        for (final ImhotepSessionInfo sessionInfo : sessions.values()) {
            timer.push("session:" + sessionInfo.displayName);
            sessionInfo.session.regroupWithProtos(messages, errorOnCollisions);
            timer.pop();
        }
        timer.pop();
    }

    public void regroupWithSingleFieldRules(
            final SingleFieldMultiRemapRule[] rules,
            final FieldOptions options,
            final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        timer.push("regroupWithSingleFieldRules(" + rules.length + " rules)");

        // Gather sessions with same real fields name together
        // to convert messages only once for each unique real field name.
        final Map<String, List<ImhotepSessionInfo>> realFieldToSessions = new HashMap<>();
        for (final ImhotepSessionInfo sessionInfo : sessions.values()) {
            final String realField = sessionInfo.session.convertField(options.field);
            if (!realFieldToSessions.containsKey(realField)) {
                realFieldToSessions.put(realField, new ArrayList<>());
            }
            realFieldToSessions.get(realField).add(sessionInfo);
        }

        for (final Map.Entry<String, List<ImhotepSessionInfo>> entry : realFieldToSessions.entrySet()) {
            final FieldOptions realOptions = new FieldOptions(entry.getKey(), options.intType, options.inequality);
            final GroupMultiRemapMessage[] convertedMessages = ImhotepMarshallerInIQL.marshal(rules, realOptions);
            for (final ImhotepSessionInfo sessionInfo : entry.getValue()) {
                timer.push("session:" + sessionInfo.displayName);
                sessionInfo.session.regroupWithPreparedProtos(convertedMessages, errorOnCollisions);
                timer.pop();
            }
        }
        timer.pop();
    }

    public void remapGroups(final int[] fromGroups, final int[] toGroups) throws ImhotepOutOfMemoryException {

        if (fromGroups.length != toGroups.length) {
            throw new IllegalStateException();
        }

        final GroupMultiRemapMessage[] messages = new GroupMultiRemapMessage[fromGroups.length];
        for (int i = 0; i < fromGroups.length; i++) {
            messages[i] = GroupMultiRemapMessage.newBuilder()
                    .setTargetGroup(fromGroups[i])
                    .setNegativeGroup(toGroups[i])
                    .build();
        }
        // TODO: Parallelize
        timer.push("remapGroups");
        for (final ImhotepSessionInfo sessionInfo : sessions.values()) {
            timer.push("session:" + sessionInfo.displayName);
            sessionInfo.session.remapGroups(messages);
            timer.pop();
        }
        timer.pop();
    }

    public void popStat() {
        // TODO: Parallelize
        timer.push("popStat");
        for (final ImhotepSessionInfo sessionInfo : sessions.values()) {
            timer.push("session:" + sessionInfo.displayName);
            sessionInfo.session.popStat();
            timer.pop();
        }
        timer.pop();
    }

    public void intOrRegroup(String field, long[] terms, int targetGroup, int negativeGroup, int positiveGroup, Set<String> scope) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        timer.push("intOrRegroup(" + terms.length + " terms)");
        for (final String s : scope) {
            if (sessions.containsKey(s)) {
                final ImhotepSessionInfo sessionInfo = sessions.get(s);
                timer.push("session:" + sessionInfo.displayName);
                sessionInfo.session.intOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
                timer.pop();
            }
        }
        timer.pop();
    }

    public void stringOrRegroup(String field, String[] terms, int targetGroup, int negativeGroup, int positiveGroup, Set<String> scope) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        timer.push("stringOrRegroup(" + terms.length + " terms)");
        for (final String s : scope) {
            if (sessions.containsKey(s)) {
                final ImhotepSessionInfo sessionInfo = sessions.get(s);
                timer.push("session:" + sessionInfo.displayName);
                sessionInfo.session.stringOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
                timer.pop();
            }
        }
        timer.pop();
    }

    public void remapGroup(final int fromGroup, final int toGroup, final Set<String> scope) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        timer.push("regroup");
        final QueryRemapRule rule = new QueryRemapRule(fromGroup, Query.newTermQuery(new Term("fakeField", true, 0L, "")), toGroup, toGroup);
        for (final String s : scope) {
            if (sessions.containsKey(s)) {
                timer.push("session:" + sessions.get(s).displayName);
                sessions.get(s).session.regroup(rule);
                timer.pop();
            }
        }
        timer.pop();
    }

    public void regexRegroup(String field, String regex, int targetGroup, int negativeGroup, int positiveGroup, ImmutableSet<String> scope) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        timer.push("regexRegroup");
        for (final Map.Entry<String, Session.ImhotepSessionInfo> entry : sessions.entrySet()) {
            if (scope.contains(entry.getKey())) {
                final Session.ImhotepSessionInfo v = entry.getValue();
                timer.push("session:" + entry.getValue().displayName);
                v.session.regexRegroup(field, regex, targetGroup, negativeGroup, positiveGroup);
                timer.pop();
            }
        }
        timer.pop();
    }

    public void randomRegroup(String field, boolean isIntField, String seed, double probability, int targetGroup, int positiveGroup, int negativeGroup, ImmutableSet<String> scope) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        timer.push("randomRegroup");
        for (final Map.Entry<String, ImhotepSessionInfo> entry : sessions.entrySet()) {
            if (scope.contains(entry.getKey())) {
                timer.push("session:" + entry.getValue().displayName);
                entry.getValue().session.randomRegroup(field, isIntField, seed, probability, targetGroup, negativeGroup, positiveGroup);
                timer.pop();
            }
        }
        timer.pop();
    }

    public long[] getSimpleDistinct(final String field, final String scope) {
        final long[] result = new long[numGroups];
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
        timer.push("getSimpleDistinct session:" + info.displayName);
        try (final GroupStatsIterator iterator = session.getDistinct(field, isIntField)) {
            timer.pop();
            // skipping group zero
            if (!iterator.hasNext()) {
                return result;
            }
            iterator.nextLong();
            // extracting result for other groups
            final int size = Math.min(iterator.getNumGroups() - 1, result.length);
            for (int i = 0; i < size; i++) {
                result[i] += iterator.nextLong();
            }
            // exhaust iterator in case there is trailing data
            while (iterator.hasNext()) {
                iterator.nextLong();
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
                Closer closer, ImhotepSessionHolder session, String field, IntList sessionMetricIndexes, @Nullable Integer presenceIndex,
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
    public static void iterateMultiInt(Map<String, ImhotepSessionHolder> sessions, Map<String, IntList> metricIndexes, Map<String, Integer> presenceIndexes, String field, IntIterateCallback callback, TreeTimer timer,
                                       final Set<String> options) throws IOException {
        iterateMultiInt(sessions, metricIndexes, presenceIndexes, field, Optional.<RemoteTopKParams>absent(), Optional.<Integer>absent(), Optional.<long[]>absent(), callback, timer, options);
    }

    /**
     * {@code metricIndexes} must be disjoint across sessions.
     */
    public static void iterateMultiInt(
            Map<String, ImhotepSessionHolder> sessions, Map<String, IntList> metricIndexes, Map<String, Integer> presenceIndexes,
            String field, Optional<RemoteTopKParams> topKParams, Optional<Integer> ftgsRowLimit,
            Optional<long[]> termSubset, IntIterateCallback callback, TreeTimer timer,
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
                timer.push("session:"+sessionName);
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
            final String field,
            final Optional<RemoteTopKParams> topKParams,
            final Optional<Integer> ftgsRowLimit,
            final Optional<long[]> termSubset,
            final IntIterateCallback callback,
            final TreeTimer timer,
            final Set<String> options)
    {
        if (!isSimple(sessions, metricIndexes, presenceIndexes, options)) {
            return false;
        }

        final ImhotepSessionHolder session = Iterables.getOnlyElement(sessions.values());
        final String sessionName = Iterables.getOnlyElement(sessions.keySet());
        timer.push("request remote FTGS iterator for single session:"+sessionName);

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
            final String field,
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

        final String[] intFields = isIntField ? new String[]{field} : new String[0];
        final String[] stringFields = isIntField ? new String[0] : new String[]{field};
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

        static Optional<SessionStringIterationState> construct(Closer closer, ImhotepSessionHolder session, String field, IntList sessionMetricIndexes, @Nullable Integer presenceIndex,
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
    public static void iterateMultiString(Map<String, ImhotepSessionHolder> sessions, Map<String, IntList> metricIndexes, Map<String, Integer> presenceIndexes, String field, StringIterateCallback callback, TreeTimer timer,
                                          final Set<String> options) throws IOException {
        iterateMultiString(sessions, metricIndexes, presenceIndexes, field, Optional.<RemoteTopKParams>absent(), Optional.<Integer>absent(), Optional.<String[]>absent(), callback, timer, options);
    }

    /**
     * {@code metricIndexes} must be disjoint across sessions.
     */
    public static void iterateMultiString(
            Map<String, ImhotepSessionHolder> sessions, Map<String, IntList> metricIndexes, Map<String, Integer> presenceIndexes, String field,
            Optional<RemoteTopKParams> topKParams, Optional<Integer> limit, Optional<String[]> termSubset, StringIterateCallback callback, TreeTimer timer,
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
                timer.push("session:" + sessionName);
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
            final String field,
            final Optional<RemoteTopKParams> topKParams,
            final Optional<Integer> ftgsRowLimit,
            final Optional<String[]> termSubset,
            final StringIterateCallback callback,
            final TreeTimer timer,
            final Set<String> options)
    {
        if (!isSimple(sessions, metricIndexes, presenceIndexes, options)) {
            return false;
        }

        final ImhotepSessionHolder session = Iterables.getOnlyElement(sessions.values());
        final String sessionName = Iterables.getOnlyElement(sessions.keySet());
        timer.push("request remote FTGS iterator for single session:"+sessionName);

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
            final ImhotepSessionHolder session, final String field, final boolean isIntField,
            final Optional<RemoteTopKParams> topKParams, final Optional<Integer> limit,
            Optional<long[]> intTermSubset, Optional<String[]> stringTermSubset
    ) {

        if (isIntField && intTermSubset.isPresent()) {
            return session.getSubsetFTGSIterator(Collections.singletonMap(field, intTermSubset.get()), Collections.<String, String[]>emptyMap());
        } else if (!isIntField && stringTermSubset.isPresent()) {
            return session.getSubsetFTGSIterator(Collections.<String, long[]>emptyMap(), Collections.singletonMap(field, stringTermSubset.get()));
        }

        final String[] intFields, strFields;
        if (isIntField) {
            intFields = new String[]{field};
            strFields = new String[0];
        } else {
            strFields = new String[]{field};
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

    public static class GroupStats {
        public final int group;
        public final double[] stats;

        @JsonCreator
        public GroupStats(@JsonProperty("group") int group, @JsonProperty("stats") double[] stats) {
            this.group = group;
            this.stats = stats;
        }
    }

    public interface RunnableWithException {
        void run() throws Throwable;
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
