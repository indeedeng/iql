package com.indeed.squall.iql2.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
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
import com.indeed.common.util.time.WallClock;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RemoteImhotepMultiSession;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.ShardIdWithVersion;
import com.indeed.squall.iql2.execution.aliasing.FieldAliasingImhotepSession;
import com.indeed.squall.iql2.execution.caseinsensitivity.CaseInsensitiveImhotepSession;
import com.indeed.squall.iql2.execution.commands.Command;
import com.indeed.squall.iql2.execution.commands.GetGroupStats;
import com.indeed.squall.iql2.execution.commands.SimpleIterate;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.dimensions.DatasetDimensions;
import com.indeed.squall.iql2.execution.dimensions.DimensionDetails;
import com.indeed.squall.iql2.execution.dimensions.DimensionsTranslator;
import com.indeed.squall.iql2.execution.groupkeys.GroupKeySets;
import com.indeed.squall.iql2.execution.groupkeys.sets.DumbGroupKeySet;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.groupkeys.sets.MaskingGroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;
import com.indeed.squall.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.squall.iql2.execution.progress.ProgressCallback;
import com.indeed.squall.iql2.execution.workarounds.GroupMultiRemapRuleRewriter;
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
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public int numGroups = 1;

    public static final ObjectMapper MAPPER = new ObjectMapper();
    private static long firstStartTimeMill;

    static {
        MAPPER.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    }

    public static final String INFINITY_SYMBOL = "∞";

    public Session(Map<String, ImhotepSessionInfo> sessions, TreeTimer timer, ProgressCallback progressCallback, @Nullable Integer groupLimit) {
        this.sessions = sessions;
        this.timer = timer;
        this.progressCallback = progressCallback;
        this.groupLimit = groupLimit == null ? -1 : groupLimit;
    }

    public static class CreateSessionResult {
        public final Optional<Session> session;
        public final long tempFileBytesWritten;

        CreateSessionResult(Optional<Session> session, long tempFileBytesWritten) {
            this.session = session;
            this.tempFileBytesWritten = tempFileBytesWritten;
        }
    }

    public static CreateSessionResult createSession(
            final ImhotepClient client,
            final Map<String, List<ShardIdWithVersion>> datasetToChosenShards,
            final JsonNode sessionRequest,
            final Closer closer,
            final Consumer<String> out,
            final Map<String, DatasetDimensions> dimensions,
            final TreeTimer treeTimer,
            final ProgressCallback progressCallback,
            final Long imhotepLocalTempFileSizeLimit,
            final Long imhotepDaemonTempFileSizeLimit,
            final WallClock clock,
            final String username
    ) throws ImhotepOutOfMemoryException, IOException {
        final Map<String, ImhotepSessionInfo> sessions = Maps.newLinkedHashMap();

        final Integer groupLimit;
        if (sessionRequest.has("groupLimit")) {
            groupLimit = sessionRequest.get("groupLimit").intValue();
        } else {
            groupLimit = null;
        }

        if (sessionRequest.has("commands")) {
            treeTimer.push("readCommands");
            final JsonNode commands = sessionRequest.get("commands");
            progressCallback.startSession(Optional.of(commands.size()));
            treeTimer.pop();

            treeTimer.push("createSubSessions");
            createSubSessions(client, sessionRequest.get("datasets"), datasetToChosenShards, closer, sessions, dimensions, treeTimer, imhotepLocalTempFileSizeLimit, imhotepDaemonTempFileSizeLimit, clock, username);
            progressCallback.sessionsOpened(sessions);
            treeTimer.pop();

            final Session session = new Session(sessions, treeTimer, progressCallback, groupLimit);
            for (int i = 0; i < commands.size(); i++) {
                final JsonNode command = commands.get(i);
                final boolean isLast = i == commands.size() - 1;
                if (isLast) {
                    session.evaluateCommandToTSV(command, out);
                } else {
                    session.evaluateCommand(command, new Consumer<String>() {
                        public void accept(String s) {
                        }
                    });
                }
                if (session.numGroups == 0) {
                    break;
                }
            }

            long tempFileBytesWritten = 0L;
            for (final ImhotepSessionInfo sessionInfo : session.sessions.values()) {
                ImhotepSession s = sessionInfo.session;

                while (s instanceof WrappingImhotepSession) {
                    s = ((WrappingImhotepSession) s).wrapped();
                }

                if (s instanceof RemoteImhotepMultiSession) {
                    final RemoteImhotepMultiSession remoteImhotepMultiSession = (RemoteImhotepMultiSession) s;
                    tempFileBytesWritten += remoteImhotepMultiSession.getTempFilesBytesWritten();
                }
            }

            return new CreateSessionResult(Optional.<Session>absent(), tempFileBytesWritten);
        } else {
            progressCallback.startSession(Optional.<Integer>absent());
            createSubSessions(client, sessionRequest, datasetToChosenShards, closer, sessions, dimensions, treeTimer, imhotepLocalTempFileSizeLimit, imhotepDaemonTempFileSizeLimit, clock, username);
            progressCallback.sessionsOpened(sessions);
            out.accept("opened");
            return new CreateSessionResult(Optional.of(new Session(sessions, treeTimer, progressCallback, groupLimit)), 0L);
        }
    }

    public static Set<String> getDatasets(ImhotepClient client) {
        final Set<String> result = new HashSet<>();
        for (final Map.Entry<Host, List<DatasetInfo>> entry : client.getShardList().entrySet()) {
            for (final DatasetInfo datasetInfo : entry.getValue()) {
                result.add(datasetInfo.getDataset());
            }
        }
        return result;
    }

    private static void createSubSessions(
            final ImhotepClient client,
            final JsonNode sessionRequest,
            final Map<String, List<ShardIdWithVersion>> datasetToChosenShards,
            final Closer closer,
            final Map<String, ImhotepSessionInfo> sessions,
            final Map<String, DatasetDimensions> dimensions,
            final TreeTimer treeTimer,
            final Long imhotepLocalTempFileSizeLimit,
            final Long imhotepDaemonTempFileSizeLimit,
            final WallClock clock,
            final String username
    ) throws ImhotepOutOfMemoryException, IOException {
        final Map<String, String> upperCaseToActualDataset = new HashMap<>();
        for (final String dataset : Session.getDatasets(client)) {
            upperCaseToActualDataset.put(dataset.toUpperCase(), dataset);
        }

        for (int i = 0; i < sessionRequest.size(); i++) {
            final JsonNode elem = sessionRequest.get(i);
            final String datasetName = elem.get("dataset").textValue();
            final String actualDataset = upperCaseToActualDataset.get(datasetName.toUpperCase());
            Preconditions.checkNotNull(actualDataset, "Dataset does not exist: %s", datasetName);
            final String start = elem.get("start").textValue();
            final String end = elem.get("end").textValue();
            final String name = elem.has("name") ? elem.get("name").textValue() : datasetName;
            final String displayName = elem.get("displayName").textValue();
            final Map<String, String> fieldAliases = MAPPER.readValue(elem.get("fieldAliases").textValue(), new TypeReference<Map<String, String>>() {
            });

            treeTimer.push("get dataset info");
            treeTimer.push("getDatasetShardInfo");
            final DatasetInfo datasetInfo = client.getDatasetShardInfo(actualDataset);
            treeTimer.pop();
            final Set<String> sessionIntFields = Sets.newHashSet(datasetInfo.getIntFields());
            final Set<String> sessionStringFields = Sets.newHashSet(datasetInfo.getStringFields());

            // Don't uppercase for usage in the dimension translator.
            final DatasetDimensions datasetDimensions = dimensions.containsKey(actualDataset) ? dimensions.get(actualDataset) : new DatasetDimensions(ImmutableMap.<String, DimensionDetails>of());

            sessionIntFields.addAll(datasetDimensions.fields());

            final Set<String> upperCasedIntFields = upperCase(sessionIntFields);
            final Set<String> upperCasedStringFields = upperCase(sessionStringFields);

            for (final Map.Entry<String, String> entry : fieldAliases.entrySet()) {
                if (upperCasedIntFields.contains(entry.getValue())) {
                    sessionIntFields.add(entry.getKey());
                    upperCasedIntFields.add(entry.getKey().toUpperCase());
                } else if (upperCasedStringFields.contains(entry.getValue())) {
                    sessionStringFields.add(entry.getKey());
                    upperCasedStringFields.add(entry.getKey().toUpperCase());
                } else {
                    throw new IllegalStateException("Field [" + entry.getValue() + "] not found in index [" + datasetName + "]");
                }
            }

            final DateTime startDateTime = parseDateTime(start, clock);
            final DateTime endDateTime = parseDateTime(end, clock);
            treeTimer.pop();
            treeTimer.push("build session");
            treeTimer.push("create session builder");
            final List<ShardIdWithVersion> chosenShards = datasetToChosenShards.get(name);
            final ImhotepClient.SessionBuilder sessionBuilder =
                    client.sessionBuilder(actualDataset, startDateTime, endDateTime)
                          .username("IQL2:" + username)
                          .shardsOverride(ShardIdWithVersion.keepShardIds(chosenShards))
                          .localTempFileSizeLimit(imhotepLocalTempFileSizeLimit)
                          .daemonTempFileSizeLimit(imhotepDaemonTempFileSizeLimit);
            treeTimer.pop();
            treeTimer.push("build session builder");
            final ImhotepSession build = sessionBuilder.build();
            treeTimer.pop();
            final ImhotepSession session = closer.register(wrapSession(fieldAliases, build, datasetDimensions, Sets.union(sessionIntFields, sessionStringFields)));
            treeTimer.pop();

            treeTimer.push("determine time range");
            final DateTime earliestStart = Ordering.natural().min(Iterables.transform(chosenShards, new Function<ShardIdWithVersion, DateTime>() {
                public DateTime apply(ShardIdWithVersion input) {
                    return input.getStart();
                }
            }));
            final DateTime latestEnd = Ordering.natural().max(Iterables.transform(chosenShards, new Function<ShardIdWithVersion, DateTime>() {
                public DateTime apply(@Nullable ShardIdWithVersion input) {
                    return input.getEnd();
                }
            }));
            treeTimer.pop();
            final boolean isRamsesIndex = datasetInfo.getIntFields().isEmpty();
            final String timeField = isRamsesIndex ? "time" : "unixtime";
            if (earliestStart.isBefore(startDateTime) || latestEnd.isAfter(endDateTime)) {
                treeTimer.push("regroup time range");
                session.pushStat(timeField);
                session.metricFilter(0, (int) (startDateTime.getMillis() / 1000), (int)((endDateTime.getMillis() - 1) / 1000), false);
                session.popStat();
                treeTimer.pop();
            }
            sessions.put(name, new ImhotepSessionInfo(session, displayName, DatasetDimensions.toUpperCase(datasetDimensions), upperCasedIntFields, upperCasedStringFields, startDateTime, endDateTime, timeField.toUpperCase()));
            if (i == 0) {
                firstStartTimeMill = startDateTime.getMillis();
            }
        }
    }

    private static Set<String> upperCase(Collection<String> collection) {
        final Set<String> result = new HashSet<>(collection.size());
        for (final String value : collection) {
            result.add(value.toUpperCase());
        }
        return result;
    }

    private static ImhotepSession wrapSession(Map<String, String> fieldAliases, ImhotepSession build, DatasetDimensions datasetDimensions, Set<String> fieldNames) {
        final DimensionsTranslator translated = new DimensionsTranslator(build, datasetDimensions);
        final CaseInsensitiveImhotepSession caseInsensitive = new CaseInsensitiveImhotepSession(translated, fieldNames);
        final FieldAliasingImhotepSession aliased = new FieldAliasingImhotepSession(caseInsensitive, fieldAliases);
        final GroupMultiRemapRuleRewriter groupMultiRemapRuleRewriter = new GroupMultiRemapRuleRewriter(aliased);
        return groupMultiRemapRuleRewriter;
    }

    private static final Pattern relativePattern = Pattern.compile("(\\d+)([smhdwMy])");
    private static DateTime parseDateTime(String descriptor, WallClock clock) {
        descriptor = descriptor.trim();
        final DateTime startOfToday = new DateTime(clock.currentTimeMillis()).withTimeAtStartOfDay();
        if (descriptor.equals("today")) {
            return startOfToday;
        } else if (descriptor.equals("yesterday")) {
            return startOfToday.minusDays(1);
        } else if (descriptor.equals("tomorrow")) {
            return startOfToday.plusDays(1);
        } else if (relativePattern.matcher(descriptor).matches()) {
            final Matcher matcher = relativePattern.matcher(descriptor);
            matcher.matches();
            final int offset = Integer.parseInt(matcher.group(1));
            final String unit = matcher.group(2);
            DateTime result = startOfToday;
            switch (unit) {
                case "s":
                    result = result.minusSeconds(offset);
                    break;
                case "m":
                    result = result.minusMinutes(offset);
                    break;
                case "h":
                    result = result.minusHours(offset);
                    break;
                case "d":
                    result = result.minusDays(offset);
                    break;
                case "w":
                    result = result.minusWeeks(offset);
                    break;
                case "M":
                    result = result.minusMonths(offset);
                    break;
                case "y":
                    result = result.minusYears(offset);
                    break;
                default:
                    throw new RuntimeException("Unrecognized unit: " + unit);
            }
            return result;
        } else {
            try {
                return DateTime.parse(descriptor.replaceAll(" ", "T"));
            } catch (final IllegalArgumentException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public void evaluateCommand(JsonNode commandTree, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final String commandTreeString = commandTree.toString();
        timer.push("evaluateCommand " + (commandTreeString.length() > 500 ? (commandTreeString.substring(0, 500) + "[...](log truncated)") : commandTreeString));
        try {
            final Command command = Commands.parseCommand(commandTree, new Function<String, PerGroupConstant>() {
                public PerGroupConstant apply(String s) {
                    return namedMetricLookup(s);
                }
            }, groupKeySet);
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

    public void evaluateCommandToTSV(JsonNode commandTree, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        timer.push("evaluateCommandToTSV " + commandTree);
        try {
            final Command command = Commands.parseCommand(commandTree, new Function<String, PerGroupConstant>() {
                @Override
                public PerGroupConstant apply(String s) {
                    return namedMetricLookup(s);
                }
            }, groupKeySet);
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
                        for (int i = 0; i < stats.length; i++) {
                            final double stat = stats[i];
                            if (i < formatStrings.length && formatStrings[i] != null) {
                                sb.append(String.format(formatStrings[i], stat)).append('\t');
                            } else if (DoubleMath.isMathematicalInteger(stat)) {
                                sb.append(String.format("%.0f", stat)).append('\t');
                            } else {
                                sb.append(stat).append('\t');
                            }
                        }
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
            metricIndexes.put(push, index);
            final String sessionName = push.sessionName;
            if (!dryRun) {
                sessions.get(sessionName).session.pushStats(push.pushes);
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

    public long getFirstStartTimeMill() {
        return firstStartTimeMill;
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
            final ImhotepSession session = sessionInfo.session;
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
        }
        final int result = (int) (oldNumGroups * Math.ceil(((double) end - start) / unitSize));
        timer.pop();
        return result;
    }

    public void densify(GroupKeySet groupKeySet) throws ImhotepOutOfMemoryException {
        timer.push("densify");
        final BitSet anyPresent = new BitSet();
        // TODO: Parallelize?
        for (final ImhotepSession session : getSessionsMapRaw().values()) {
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

    public Map<String, ImhotepSession> getSessionsMapRaw() {
        final Map<String, ImhotepSession> sessionMap = Maps.newHashMap();
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

    public void checkGroupLimit(int numGroups) {
        if (groupLimit > 0 && numGroups > groupLimit) {
            throw new IllegalArgumentException("Number of groups [" + numGroups + "] exceeds the group limit [" + groupLimit + "]");
        }
        log.debug("checkGroupLimit(" + numGroups + ")");
    }

    // TODO: Parallelize across sessions?
    public void popStats() {
        timer.push("popStats");
        for (final ImhotepSessionInfo imhotepSessionInfo : sessions.values()) {
            final ImhotepSession session = imhotepSessionInfo.session;
            final int numStats = session.getNumStats();
            for (int i = 0; i < numStats; i++) {
                session.popStat();
            }
        }
        timer.pop();
    }

    public void regroup(GroupMultiRemapRule[] rules, boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        timer.push("regroup");
        for (final ImhotepSessionInfo sessionInfo : sessions.values()) {
            sessionInfo.session.regroup(rules, errorOnCollisions);
        }
        timer.pop();
    }

    public void regroup(GroupRemapRule[] rules) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        timer.push("regroup");
        for (final ImhotepSessionInfo sessionInfo : sessions.values()) {
            sessionInfo.session.regroup(rules);
        }
        timer.pop();
    }

    public void popStat() {
        // TODO: Parallelize
        timer.push("popStat");
        for (final ImhotepSessionInfo sessionInfo : sessions.values()) {
            sessionInfo.session.popStat();
        }
        timer.pop();
    }

    public void intOrRegroup(String field, long[] terms, int targetGroup, int negativeGroup, int positiveGroup, Set<String> scope) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        timer.push("intOrRegroup");
        for (final String s : scope) {
            if (sessions.containsKey(s)) {
                final ImhotepSessionInfo sessionInfo = sessions.get(s);
                sessionInfo.session.intOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
            }
        }
        timer.pop();
    }

    public void stringOrRegroup(String field, String[] terms, int targetGroup, int negativeGroup, int positiveGroup, Set<String> scope) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        timer.push("stringOrRegroup");
        for (final String s : scope) {
            if (sessions.containsKey(s)) {
                final ImhotepSessionInfo sessionInfo = sessions.get(s);
                sessionInfo.session.stringOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
            }
        }
        timer.pop();
    }

    public void regroup(QueryRemapRule rule, Set<String> scope) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        timer.push("regroup");
        for (final String s : scope) {
            if (sessions.containsKey(s)) {
                sessions.get(s).session.regroup(rule);
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
                v.session.regexRegroup(field, regex, targetGroup, negativeGroup, positiveGroup);
            }
        }
        timer.pop();
    }

    public void randomRegroup(String field, boolean isIntField, String seed, double probability, int targetGroup, int positiveGroup, int negativeGroup, ImmutableSet<String> scope) throws ImhotepOutOfMemoryException {
        // TODO: Parallelize
        timer.push("randomRegroup");
        for (final Map.Entry<String, ImhotepSessionInfo> entry : sessions.entrySet()) {
            if (scope.contains(entry.getKey())) {
                entry.getValue().session.randomRegroup(field, isIntField, seed, probability, targetGroup, negativeGroup, positiveGroup);
            }
        }
        timer.pop();
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
                Closer closer, ImhotepSession session, String field, IntList sessionMetricIndexes, @Nullable Integer presenceIndex,
                Optional<RemoteTopKParams> topKParams, Optional<Integer> ftgsRowLimit) {
            final FTGSIterator it = closer.register(getFTGSIterator(session, field, true, topKParams, ftgsRowLimit));
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
    }

    public static void iterateMultiInt(Map<String, ImhotepSession> sessions, Map<String, IntList> metricIndexes, Map<String, Integer> presenceIndexes, String field, IntIterateCallback callback) throws IOException {
        iterateMultiInt(sessions, metricIndexes, presenceIndexes, field, Optional.<RemoteTopKParams>absent(), Optional.<Integer>absent(), callback);
    }

    public static void iterateMultiInt(
            Map<String, ImhotepSession> sessions, Map<String, IntList> metricIndexes, Map<String, Integer> presenceIndexes, String field,
            Optional<RemoteTopKParams> topKParams, Optional<Integer> ftgsRowLimit, IntIterateCallback callback) throws IOException {
        iterateMultiInt(sessions, metricIndexes, presenceIndexes, field, topKParams, ftgsRowLimit, callback, Optional.<TreeTimer>absent());
    }

    public static void iterateMultiInt(
            Map<String, ImhotepSession> sessions, Map<String, IntList> metricIndexes, Map<String, Integer> presenceIndexes,
            String field, Optional<RemoteTopKParams> topKParams, Optional<Integer> ftgsRowLimit, IntIterateCallback callback, Optional<TreeTimer> timer) throws IOException
    {
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
            final PriorityQueue<SessionIntIterationState> pq = new PriorityQueue<>(sessions.size(), comparator);
            if (timer.isPresent()) {
                timer.get().push("call imhotep iterator");
            }
            for (final String sessionName : sessions.keySet()) {
                final ImhotepSession session = sessions.get(sessionName);
                final IntList sessionMetricIndexes = Objects.firstNonNull(metricIndexes.get(sessionName), new IntArrayList());
                final Integer presenceIndex = presenceIndexes.get(sessionName);
                final Optional<SessionIntIterationState> constructed = SessionIntIterationState.construct(
                        closer, session, field, sessionMetricIndexes, presenceIndex, topKParams, ftgsRowLimit);
                if (constructed.isPresent()) {
                    pq.add(constructed.get());
                }
            }
            if (timer.isPresent()) {
                timer.get().pop();
            }
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

    private static void copyStats(SessionIntIterationState state, long[] dst) {
        for (int i = 0; i < state.metricIndexes.size(); i++) {
            dst[state.metricIndexes.getInt(i)] = state.statsBuff[i];
        }
        if (state.presenceIndex != null) {
            dst[state.presenceIndex] = 1;
        }
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

        static Optional<SessionStringIterationState> construct(Closer closer, ImhotepSession session, String field, IntList sessionMetricIndexes, @Nullable Integer presenceIndex) {
            final FTGSIterator it = closer.register(session.getFTGSIterator(new String[0], new String[]{field}));
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
    }

    public static void iterateMultiString(Map<String, ImhotepSession> sessions, Map<String, IntList> metricIndexes, Map<String, Integer> presenceIndexes, String field, StringIterateCallback callback) throws IOException {
        iterateMultiString(sessions, metricIndexes, presenceIndexes, field, Optional.<RemoteTopKParams>absent(), Optional.<Integer>absent(), callback);
    }

    public static void iterateMultiString(
            Map<String, ImhotepSession> sessions, Map<String, IntList> metricIndexes, Map<String, Integer> presenceIndexes, String field,
            Optional<RemoteTopKParams> topKParams, Optional<Integer> limit, StringIterateCallback callback) throws IOException {
        iterateMultiString(sessions, metricIndexes, presenceIndexes, field, topKParams, limit, callback, Optional.<TreeTimer>absent());
    }

    public static void iterateMultiString(
            Map<String, ImhotepSession> sessions, Map<String, IntList> metricIndexes, Map<String, Integer> presenceIndexes, String field,
            Optional<RemoteTopKParams> topKParams, Optional<Integer> limit, StringIterateCallback callback, Optional<TreeTimer> timer) throws IOException {
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
            if (timer.isPresent()) {
                timer.get().push("call imhotep iterator");
            }
            final PriorityQueue<SessionStringIterationState> pq = new PriorityQueue<>(sessions.size(), comparator);
            for (final String sessionName : sessions.keySet()) {
                final ImhotepSession session = sessions.get(sessionName);
                final IntList sessionMetricIndexes = Objects.firstNonNull(metricIndexes.get(sessionName), new IntArrayList());
                final Integer presenceIndex = presenceIndexes.get(sessionName);
                final Optional<SessionStringIterationState> constructed = SessionStringIterationState.construct(closer, session, field, sessionMetricIndexes, presenceIndex);
                if (constructed.isPresent()) {
                    pq.add(constructed.get());
                }
            }
            if (timer.isPresent()) {
                timer.get().pop();
            }
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
        }
    }

    private static FTGSIterator getFTGSIterator(
            final ImhotepSession session, final String field, final boolean isIntField,
            final Optional<RemoteTopKParams> topKParams, final Optional<Integer> limit) {
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

    private static void copyStats(SessionStringIterationState state, long[] dst) {
        for (int i = 0; i < state.metricIndexes.size(); i++) {
            dst[state.metricIndexes.getInt(i)] = state.statsBuff[i];
        }
        if (state.presenceIndex != null) {
            dst[state.presenceIndex] = 1;
        }
    }

    // TODO: JsonSerializable..?
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
        public final ImhotepSession session;
        public final String displayName;
        public final DatasetDimensions datasetDimensions;
        public final Collection<String> intFields;
        public final Collection<String> stringFields;
        public final DateTime startTime;
        public final DateTime endTime;
        public final String timeFieldName;

        @VisibleForTesting
        ImhotepSessionInfo(ImhotepSession session, String displayName, DatasetDimensions datasetDimensions, Collection<String> intFields, Collection<String> stringFields, DateTime startTime, DateTime endTime, String timeFieldName) {
            this.session = session;
            this.displayName = displayName;
            this.datasetDimensions = datasetDimensions;
            this.intFields = Collections.unmodifiableCollection(intFields);
            this.stringFields = Collections.unmodifiableCollection(stringFields);
            this.startTime = startTime;
            this.endTime = endTime;
            this.timeFieldName = timeFieldName;
        }
    }
}
