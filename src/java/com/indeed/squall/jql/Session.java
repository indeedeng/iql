package com.indeed.squall.jql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.common.util.Pair;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.squall.jql.commands.ComputeAndCreateGroupStatsLookup;
import com.indeed.squall.jql.commands.CreateGroupStatsLookup;
import com.indeed.squall.jql.commands.ExplodeDayOfWeek;
import com.indeed.squall.jql.commands.ExplodeGroups;
import com.indeed.squall.jql.commands.ExplodePerGroup;
import com.indeed.squall.jql.commands.ExplodeSessionNames;
import com.indeed.squall.jql.commands.FilterDocs;
import com.indeed.squall.jql.commands.GetGroupDistincts;
import com.indeed.squall.jql.commands.GetGroupPercentiles;
import com.indeed.squall.jql.commands.GetGroupStats;
import com.indeed.squall.jql.commands.GetNumGroups;
import com.indeed.squall.jql.commands.Iterate;
import com.indeed.squall.jql.commands.IterateAndExplode;
import com.indeed.squall.jql.commands.MetricRegroup;
import com.indeed.squall.jql.commands.TimeRegroup;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleCollection;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author jwolfe
 */
public class Session {
    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    private static final Logger log = Logger.getLogger(Session.class);

    public List<GroupKey> groupKeys = Lists.newArrayList(null, new GroupKey(null, 1, null));
    public final Map<String, SavedGroupStats> savedGroupStats = Maps.newHashMap();
    public int currentDepth = 0;

    public final Map<String, ImhotepSessionInfo> sessions;
    public int numGroups = 1;

    public static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        final SimpleModule module = new SimpleModule();
        module.addSerializer(TermSelects.class, new JsonSerializer<TermSelects>() {
            @Override
            public void serialize(TermSelects value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
                jgen.writeStartObject();
                jgen.writeObjectField("field", value.field);
                if (value.isIntTerm) {
                    jgen.writeObjectField("intTerm", value.intTerm);
                } else {
                    jgen.writeObjectField("stringTerm", value.stringTerm);
                }
                jgen.writeObjectField("selects", value.selects);
                if (value.groupKey != null) {
                    jgen.writeObjectField("key", value.groupKey);
                }
                jgen.writeEndObject();
            }
        });
        module.addSerializer(GroupKey.class, new JsonSerializer<GroupKey>() {
            @Override
            public void serialize(GroupKey value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
                jgen.writeObject(value.asList());
            }
        });
        MAPPER.registerModule(module);
        MAPPER.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    }

    public static final String INFINITY_SYMBOL = "∞";

    public Session(Map<String, ImhotepSessionInfo> sessions) {
        this.sessions = sessions;
    }

    public static void main(String[] args) throws Exception {
        org.apache.log4j.BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        final Properties props = new Properties();
        final InputStream propsStream = Session.class.getResourceAsStream("config.properties");
        if (propsStream != null) {
            props.load(propsStream);
        }
        final String zkPath = (String) props.getOrDefault("zk_path", "***REMOVED***");
        log.info("zkPath = " + zkPath);

        final ImhotepClient client = new ImhotepClient(zkPath, true);

        final int wsSocketPort = Integer.parseInt((String) props.getOrDefault("ws_socket", "8001"));
        log.info("wsSocketPort = " + wsSocketPort);
        final int unixSocketPort = Integer.parseInt((String) props.getOrDefault("unix_socket", "28347"));
        log.info("unixSocketPort = " + unixSocketPort);
        try (WebSocketSessionServer wsServer = new WebSocketSessionServer(client, new InetSocketAddress(wsSocketPort))) {
            new Thread(wsServer::run).start();

            final ServerSocket serverSocket = new ServerSocket(unixSocketPort);
            while (true) {
                final Socket clientSocket = serverSocket.accept();
                new Thread(() -> {
                    try (final PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                         final BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {

                        final Supplier<JsonNode> nodeSupplier = () -> {
                            try {
                                final String line = in.readLine();
                                return line == null ? null : MAPPER.readTree(line);
                            } catch (final IOException e) {
                                throw Throwables.propagate(e);
                            }
                        };

                        final Consumer<String> resultConsumer = out::println;

                        processConnection(client, nodeSupplier, resultConsumer);
                    } catch (Throwable e) {
                        log.error("wat", e);
                        System.out.println("e = " + e);
                    }
                }).start();
            }
        }
    }

    public static void processConnection(ImhotepClient client, Supplier<JsonNode> in, Consumer<String> out) throws IOException, ImhotepOutOfMemoryException {
        final JsonNode sessionRequest = in.get();
        if (sessionRequest.has("describe")) {
            processDescribe(client, out, sessionRequest);
        } else {
            try (final Closer closer = Closer.create()) {
                final Optional<Session> session = createSession(client, sessionRequest, closer, out);
                if (session.isPresent()) {
                    // Not using ifPresent because of exceptions
                    final Session session1 = session.get();
                    JsonNode inputTree;
                    while ((inputTree = in.get()) != null) {
                        System.out.println("inputLine = " + inputTree);
                        session1.evaluateCommand(inputTree, out);
                        System.out.println("Evaluated.");
                    }
                }
            } catch (Exception e) {
                final String error = Session.MAPPER.writeValueAsString(ImmutableMap.of("error", "1", "message", ""+e.getMessage(), "cause", ""+e.getCause(), "stackTrace", ""+ Arrays.toString(e.getStackTrace())));
                System.out.println("error = " + error);
                out.accept(error);
                throw e;
            }
        }
    }

    public static void processDescribe(ImhotepClient client, Consumer<String> out, JsonNode sessionRequest) throws JsonProcessingException {
        final DatasetInfo datasetInfo = client.getDatasetToShardList().get(sessionRequest.get("describe").textValue());
        final DatasetDescriptor datasetDescriptor = DatasetDescriptor.from(datasetInfo);
        out.accept(MAPPER.writeValueAsString(datasetDescriptor));
    }

    public static Optional<Session> createSession(ImhotepClient client, JsonNode sessionRequest, Closer closer, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final Map<String, ImhotepSessionInfo> sessions = Maps.newHashMap();
        if (sessionRequest.has("commands")) {
            createSubSessions(client, sessionRequest.get("datasets"), closer, sessions);
            final Session session = new Session(sessions);
            final JsonNode commands = sessionRequest.get("commands");
            for (int i = 0; i < commands.size(); i++) {
                final JsonNode command = commands.get(i);
                final boolean isLast = i == commands.size() - 1;
                if (isLast) {
                    session.evaluateCommandToTSV(command, out);
                } else {
                    session.evaluateCommand(command, x -> {});
                }
            }
            return Optional.empty();
        } else {
            createSubSessions(client, sessionRequest, closer, sessions);
            out.accept("opened");
            return Optional.of(new Session(sessions));
        }
    }

    private static void createSubSessions(ImhotepClient client, JsonNode sessionRequest, Closer closer, Map<String, ImhotepSessionInfo> sessions) {
        for (int i = 0; i < sessionRequest.size(); i++) {
            final JsonNode elem = sessionRequest.get(i);
            final String dataset = elem.get("dataset").textValue();
            final String start = elem.get("start").textValue();
            final String end = elem.get("end").textValue();
            final String name = elem.has("name") ? elem.get("name").textValue() : dataset;

            final DatasetInfo datasetInfo = client.getDatasetToShardList().get(dataset);
            final Collection<String> sessionIntFields = datasetInfo.getIntFields();
            final Collection<String> sessionStringFields = datasetInfo.getStringFields();
            final DateTime startDateTime = parseDateTime(start);
            final DateTime endDateTime = parseDateTime(end);
            final ImhotepSession session = closer.register(client.sessionBuilder(dataset, startDateTime, endDateTime).build());

            final boolean isRamsesIndex = datasetInfo.getIntFields().isEmpty();

            sessions.put(name, new ImhotepSessionInfo(session, sessionIntFields, sessionStringFields, startDateTime, endDateTime, isRamsesIndex ? "time" : "unixtime"));
        }
    }

    private static final Pattern relativePattern = Pattern.compile("(\\d+)([smhdwMy])");
    private static DateTime parseDateTime(String descriptor) {
        final DateTime startOfToday = DateTime.now().withTimeAtStartOfDay();
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
        final Object command = Commands.parseCommand(commandTree, this::namedMetricLookup);
        evaluateCommandInternal(commandTree, out, command);
    }

    public void evaluateCommandToTSV(JsonNode commandTree, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        final Object command = Commands.parseCommand(commandTree, this::namedMetricLookup);
        if (command instanceof Iterate) {
            final List<List<List<TermSelects>>> results = Iterate.performIterate((Iterate) command, this);
            final StringBuilder sb = new StringBuilder();
            for (final List<List<TermSelects>> groupFieldTerms : results) {
                final List<TermSelects> groupTerms = groupFieldTerms.get(0);
                for (final TermSelects termSelects : groupTerms) {
                    final List<String> keyColumns = termSelects.groupKey.asList();
                    keyColumns.forEach(k -> sb.append(k).append('\t'));
                    if (termSelects.isIntTerm) {
                        sb.append(termSelects.intTerm).append('\t');
                    } else {
                        sb.append(termSelects.stringTerm).append('\t');
                    }
                    for (final double stat : termSelects.selects) {
                        if (DoubleMath.isMathematicalInteger(stat)) {
                            sb.append((long)stat).append('\t');
                        } else {
                            sb.append(stat).append('\t');
                        }
                    }
                    sb.setLength(sb.length() - 1);
                    sb.append('\n');
                }
            }
            sb.setLength(sb.length() - 1);
            out.accept(MAPPER.writeValueAsString(Arrays.asList(sb.toString())));
        } else if (command instanceof GetGroupStats) {
            final GetGroupStats getGroupStats = (GetGroupStats) command;
            final List<GroupStats> results = GetGroupStats.getGroupStats(getGroupStats, groupKeys, getSessionsMapRaw(), numGroups, getGroupStats.returnGroupKeys);
            final StringBuilder sb = new StringBuilder();
            for (final GroupStats result : results) {
                final List<String> keyColumns = result.key.asList();
                keyColumns.forEach(k -> sb.append(k).append('\t'));
                for (final double stat : result.stats) {
                    if (DoubleMath.isMathematicalInteger(stat)) {
                        sb.append((long)stat).append('\t');
                    } else {
                        sb.append(stat).append('\t');
                    }
                }
                if (keyColumns.size() + result.stats.length > 0) {
                    sb.setLength(sb.length() - 1);
                }
                sb.append('\n');
            }
            if (results.size() > 0) {
                sb.setLength(sb.length() - 1);
            }
            out.accept(MAPPER.writeValueAsString(Arrays.asList(sb.toString())));
        } else {
            throw new IllegalArgumentException("Don't know how to evaluate [" + command + "] to TSV");
        }
    }

    private void evaluateCommandInternal(JsonNode commandTree, Consumer<String> out, Object command) throws ImhotepOutOfMemoryException, IOException {
        if (command instanceof Iterate) {
            final Iterate iterate = (Iterate) command;
            final List<List<List<TermSelects>>> allTermSelects = Iterate.performIterate(iterate, this);
            out.accept(MAPPER.writeValueAsString(allTermSelects));
        } else if (command instanceof FilterDocs) {
            final FilterDocs filterDocs = (FilterDocs) command;
            FilterDocs.filterDocs(filterDocs, this);
            out.accept("{}");
        } else if (command instanceof ExplodeGroups) {
            final ExplodeGroups explodeGroups = (ExplodeGroups) command;
            ExplodeGroups.explodeGroups(explodeGroups, this);
            out.accept("success");
        } else if (command instanceof MetricRegroup) {
            final MetricRegroup metricRegroup = (MetricRegroup) command;
            MetricRegroup.metricRegroup(metricRegroup, this);
            out.accept("success");
        } else if (command instanceof TimeRegroup) {
            final TimeRegroup timeRegroup = (TimeRegroup) command;
            TimeRegroup.timeRegroup(timeRegroup, this);
            out.accept("success");
        } else if (command instanceof GetGroupStats) {
            final GetGroupStats getGroupStats = (GetGroupStats) command;
            final List<GroupStats> results = GetGroupStats.getGroupStats(getGroupStats, groupKeys, getSessionsMapRaw(), numGroups, getGroupStats.returnGroupKeys);
            out.accept(MAPPER.writeValueAsString(results));
        } else if (command instanceof CreateGroupStatsLookup) {
            final CreateGroupStatsLookup createGroupStatsLookup = (CreateGroupStatsLookup) command;
            final String lookupName = CreateGroupStatsLookup.createGroupStatsLookup(createGroupStatsLookup, this);
            out.accept(MAPPER.writeValueAsString(Arrays.asList(lookupName)));
        } else if (command instanceof GetGroupDistincts) {
            final GetGroupDistincts getGroupDistincts = (GetGroupDistincts) command;
            final long[] groupCounts = GetGroupDistincts.getGroupDistincts(getGroupDistincts, this);
            out.accept(MAPPER.writeValueAsString(groupCounts));
        } else if (command instanceof GetGroupPercentiles) {
            final GetGroupPercentiles getGroupPercentiles = (GetGroupPercentiles) command;
            final long[][] results = GetGroupPercentiles.getGroupPercentiles(getGroupPercentiles, this);
            out.accept(MAPPER.writeValueAsString(results));
        } else if (command instanceof GetNumGroups) {
            out.accept(MAPPER.writeValueAsString(Collections.singletonList(numGroups)));
        } else if (command instanceof ExplodePerGroup) {
            ExplodePerGroup.performExplodePerGroup((ExplodePerGroup) command, this);
            out.accept("success");
        } else if (command instanceof ExplodeDayOfWeek) {
            ExplodeDayOfWeek.explodeDayOfWeek(this);
            out.accept("success");
        } else if (command instanceof ExplodeSessionNames) {
            final TreeSet<String> names = Sets.newTreeSet(sessions.keySet());
            // TODO: This
            throw new UnsupportedOperationException("Get around to implementing ExplodeSessionNames");
        } else if (command instanceof IterateAndExplode) {
            final IterateAndExplode iterateAndExplode = (IterateAndExplode) command;
            IterateAndExplode.iterateAndExplode(iterateAndExplode, this);
        } else if (command instanceof ComputeAndCreateGroupStatsLookup) {
            // TODO: Seriously? Serializing to JSON and then back? To the same program?
            final ComputeAndCreateGroupStatsLookup computeAndCreateGroupStatsLookup = (ComputeAndCreateGroupStatsLookup) command;
            final AtomicReference<String> reference = new AtomicReference<>();
            final Object computation = computeAndCreateGroupStatsLookup.computation;
            evaluateCommandInternal(null, reference::set, computation);
            double[] results;
            if (computation instanceof GetGroupDistincts) {
                results = MAPPER.readValue(reference.get(), new TypeReference<double[]>(){});
            } else if (computation instanceof GetGroupPercentiles) {
                final List<double[]> intellijDoesntLikeInlining = MAPPER.readValue(reference.get(), new TypeReference<List<double[]>>(){});
                results = intellijDoesntLikeInlining.get(0);
            } else if (computation instanceof GetGroupStats) {
                final List<GroupStats> groupStats = MAPPER.readValue(reference.get(), new TypeReference<List<GroupStats>>(){});
                results = new double[groupStats.size()];
                for (int i = 0; i < groupStats.size(); i++) {
                    results[i] = groupStats.get(i).stats[0];
                }
            } else {
                throw new IllegalArgumentException("Shouldn't be able to reach here. Bug in ComputeAndCreateGroupStatsLookup parser.");
            }
            evaluateCommandInternal(null, out, new CreateGroupStatsLookup(prependZero(results), computeAndCreateGroupStatsLookup.name));
        } else if (command instanceof Commands.ExplodeByAggregatePercentile) {
            final Commands.ExplodeByAggregatePercentile explodeCommand = (Commands.ExplodeByAggregatePercentile) command;
            final String field = explodeCommand.field;
            final AggregateMetric metric = explodeCommand.metric;
            final int numBuckets = explodeCommand.numBuckets;
            final HashMap<QualifiedPush, Integer> metricIndexes = new HashMap<>();
            final HashMap<String, IntList> sessionMetricIndexes = new HashMap<>();
            pushMetrics(metric.requires(), metricIndexes, sessionMetricIndexes);
            metric.register(metricIndexes, groupKeys);

            final List<GroupKey> nextGroupKeys = Lists.newArrayListWithCapacity(1 + numGroups * numBuckets);
            nextGroupKeys.add(null);

            if (isIntField(field)) {
                final Int2ObjectOpenHashMap<Long2DoubleOpenHashMap> perGroupTermToValue = new Int2ObjectOpenHashMap<>();
                iterateMultiInt(getSessionsMapRaw(), sessionMetricIndexes, field,
                        (term, stats, group) -> perGroupTermToValue.computeIfAbsent(group, g -> new Long2DoubleOpenHashMap()).put(term, metric.apply(term, stats, group))
                );
                final List<GroupMultiRemapRule> rules = Lists.newArrayListWithCapacity(numGroups);
                for (int group = 1; group <= numGroups; group++) {
                    final int groupBase = 1 + (group - 1) * numBuckets;
                    final Long2DoubleOpenHashMap termToValue = perGroupTermToValue.get(group);
                    final double[] percentiles = getPercentiles(termToValue.values(), numBuckets);
                    final Int2ObjectOpenHashMap<LongArrayList> groupOffsetToTerms = new Int2ObjectOpenHashMap<>();
                    for (final Long2DoubleMap.Entry entry : termToValue.long2DoubleEntrySet()) {
                        final int groupOffset = findPercentile(entry.getDoubleValue(), percentiles);
                        groupOffsetToTerms.computeIfAbsent(groupOffset, k -> new LongArrayList()).add(entry.getLongKey());
                    }

                    final int arraySize = groupOffsetToTerms.values().stream().mapToInt(LongArrayList::size).sum();
                    final int[] positiveGroups = new int[arraySize];
                    final RegroupCondition[] conditions = new RegroupCondition[arraySize];
                    int arrayIndex = 0;
                    for (int i = 0; i < numBuckets; i++) {
                        final LongArrayList terms = groupOffsetToTerms.get(i);
                        if (terms != null) {
                            for (final long term : terms) {
                                positiveGroups[arrayIndex] = groupBase + i;
                                conditions[arrayIndex] = new RegroupCondition(field, true, term, null, false);
                                arrayIndex++;
                            }
                        }
                        final String keyTerm = "[" + (double) i / numBuckets + ", " + (double) (i + 1) / numBuckets + ")";
                        nextGroupKeys.add(new GroupKey(keyTerm, nextGroupKeys.size(), groupKeys.get(group)));
                    }
                    rules.add(new GroupMultiRemapRule(group, 0, positiveGroups, conditions));
                }
                final GroupMultiRemapRule[] rulesArr = rules.toArray(new GroupMultiRemapRule[rules.size()]);
                sessions.values().forEach(s -> unchecked(() -> s.session.regroup(rulesArr)));
            } else if (isStringField(field)) {
                final Int2ObjectOpenHashMap<Object2DoubleOpenHashMap<String>> perGroupTermToValue = new Int2ObjectOpenHashMap<>();
                iterateMultiString(getSessionsMapRaw(), sessionMetricIndexes, field,
                        (term, stats, group) -> perGroupTermToValue.computeIfAbsent(group, g -> new Object2DoubleOpenHashMap<>()).put(term, metric.apply(term, stats, group))
                );
                final List<GroupMultiRemapRule> rules = Lists.newArrayListWithCapacity(numGroups);
                for (int group = 1; group <= numGroups; group++) {
                    final int groupBase = 1 + (group - 1) * numBuckets;
                    final Object2DoubleOpenHashMap<String> termToValue = perGroupTermToValue.get(group);
                    final double[] percentiles = getPercentiles(termToValue.values(), numBuckets);
                    final Int2ObjectOpenHashMap<ArrayList<String>> groupOffsetToTerms = new Int2ObjectOpenHashMap<>();
                    for (final Map.Entry<String, Double> entry : termToValue.entrySet()) {
                        final int groupOffset = findPercentile(entry.getValue(), percentiles);
                        groupOffsetToTerms.computeIfAbsent(groupOffset, k -> new ArrayList<>()).add(entry.getKey());
                    }

                    final int arraySize = groupOffsetToTerms.values().stream().mapToInt(ArrayList::size).sum();
                    final int[] positiveGroups = new int[arraySize];
                    final RegroupCondition[] conditions = new RegroupCondition[arraySize];
                    int arrayIndex = 0;
                    for (int i = 0; i < numBuckets; i++) {
                        final ArrayList<String> terms = groupOffsetToTerms.get(i);
                        if (terms != null) {
                            for (final String term : terms) {
                                positiveGroups[arrayIndex] = groupBase + i;
                                conditions[arrayIndex] = new RegroupCondition(field, false, 0, term, false);
                                arrayIndex++;
                            }
                        }
                        final String keyTerm = "[" + (double) i / numBuckets + ", " + (double) (i + 1) / numBuckets + ")";
                        nextGroupKeys.add(new GroupKey(keyTerm, nextGroupKeys.size(), groupKeys.get(group)));
                    }
                    rules.add(new GroupMultiRemapRule(group, 0, positiveGroups, conditions));
                }
                final GroupMultiRemapRule[] rulesArr = rules.toArray(new GroupMultiRemapRule[rules.size()]);
                sessions.values().forEach(s -> unchecked(() -> s.session.regroup(rulesArr)));
            } else {
                throw new IllegalArgumentException("Field is neither int field nor string field: " + field);
            }
            sessions.values().forEach(v -> unchecked(() -> {
                while (v.session.getNumStats() > 0) {
                    v.session.popStat();
                }
            }));

            numGroups = nextGroupKeys.size() - 1;
            groupKeys = nextGroupKeys;
            currentDepth += 1;

            out.accept("ExlodedByAggregatePercentile");
        } else if (command instanceof Commands.ExplodePerDocPercentile) {
            final Commands.ExplodePerDocPercentile explodeCommand = (Commands.ExplodePerDocPercentile) command;
            final String field = explodeCommand.field;
            final int numBuckets = explodeCommand.numBuckets;

            final long[] counts = new long[numGroups + 1];
            sessions.values().forEach(s -> unchecked(() -> {
                s.session.pushStat("count()");
                final long[] stats = s.session.getGroupStats(0);
                for (int i = 0; i < stats.length; i++) {
                    counts[i] += stats[i];
                }
            }));

            final long[] runningCounts = new long[numGroups + 1];
            final long[][] cutoffs = new long[numGroups + 1][numBuckets];
            final int[] soFar = new int[numGroups + 1];
            iterateMultiInt(getSessionsMapRaw(), sessions.keySet().stream().collect(Collectors.toMap(k -> k, k -> new IntArrayList(new int[]{0}))), field, new IntIterateCallback() {
                @Override
                public void term(long term, long[] stats, int group) {
                    runningCounts[group] += stats[0];
                    final int fraction = (int) Math.floor((double) numBuckets * runningCounts[group] / counts[group]);
                    for (int i = soFar[group] + 1; i < fraction; i++) {
                        cutoffs[group][i] = term;
                        soFar[group] = i;
                    }
                }
            });

            for (int group = 1; group <= numGroups; group++) {
                for (int idx = soFar[group] + 1; idx < numBuckets; idx++) {
                    cutoffs[group][idx] = Integer.MAX_VALUE;
                }
            }

            final List<GroupMultiRemapRule> rules = Lists.newArrayList();
            final List<GroupKey> nextGroupKeys = Lists.newArrayList();
            nextGroupKeys.add(null);
            for (int group = 1; group <= numGroups; group++) {
                final IntArrayList positiveGroups = new IntArrayList();
                final List<RegroupCondition> conditions = Lists.newArrayList();
                for (int bucket = 0; bucket < numBuckets; bucket++) {
                    if (bucket > 0 && cutoffs[group][bucket] == cutoffs[group][bucket - 1]) {
                        continue;
                    }
                    final int end = ArrayUtils.lastIndexOf(cutoffs[group], cutoffs[group][bucket]);
                    final String keyTerm = "[" + (double) bucket / numBuckets + ", " + (double) (end + 1) / numBuckets + ")";
                    final int newGroup = nextGroupKeys.size();
                    nextGroupKeys.add(new GroupKey(keyTerm, nextGroupKeys.size(), groupKeys.get(group)));
                    positiveGroups.add(newGroup);
                    conditions.add(new RegroupCondition(field, true, cutoffs[group][bucket], null, true));
                }
                final int[] positiveGroupsArr = positiveGroups.toIntArray(new int[positiveGroups.size()]);
                final RegroupCondition[] conditionsArr = conditions.toArray(new RegroupCondition[conditions.size()]);
                rules.add(new GroupMultiRemapRule(group, 0, positiveGroupsArr, conditionsArr));
            }

            final GroupMultiRemapRule[] rulesArr = rules.toArray(new GroupMultiRemapRule[rules.size()]);

            sessions.values().forEach(s -> unchecked(() -> {
                s.session.regroup(rulesArr);
                s.session.popStat();
            }));

            numGroups = nextGroupKeys.size() - 1;
            groupKeys = nextGroupKeys;
            currentDepth += 1;

        } else {
            throw new IllegalArgumentException("Invalid command: " + commandTree);
        }
    }

    private int findPercentile(double v, double[] percentiles) {
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
    private static double[] getPercentiles(DoubleCollection values, int k) {
        final DoubleArrayList list = new DoubleArrayList(values);
        list.sort(Double::compare);
        final double[] result = new double[k];
        for (int i = 0; i < k; i++) {
            result[i] = list.get((int) Math.ceil((double) list.size() * i / k));
        }
        return result;
    }

    // TODO: Any call sites of this could be optimized.
    private static double[] prependZero(double[] in) {
        final double[] out = new double[in.length + 1];
        System.arraycopy(in, 0, out, 1, in.length);
        return out;
    }

    public void registerMetrics(Map<QualifiedPush, Integer> metricIndexes, Iterable<AggregateMetric> metrics, Iterable<AggregateFilter> filters) {
        metrics.forEach(m -> m.register(metricIndexes, groupKeys));
        filters.forEach(f -> f.register(metricIndexes, groupKeys));
    }

    public void pushMetrics(Set<QualifiedPush> allPushes, Map<QualifiedPush, Integer> metricIndexes, Map<String, IntList> sessionMetricIndexes) throws ImhotepOutOfMemoryException {
        int numStats = 0;
        for (final QualifiedPush push : allPushes) {
            final int index = numStats++;
            metricIndexes.put(push, index);
            final String sessionName = push.sessionName;
            sessions.get(sessionName).session.pushStats(push.pushes);
            sessionMetricIndexes.computeIfAbsent(sessionName, k -> new IntArrayList()).add(index);
        }
    }

    public long getLatestEnd() {
        return sessions.values().stream().mapToLong(x -> x.endTime.getMillis()).max().getAsLong();
    }

    public long getEarliestStart() {
        return sessions.values().stream().mapToLong(x -> x.startTime.getMillis()).min().getAsLong();
    }

    public int performTimeRegroup(long start, long end, long unitSize, Optional<String> fieldOverride) {
        final int oldNumGroups = this.numGroups;
        sessions.values().forEach(sessionInfo -> unchecked(() -> {
            final ImhotepSession session = sessionInfo.session;
            session.pushStat(fieldOverride.orElseGet(() -> sessionInfo.timeFieldName));
            session.metricRegroup(0, start / 1000, end / 1000, unitSize / 1000, true);
            session.popStat();
        }));
        return (int) (oldNumGroups * Math.ceil(((double) end - start) / unitSize));
    }

    public void densify(Function<Integer, Pair<String, GroupKey>> indexedInfoProvider) throws ImhotepOutOfMemoryException {
        final BitSet anyPresent = new BitSet();
        getSessionsMapRaw().values().forEach(session -> unchecked(() -> {
            session.pushStat("count()");
            final long[] counts = session.getGroupStats(0);
            for (int i = 0; i < counts.length; i++) {
                if (counts[i] > 0L) {
                    anyPresent.set(i);
                }
            }
            session.popStat();
        }));

        final List<GroupKey> nextGroupKeys = Lists.newArrayList((GroupKey) null);
        final List<GroupRemapRule> rules = Lists.newArrayList();
        boolean anyNonIdentity = false;
        for (int i = 0; i < anyPresent.size(); i++) {
            if (anyPresent.get(i)) {
                final int newGroup = nextGroupKeys.size();
                final Pair<String, GroupKey> p = indexedInfoProvider.apply(i);
                nextGroupKeys.add(new GroupKey(p.getFirst(), newGroup, p.getSecond()));
                rules.add(new GroupRemapRule(i, new RegroupCondition("fakeField", true, 23L, null, false), newGroup, newGroup));
                if (newGroup != i) {
                    anyNonIdentity = true;
                }
            }
        }

        if (anyNonIdentity) {
            final GroupRemapRule[] ruleArray = rules.toArray(new GroupRemapRule[rules.size()]);
            getSessionsMapRaw().values().forEach(session -> unchecked(() -> session.regroup(ruleArray)));
        }

        numGroups = nextGroupKeys.size() - 1;
        groupKeys = nextGroupKeys;
    }

    public void assumeDense(Function<Integer, Pair<String, GroupKey>> indexedInfoProvider, int newNumGroups) throws ImhotepOutOfMemoryException {
        final List<GroupKey> nextGroupKeys = Lists.newArrayList((GroupKey) null);
        final List<GroupRemapRule> rules = Lists.newArrayList();
        for (int i = 1; i <= newNumGroups; i++) {
            final int newGroup = nextGroupKeys.size();
            final Pair<String, GroupKey> p = indexedInfoProvider.apply(i);
            nextGroupKeys.add(new GroupKey(p.getFirst(), newGroup, p.getSecond()));
            rules.add(new GroupRemapRule(i, new RegroupCondition("fakeField", true, 23L, null, false), newGroup, newGroup));
        }

        numGroups = nextGroupKeys.size() - 1;
        groupKeys = nextGroupKeys;
    }

    public Map<String, ImhotepSession> getSessionsMapRaw() {
        final Map<String, ImhotepSession> sessionMap = Maps.newHashMap();
        sessions.forEach((k,v) -> sessionMap.put(k, v.session));
        return sessionMap;
    }

    private Map<String, ImhotepSessionInfo> getSessionsMap() {
        return Maps.newHashMap(sessions);
    }

    public boolean isIntField(String field) {
        return sessions.values().stream().allMatch(x -> x.intFields.contains(field));
    }

    public boolean isStringField(String field) {
        return sessions.values().stream().allMatch(x -> x.stringFields.contains(field));
    }

    private AggregateMetric.PerGroupConstant namedMetricLookup(String name) {
        final SavedGroupStats savedStat = savedGroupStats.get(name);
        final int depthChange = currentDepth - savedStat.depth;
        final double[] stats = new double[numGroups + 1];
        for (int group = 1; group <= numGroups; group++) {
            GroupKey key = groupKeys.get(group);
            for (int i = 0; i < depthChange; i++) {
                key = key.parent;
            }
            stats[group] = savedStat.stats[key.index];
        }
        return new AggregateMetric.PerGroupConstant(stats);
    }

    public static void unchecked(RunnableWithException runnable) {
        try {
            runnable.run();
        } catch (final Throwable t) {
            log.error("unchecked error", t);
            throw Throwables.propagate(t);
        }
    }

    private static class SessionIntIterationState {
        public final FTGSIterator iterator;
        private final IntList metricIndexes;
        public final long[] statsBuff;
        public long nextTerm;
        public int nextGroup;

        private SessionIntIterationState(FTGSIterator iterator, IntList metricIndexes, long[] statsBuff, long nextTerm, int nextGroup) {
            this.iterator = iterator;
            this.metricIndexes = metricIndexes;
            this.statsBuff = statsBuff;
            this.nextTerm = nextTerm;
            this.nextGroup = nextGroup;
        }

        static Optional<SessionIntIterationState> construct(Closer closer, ImhotepSession session, String field, IntList sessionMetricIndexes) {
            final FTGSIterator it = closer.register(session.getFTGSIterator(new String[]{field}, new String[0]));
            final int numStats = session.getNumStats();
            final long[] statsBuff = new long[numStats];
            if (!it.nextField()) {
                return Optional.empty();
            }
            if (!it.nextTerm()) {
                return Optional.empty();
            }
            if (!it.nextGroup()) {
                return Optional.empty();
            }
            it.groupStats(statsBuff);
            return Optional.of(new SessionIntIterationState(it, sessionMetricIndexes, statsBuff, it.termIntVal(), it.group()));
        }
    }

    public interface IntIterateCallback {
        void term(long term, long[] stats, int group);
    }

    public static void iterateMultiInt(Map<String, ImhotepSession> sessions, Map<String, IntList> metricIndexes, String field, IntIterateCallback callback) throws IOException {
        final int numMetrics = metricIndexes.values().stream().mapToInt(IntList::size).sum();
        try (final Closer closer = Closer.create()) {
            final PriorityQueue<SessionIntIterationState> pq = new PriorityQueue<>((Comparator<SessionIntIterationState>)(x, y) -> {
                int r = Longs.compare(x.nextTerm, y.nextTerm);
                if (r != 0) return r;
                return Ints.compare(x.nextGroup, y.nextGroup);
            });
            for (final String sessionName : sessions.keySet()) {
                final ImhotepSession session = sessions.get(sessionName);
                final IntList sessionMetricIndexes = Objects.firstNonNull(metricIndexes.get(sessionName), new IntArrayList());
                SessionIntIterationState.construct(closer, session, field, sessionMetricIndexes).ifPresent(pq::add);
            }
            final long[] realBuffer = new long[numMetrics];
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
        } else if (iterator.nextTerm() && iterator.nextGroup()) {
            state.nextTerm = iterator.termIntVal();
            state.nextGroup = iterator.group();
            iterator.groupStats(state.statsBuff);
            pq.add(state);
        }
    }

    private static void copyStats(SessionIntIterationState state, long[] dst) {
        for (int i = 0; i < state.metricIndexes.size(); i++) {
            dst[state.metricIndexes.getInt(i)] = state.statsBuff[i];
        }
    }

    private static class SessionStringIterationState {
        public final FTGSIterator iterator;
        private final IntList metricIndexes;
        public final long[] statsBuff;
        public String nextTerm;
        public int nextGroup;

        private SessionStringIterationState(FTGSIterator iterator, IntList metricIndexes, long[] statsBuff, String nextTerm, int nextGroup) {
            this.iterator = iterator;
            this.metricIndexes = metricIndexes;
            this.statsBuff = statsBuff;
            this.nextTerm = nextTerm;
            this.nextGroup = nextGroup;
        }

        static Optional<SessionStringIterationState> construct(Closer closer, ImhotepSession session, String field, IntList sessionMetricIndexes) {
            final FTGSIterator it = closer.register(session.getFTGSIterator(new String[0], new String[]{field}));
            final int numStats = session.getNumStats();
            final long[] statsBuff = new long[numStats];
            if (!it.nextField()) {
                return Optional.empty();
            }
            if (!it.nextTerm()) {
                return Optional.empty();
            }
            if (!it.nextGroup()) {
                return Optional.empty();
            }
            it.groupStats(statsBuff);
            return Optional.of(new SessionStringIterationState(it, sessionMetricIndexes, statsBuff, it.termStringVal(), it.group()));
        }
    }

    public interface StringIterateCallback {
        void term(String term, long[] stats, int group);
    }

    public static void iterateMultiString(Map<String, ImhotepSession> sessions, Map<String, IntList> metricIndexes, String field, StringIterateCallback callback) throws IOException {
        final int numMetrics = metricIndexes.values().stream().mapToInt(IntList::size).sum();
        try (final Closer closer = Closer.create()) {
            final PriorityQueue<SessionStringIterationState> pq = new PriorityQueue<>((Comparator<SessionStringIterationState>)(x, y) -> {
                int r = x.nextTerm.compareTo(y.nextTerm);
                if (r != 0) return r;
                return Ints.compare(x.nextGroup, y.nextGroup);
            });
            for (final String sessionName : sessions.keySet()) {
                final ImhotepSession session = sessions.get(sessionName);
                final IntList sessionMetricIndexes = Objects.firstNonNull(metricIndexes.get(sessionName), new IntArrayList());
                SessionStringIterationState.construct(closer, session, field, sessionMetricIndexes).ifPresent(pq::add);
            }
            final long[] realBuffer = new long[numMetrics];
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

    private static void advanceAndEnqueue(SessionStringIterationState state, PriorityQueue<SessionStringIterationState> pq) {
        final FTGSIterator iterator = state.iterator;
        if (iterator.nextGroup()) {
            state.nextGroup = iterator.group();
            iterator.groupStats(state.statsBuff);
            pq.add(state);
        } else if (iterator.nextTerm() && iterator.nextGroup()) {
            state.nextTerm = iterator.termStringVal();
            state.nextGroup = iterator.group();
            iterator.groupStats(state.statsBuff);
            pq.add(state);
        }
    }

    private static void copyStats(SessionStringIterationState state, long[] dst) {
        for (int i = 0; i < state.metricIndexes.size(); i++) {
            dst[state.metricIndexes.getInt(i)] = state.statsBuff[i];
        }
    }

    public static class GroupStats {
        public final GroupKey key;
        public final double[] stats;

        @JsonCreator
        public GroupStats(@JsonProperty("key") GroupKey key, @JsonProperty("stats") double[] stats) {
            this.key = key;
            this.stats = stats;
        }
    }

    public static class GroupKey {
        public final String term;
        public final int index;
        public final GroupKey parent;

        public GroupKey(String term, int index, GroupKey parent) {
            this.term = term;
            this.index = index;
            this.parent = parent;
        }

        public List<String> asList() {
            final List<String> keys = Lists.newArrayList();
            GroupKey node = this;
            while (node != null && node.term != null) {
                keys.add(node.term);
                node = node.parent;
            }
            return Lists.reverse(keys);
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

    public static class ImhotepSessionInfo {
        public final ImhotepSession session;
        public final Collection<String> intFields;
        public final Collection<String> stringFields;
        public final DateTime startTime;
        public final DateTime endTime;
        public final String timeFieldName;

        private ImhotepSessionInfo(ImhotepSession session, Collection<String> intFields, Collection<String> stringFields, DateTime startTime, DateTime endTime, String timeFieldName) {
            this.session = session;
            this.intFields = Collections.unmodifiableCollection(intFields);
            this.stringFields = Collections.unmodifiableCollection(stringFields);
            this.startTime = startTime;
            this.endTime = endTime;
            this.timeFieldName = timeFieldName;
        }
    }
}
