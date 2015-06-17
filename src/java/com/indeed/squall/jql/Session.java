package com.indeed.squall.jql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.math.DoubleMath;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.common.util.Pair;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ImhotepClient;
import com.indeed.imhotep.client.ShardIdWithVersion;
import com.indeed.squall.jql.commands.ApplyFilterActions;
import com.indeed.squall.jql.commands.ComputeAndCreateGroupStatsLookup;
import com.indeed.squall.jql.commands.ComputeAndCreateGroupStatsLookups;
import com.indeed.squall.jql.commands.CreateGroupStatsLookup;
import com.indeed.squall.jql.commands.ExplodeByAggregatePercentile;
import com.indeed.squall.jql.commands.ExplodeDayOfWeek;
import com.indeed.squall.jql.commands.ExplodeGroups;
import com.indeed.squall.jql.commands.ExplodeMonthOfYear;
import com.indeed.squall.jql.commands.ExplodePerDocPercentile;
import com.indeed.squall.jql.commands.ExplodePerGroup;
import com.indeed.squall.jql.commands.ExplodeSessionNames;
import com.indeed.squall.jql.commands.ExplodeTimeBuckets;
import com.indeed.squall.jql.commands.FilterDocs;
import com.indeed.squall.jql.commands.GetGroupDistincts;
import com.indeed.squall.jql.commands.GetGroupPercentiles;
import com.indeed.squall.jql.commands.GetGroupStats;
import com.indeed.squall.jql.commands.GetNumGroups;
import com.indeed.squall.jql.commands.Iterate;
import com.indeed.squall.jql.commands.IterateAndExplode;
import com.indeed.squall.jql.commands.MetricRegroup;
import com.indeed.squall.jql.commands.RegroupIntoLastSiblingWhere;
import com.indeed.squall.jql.commands.RegroupIntoParent;
import com.indeed.squall.jql.commands.SampleFields;
import com.indeed.squall.jql.commands.SimpleIterate;
import com.indeed.squall.jql.commands.SumAcross;
import com.indeed.squall.jql.commands.TimePeriodRegroup;
import com.indeed.squall.jql.commands.TimeRegroup;
import com.indeed.squall.jql.dimensions.DatasetDimensions;
import com.indeed.squall.jql.dimensions.DimensionsLoader;
import com.indeed.squall.jql.dimensions.DimensionsTranslator;
import com.indeed.squall.jql.metrics.aggregate.AggregateMetric;
import com.indeed.squall.jql.metrics.aggregate.PerGroupConstant;
import com.indeed.util.core.TreeTimer;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleCollection;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
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

    public List<GroupKey> groupKeys = Lists.newArrayList(null, new GroupKey(null, 1, null));
    public final Map<String, SavedGroupStats> savedGroupStats = Maps.newHashMap();
    public int currentDepth = 0;

    public final Map<String, ImhotepSessionInfo> sessions;
    public final TreeTimer timer;
    public int numGroups = 1;

    public static final ObjectMapper MAPPER = new ObjectMapper();
    static {
        final SimpleModule module = new SimpleModule();
        module.addSerializer(TermSelects.class, new JsonSerializer<TermSelects>() {
            @Override
            public void serialize(TermSelects value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
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
            public void serialize(GroupKey value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
                jgen.writeObject(value.asList(false));
            }
        });
        MAPPER.registerModule(module);
        MAPPER.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    }

    public static final String INFINITY_SYMBOL = "∞";

    public Session(Map<String, ImhotepSessionInfo> sessions, TreeTimer timer) {
        this.sessions = sessions;
        this.timer = timer;
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

        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        final DimensionsLoader dimensionsLoader = new DimensionsLoader("dataset-dimensions", new File("/var/lucene/__/ramses-meta"));
        executor.scheduleAtFixedRate(dimensionsLoader, 0, 5, TimeUnit.MINUTES);

        try (WebSocketSessionServer wsServer = new WebSocketSessionServer(client, new InetSocketAddress(wsSocketPort), dimensionsLoader)) {
            new Thread(wsServer::run).start();

            final ServerSocket serverSocket = new ServerSocket(unixSocketPort);
            while (true) {
                final Socket clientSocket = serverSocket.accept();
                final TreeTimer treeTimer = new TreeTimer();
                treeTimer.push("request");
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

                        System.out.println("Found connection");
                        final Consumer<String> resultConsumer = out::println;
                        treeTimer.push("processConnection");
                        processConnection(client, nodeSupplier, resultConsumer, dimensionsLoader.getDimensions(), treeTimer);
                        treeTimer.pop();
                    } catch (Throwable e) {
                        log.error("wat", e);
                        System.out.println("e = " + e);
                    } finally {
                        treeTimer.pop();
                        System.out.println(treeTimer.toString());
                    }
                }).start();
            }
        }
    }

    public static void processConnection(ImhotepClient client, Supplier<JsonNode> in, Consumer<String> out, Map<String, DatasetDimensions> dimensions, TreeTimer treeTimer) throws IOException, ImhotepOutOfMemoryException {
        treeTimer.push("firstLine");
        final JsonNode sessionRequest = in.get();
        treeTimer.pop();
        if (sessionRequest.has("describe")) {
            processDescribe(client, out, sessionRequest);
        } else {
            try (final Closer closer = Closer.create()) {
                treeTimer.push("createSession");
                final Optional<Session> session = createSession(client, sessionRequest, closer, out, dimensions, treeTimer);
                treeTimer.pop();
                treeTimer.push("commands");
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
                treeTimer.pop();
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

    public static Optional<Session> createSession(ImhotepClient client, JsonNode sessionRequest, Closer closer, Consumer<String> out, Map<String, DatasetDimensions> dimensions, TreeTimer treeTimer) throws ImhotepOutOfMemoryException, IOException {
        final Map<String, ImhotepSessionInfo> sessions = Maps.newHashMap();
        if (sessionRequest.has("commands")) {
            treeTimer.push("createSubSessions");
            createSubSessions(client, sessionRequest.get("datasets"), closer, sessions, dimensions, treeTimer);
            treeTimer.pop();
            final Session session = new Session(sessions, treeTimer);
            treeTimer.push("readCommands");
            final JsonNode commands = sessionRequest.get("commands");
            treeTimer.pop();
            for (int i = 0; i < commands.size(); i++) {
                final JsonNode command = commands.get(i);
                System.out.println("Evaluating command: " + command);
                final boolean isLast = i == commands.size() - 1;
                if (isLast) {
                    session.evaluateCommandToTSV(command, out);
                } else {
                    session.evaluateCommand(command, x -> {});
                }
            }
            return Optional.empty();
        } else {
            createSubSessions(client, sessionRequest, closer, sessions, dimensions, treeTimer);
            out.accept("opened");
            return Optional.of(new Session(sessions, treeTimer));
        }
    }

    private static DatasetInfo getDatasetShardList(ImhotepClient client, String dataset) {
        final Map<Host, List<DatasetInfo>> shardListMap = client.getShardList();
        final DatasetInfo ret = new DatasetInfo(dataset, new HashSet<>(), new HashSet<>(), new HashSet<>(), new HashSet<>());
        for (final List<DatasetInfo> datasetList : shardListMap.values()) {
            for (final DatasetInfo d : datasetList) {
                if (d.getDataset().equals(dataset)) {
                    ret.getShardList().addAll(d.getShardList());
                    ret.getIntFields().addAll(d.getIntFields());
                    ret.getStringFields().addAll(d.getStringFields());
                    ret.getMetrics().addAll(d.getMetrics());
                }
            }
        }
        return ret;
    }

    private static void createSubSessions(ImhotepClient client, JsonNode sessionRequest, Closer closer, Map<String, ImhotepSessionInfo> sessions, Map<String, DatasetDimensions> dimensions, TreeTimer treeTimer) throws ImhotepOutOfMemoryException {
        for (int i = 0; i < sessionRequest.size(); i++) {
            final JsonNode elem = sessionRequest.get(i);
            final String dataset = elem.get("dataset").textValue();
            final String start = elem.get("start").textValue();
            final String end = elem.get("end").textValue();
            final String name = elem.has("name") ? elem.get("name").textValue() : dataset;

            treeTimer.push("get dataset info");
            treeTimer.push("getDatasetShardList");
            final DatasetInfo datasetInfo = getDatasetShardList(client, dataset);
            treeTimer.pop();
            final Collection<String> sessionIntFields = datasetInfo.getIntFields();
            final Collection<String> sessionStringFields = datasetInfo.getStringFields();
            final DateTime startDateTime = parseDateTime(start);
            final DateTime endDateTime = parseDateTime(end);
            treeTimer.pop();
            treeTimer.push("build session");
            treeTimer.push("create session builder");
            final ImhotepClient.SessionBuilder sessionBuilder = client.sessionBuilder(dataset, startDateTime, endDateTime);
            treeTimer.pop();
            treeTimer.push("get shards");
            final List<ShardIdWithVersion> shards = sessionBuilder.getChosenShards();
            treeTimer.pop();
            treeTimer.push("build session builder");
            final ImhotepSession build = sessionBuilder.build();
            treeTimer.pop();
            final ImhotepSession session = closer.register(new DimensionsTranslator(build, dimensions.get(dataset)));
            treeTimer.pop();

            treeTimer.push("determine time range");
            final DateTime earliestStart = Ordering.natural().min(Iterables.transform(shards, ShardIdWithVersion::getStart));
            final DateTime latestEnd = Ordering.natural().max(Iterables.transform(shards, ShardIdWithVersion::getEnd));
            treeTimer.pop();
            final boolean isRamsesIndex = datasetInfo.getIntFields().isEmpty();
            final String timeField = isRamsesIndex ? "time" : "unixtime";
            if (earliestStart.isBefore(startDateTime) || latestEnd.isAfter(endDateTime)) {
                treeTimer.push("regroup time range");
                session.regroup(new QueryRemapRule(1, Query.newRangeQuery(timeField, startDateTime.getMillis() / 1000, endDateTime.getMillis() / 1000, false), 0, 1));
                treeTimer.pop();
            }
            sessions.put(name, new ImhotepSessionInfo(session, sessionIntFields, sessionStringFields, startDateTime, endDateTime, timeField));
        }
    }

    private static final Pattern relativePattern = Pattern.compile("(\\d+)([smhdwMy])");
    private static DateTime parseDateTime(String descriptor) {
        descriptor = descriptor.trim();
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
        timer.push("evaluateCommand " + commandTree);
        try {
            final Object command = Commands.parseCommand(commandTree, this::namedMetricLookup);
            evaluateCommandInternal(commandTree, out, command);
        } finally {
            timer.pop();
        }
    }

    public void evaluateCommandToTSV(JsonNode commandTree, Consumer<String> out) throws ImhotepOutOfMemoryException, IOException {
        timer.push("evaluateCommandToTSV " + commandTree);
        try {

            final Object command = Commands.parseCommand(commandTree, this::namedMetricLookup);
            if (command instanceof Iterate) {
                final List<List<List<TermSelects>>> results = ((Iterate) command).execute(this);
                final StringBuilder sb = new StringBuilder();
                writeTermSelectsJson(results, sb);
                out.accept(MAPPER.writeValueAsString(Collections.singletonList(sb.toString())));
            } else if (command instanceof SimpleIterate) {
                final SimpleIterate simpleIterate = (SimpleIterate) command;
                final List<List<List<TermSelects>>> result = simpleIterate.execute(this, out);
                //noinspection StatementWithEmptyBody
                if (simpleIterate.streamResult) {
                    // result already sent
                } else {
                    for (final List<List<TermSelects>> groupFieldTerms : result) {
                        final List<TermSelects> groupTerms = groupFieldTerms.get(0);
                        for (final TermSelects termSelect : groupTerms) {
                            if (termSelect.isIntTerm) {
                                out.accept(SimpleIterate.createRow(termSelect.groupKey, termSelect.intTerm, termSelect.selects));
                            } else {
                                out.accept(SimpleIterate.createRow(termSelect.groupKey, termSelect.stringTerm, termSelect.selects));
                            }
                        }
                    }
                    out.accept("");
                }
            } else if (command instanceof GetGroupStats) {
                final GetGroupStats getGroupStats = (GetGroupStats) command;
                final List<GroupStats> results = getGroupStats.execute(this);
                final StringBuilder sb = new StringBuilder();
                for (final GroupStats result : results) {
                    final List<String> keyColumns = result.key.asList(false);
                    keyColumns.forEach(k -> sb.append(k).append('\t'));
                    for (final double stat : result.stats) {
                        if (DoubleMath.isMathematicalInteger(stat)) {
                            sb.append((long) stat).append('\t');
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
        } finally {
            timer.pop();
        }
    }

    private void writeTermSelectsJson(List<List<List<TermSelects>>> results, StringBuilder sb) {
        for (final List<List<TermSelects>> groupFieldTerms : results) {
            final List<TermSelects> groupTerms = groupFieldTerms.get(0);
            for (final TermSelects termSelects : groupTerms) {
                final List<String> keyColumns = termSelects.groupKey.asList(true);
                keyColumns.forEach(k -> sb.append(k).append('\t'));
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

    public void evaluateCommandInternal(JsonNode commandTree, Consumer<String> out, Object command) throws ImhotepOutOfMemoryException, IOException {
        if (command instanceof Iterate) {
            final Iterate iterate = (Iterate) command;
            final List<List<List<TermSelects>>> allTermSelects = iterate.execute(this);
            out.accept(MAPPER.writeValueAsString(allTermSelects));
        } else if (command instanceof SimpleIterate) {
            final SimpleIterate simpleIterate = (SimpleIterate) command;
            if (simpleIterate.streamResult) {
                throw new IllegalArgumentException("Cannot stream SimpleIterate result except in evaluateToTSV!");
            } else {
                final List<List<List<TermSelects>>> result = simpleIterate.execute(this, null);
                final StringBuilder sb = new StringBuilder();
                writeTermSelectsJson(result, sb);
                out.accept(MAPPER.writeValueAsString(Collections.singletonList(sb.toString())));
            }
        } else if (command instanceof FilterDocs) {
            final FilterDocs filterDocs = (FilterDocs) command;
            filterDocs.execute(this);
            out.accept("{}");
        } else if (command instanceof ExplodeGroups) {
            final ExplodeGroups explodeGroups = (ExplodeGroups) command;
            explodeGroups.execute(this);
            out.accept("success");
        } else if (command instanceof MetricRegroup) {
            final MetricRegroup metricRegroup = (MetricRegroup) command;
            metricRegroup.execute(this);
            out.accept("success");
        } else if (command instanceof TimeRegroup) {
            final TimeRegroup timeRegroup = (TimeRegroup) command;
            timeRegroup.execute(this);
            out.accept("success");
        } else if (command instanceof GetGroupStats) {
            final GetGroupStats getGroupStats = (GetGroupStats) command;
            final List<GroupStats> results = getGroupStats.execute(this);
            out.accept(MAPPER.writeValueAsString(results));
        } else if (command instanceof CreateGroupStatsLookup) {
            final CreateGroupStatsLookup createGroupStatsLookup = (CreateGroupStatsLookup) command;
            final String lookupName = createGroupStatsLookup.execute(this);
            out.accept(MAPPER.writeValueAsString(Arrays.asList(lookupName)));
        } else if (command instanceof GetGroupDistincts) {
            final GetGroupDistincts getGroupDistincts = (GetGroupDistincts) command;
            final long[] groupCounts = getGroupDistincts.execute(this);
            out.accept(MAPPER.writeValueAsString(groupCounts));
        } else if (command instanceof GetGroupPercentiles) {
            final GetGroupPercentiles getGroupPercentiles = (GetGroupPercentiles) command;
            final long[][] results = getGroupPercentiles.execute(this);
            out.accept(MAPPER.writeValueAsString(results));
        } else if (command instanceof GetNumGroups) {
            out.accept(MAPPER.writeValueAsString(Collections.singletonList(numGroups)));
        } else if (command instanceof ExplodePerGroup) {
            ((ExplodePerGroup) command).execute(this);
            out.accept("success");
        } else if (command instanceof ExplodeDayOfWeek) {
            ((ExplodeDayOfWeek) command).execute(this);
            out.accept("success");
        } else if (command instanceof ExplodeSessionNames) {
            final TreeSet<String> names = Sets.newTreeSet(sessions.keySet());
            // TODO: This
            throw new UnsupportedOperationException("Get around to implementing ExplodeSessionNames");
        } else if (command instanceof IterateAndExplode) {
            final IterateAndExplode iterateAndExplode = (IterateAndExplode) command;
            iterateAndExplode.execute(this);
        } else if (command instanceof ComputeAndCreateGroupStatsLookup) {
            final ComputeAndCreateGroupStatsLookup computeAndCreateGroupStatsLookup = (ComputeAndCreateGroupStatsLookup) command;
            computeAndCreateGroupStatsLookup.execute(this, out);
        } else if (command instanceof ComputeAndCreateGroupStatsLookups) {
            final ComputeAndCreateGroupStatsLookups computeAndCreateGroupStatsLookups = (ComputeAndCreateGroupStatsLookups) command;
            computeAndCreateGroupStatsLookups.execute(this);
        } else if (command instanceof ExplodeByAggregatePercentile) {
            final ExplodeByAggregatePercentile explodeCommand = (ExplodeByAggregatePercentile) command;
            explodeCommand.execute(this);
            out.accept("ExplodedByAggregatePercentile");
        } else if (command instanceof ExplodePerDocPercentile) {
            final ExplodePerDocPercentile explodeCommand = (ExplodePerDocPercentile) command;
            explodeCommand.execute(this);
            out.accept("ExplodedPerDocPercentile");
        } else if (command instanceof SumAcross) {
            final SumAcross sumAcross = (SumAcross) command;
            final double[] results = sumAcross.execute(this);
            out.accept(MAPPER.writeValueAsString(results));
        } else if (command instanceof RegroupIntoParent) {
            final RegroupIntoParent regroupIntoParent = (RegroupIntoParent) command;
            regroupIntoParent.execute(this);
            out.accept("RegroupedIntoParent");
        } else if (command instanceof RegroupIntoLastSiblingWhere) {
            final RegroupIntoLastSiblingWhere regroupIntoLastSiblingWhere = (RegroupIntoLastSiblingWhere) command;
            final boolean[] merged = regroupIntoLastSiblingWhere.execute(this);
            out.accept(MAPPER.writeValueAsString(merged));
        } else if (command instanceof ExplodeMonthOfYear) {
            ((ExplodeMonthOfYear) command).execute(this);
            out.accept("ExplodedMonthOfYear");
        } else if (command instanceof TimePeriodRegroup) {
            ((TimePeriodRegroup) command).execute(this);
            out.accept("TimePeriodRegrouped");
        } else if (command instanceof ExplodeTimeBuckets) {
            ((ExplodeTimeBuckets) command).execute(this);
            out.accept("ExplodedTimeBuckets");
        } else if (command instanceof SampleFields) {
            ((SampleFields) command).execute(this);
            out.accept("SampledFields");
        } else if (command instanceof ApplyFilterActions) {
            ((ApplyFilterActions) command).execute(this);
            out.accept("Applied filters");
        } else {
            throw new IllegalArgumentException("Invalid command: " + commandTree);
        }
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
        list.sort(Double::compare);
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
        return sessions.values().stream().anyMatch(x -> x.intFields.contains(field));
    }

    public boolean isStringField(String field) {
        return !isIntField(field) && sessions.values().stream().anyMatch(x -> x.stringFields.contains(field));
    }

    private PerGroupConstant namedMetricLookup(String name) {
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

        public List<String> asList(boolean appendingTerm) {
            if (term == null && !appendingTerm) {
                return Collections.singletonList("");
            } else {
                final List<String> keys = Lists.newArrayList();
                GroupKey node = this;
                while (node != null && node.term != null) {
                    keys.add(node.term);
                    node = node.parent;
                }
                return Lists.reverse(keys);
            }
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
