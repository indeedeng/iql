import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.common.datastruct.BoundedPriorityQueue;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.ImhotepClient;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.LongStream;

/**
 * @author jwolfe
 */
public class Session {
    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }
    private static final Logger log = Logger.getLogger(Session.class);

    public List<String> fields = Lists.newArrayList();
    public List<GroupKey> groupKeys = Lists.newArrayList(null, new GroupKey(null, 1, null));
    public final Map<String, SavedGroupStats> savedGroupStats = Maps.newHashMap();
    public int currentDepth = 0;

    private final Map<String, ImhotepSession> sessions;
    private final Map<String, Collection<String>> intFields;
    private int numGroups = 1;

    private static final ObjectMapper mapper = new ObjectMapper();
    static {
        final SimpleModule module = new SimpleModule();
        module.addSerializer(TermSelects.class, new JsonSerializer<TermSelects>() {
            @Override
            public void serialize(TermSelects value, JsonGenerator jgen, SerializerProvider provider) throws IOException, JsonProcessingException {
                jgen.writeStartObject();
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
        mapper.registerModule(module);
        mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    }

    public Session(Map<String, ImhotepSession> sessions, Map<String, Collection<String>> intFields) {
        this.sessions = sessions;
        this.intFields = intFields;
    }

    public static void main(String[] args) throws IOException, ImhotepOutOfMemoryException {
        org.apache.log4j.BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

//        String[] commands = ("{\"command\":\"filterDocs\",\"filter\":{\"arg1\":{\"value\":\"x\",\"type\":\"atom\"},\"type\":\"metricEquals\",\"arg2\":{\"value\":\"y\",\"type\":\"atom\"}}}\n" +
//                "{\"command\":\"iterate\",\"field\":\"country\",\"opts\":[]}\n" +
//                "{\"command\":\"iterate\",\"field\":\"qnorm\",\"opts\":[{\"metrics\":[{\"m2\":{\"value\":[\"oji\"],\"type\":\"atom\"},\"m1\":{\"value\":[\"ojc\"],\"type\":\"atom\"},\"type\":\"division\"},{\"m2\":{\"value\":[\"sji\"],\"type\":\"atom\"},\"m1\":{\"value\":[\"sjc\"],\"type\":\"atom\"},\"type\":\"division\"}],\"type\":\"selecting\"},{\"filter\":{\"field\":\"qnorm\",\"value\":\"part time .*\",\"type\":\"regex\"},\"type\":\"filter\"}]}").split("\n");
//        String[] commands = {"{\"command\":\"iterate\",\"field\":\"qnorm\",\"opts\":[{\"metrics\":[{\"value\":[\"ojc\"],\"type\":\"atom\"},{\"value\":[\"oji\"],\"type\":\"atom\"},{\"m2\":{\"value\":[\"oji\"],\"type\":\"atom\"},\"m1\":{\"value\":[\"ojc\"],\"type\":\"atom\"},\"type\":\"division\"},{\"value\":[\"sjc\"],\"type\":\"atom\"},{\"value\":[\"sji\"],\"type\":\"atom\"},{\"m2\":{\"value\":[\"sji\"],\"type\":\"atom\"},\"m1\":{\"value\":[\"sjc\"],\"type\":\"atom\"},\"type\":\"division\"}],\"type\":\"selecting\"},{\"filter\":{\"field\":\"qnorm\",\"value\":\"part time .*\",\"type\":\"regex\"},\"type\":\"filter\"}]}"};
//        String[] commands = {"{\"command\":\"iterate\",\"field\":\"qnorm\",\"opts\":[{\"metrics\":[{\"value\":[\"ojc\"],\"type\":\"atom\"},{\"value\":[\"oji\"],\"type\":\"atom\"},{\"m2\":{\"value\":[\"oji\"],\"type\":\"atom\"},\"m1\":{\"value\":[\"ojc\"],\"type\":\"atom\"},\"type\":\"division\"}],\"type\":\"selecting\"},{\"filter\":{\"arg1\":{\"arg1\":{\"value\":[\"oji\"],\"type\":\"atom\"},\"type\":\"greaterThan\",\"arg2\":{\"value\":100000,\"type\":\"constant\"}},\"type\":\"and\",\"arg2\":{\"arg1\":{\"m2\":{\"value\":[\"oji\"],\"type\":\"atom\"},\"m1\":{\"value\":[\"ojc\"],\"type\":\"atom\"},\"type\":\"division\"},\"type\":\"greaterThan\",\"arg2\":{\"value\":4.0e-2,\"type\":\"constant\"}}},\"type\":\"filter\"}]}"};
        String[] commands = {"{\"command\":\"filterDocs\",\"filter\":{\"field\":\"country\",\"value\":{\"value\":\"us\",\"type\":\"string\"},\"type\":\"fieldEquals\"}}",
                "{\"command\":\"iterate\",\"field\":\"qnorm\",\"opts\":[{\"metrics\":[{\"value\":[\"ojc\"],\"type\":\"atom\"},{\"value\":[\"oji\"],\"type\":\"atom\"},{\"m2\":{\"value\":[\"oji\"],\"type\":\"atom\"},\"m1\":{\"value\":[\"ojc\"],\"type\":\"atom\"},\"type\":\"division\"}],\"type\":\"selecting\"},{\"filter\":{\"arg1\":{\"arg1\":{\"value\":[\"oji\"],\"type\":\"atom\"},\"type\":\"greaterThan\",\"arg2\":{\"value\":100000,\"type\":\"constant\"}},\"type\":\"and\",\"arg2\":{\"arg1\":{\"m2\":{\"value\":[\"oji\"],\"type\":\"atom\"},\"m1\":{\"value\":[\"ojc\"],\"type\":\"atom\"},\"type\":\"division\"},\"type\":\"greaterThan\",\"arg2\":{\"value\":4.0e-2,\"type\":\"constant\"}}},\"type\":\"filter\"}]}"};
        final ImhotepClient client = new ImhotepClient("/home/jwolfe/hosts.txt");
//        final List<String> commands2 = Arrays.asList(
//                "{\"command\":\"iterate\",\"field\":\"country\",\"opts\":[]}",
//                "{\"command\":\"explodeGroups\",\"field\":\"country\",\"strings\":[[\"it\",\"nl\",\"ru\",\"br\",\"de\",\"ca\",\"fr\",\"jp\",\"gb\",\"us\"]]}"
//        );
//        try (final ImhotepSession session = client.sessionBuilder("organic", DateTime.parse("2014-07-01T00:00:00"), DateTime.parse("2014-07-02T00:00:00")).build()) {
//            Session session1 = new Session(session);
//            for (final String command : commands2) {
//                System.out.println("command = " + command);
//                session1.evaluateCommand(command, new PrintWriter(System.err));
//            }
//        }

        final ServerSocket serverSocket = new ServerSocket(28347);
        while (true) {
            final Socket clientSocket = serverSocket.accept();
            new Thread(() -> {
                try (final PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                     final BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
                    final JsonNode sessionRequest = mapper.readTree(in.readLine());
                    final Map<String, ImhotepSession> sessions = Maps.newHashMap();
                    final Map<String, Collection<String>> intFields = Maps.newHashMap();
                    try (final Closer closer = Closer.create()) {
                        for (int i = 0; i < sessionRequest.size(); i++) {
                            final JsonNode elem = sessionRequest.get(i);
                            final String dataset = elem.get("dataset").asText();
                            final String start = elem.get("start").asText();
                            final String end = elem.get("end").asText();
                            final String name = elem.has("name") ? elem.get("name").asText() : dataset;

                            intFields.put(name, client.getDatasetToShardList().get(dataset).getIntFields());
                            final ImhotepSession session = closer.register(client.sessionBuilder(dataset, parseDateTime(start), parseDateTime(end)).build());
                            sessions.put(name, session);
                        }
                        final Session session1 = new Session(sessions, intFields);
                        out.println("opened");
                        String inputLine;
                        while ((inputLine = in.readLine()) != null) {
                            System.out.println("inputLine = " + inputLine);
                            session1.evaluateCommand(inputLine, out);
                        }
                    }
                } catch (Throwable e) {
                    log.error("wat", e);
                }
            }).start();
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

    public void evaluateCommand(String commandString, PrintWriter out) throws ImhotepOutOfMemoryException, IOException {
        final Object command = Commands.parseCommand(mapper.readTree(commandString), this::namedMetricLookup);
        if (command instanceof Commands.Iterate) {
            final Commands.Iterate iterate = (Commands.Iterate) command;
            final Set<QualifiedPush> allPushes = Sets.newHashSet();
            final List<AggregateMetric> metrics = Lists.newArrayList();
            iterate.topK.ifPresent(topK -> metrics.add(topK.metric));
            metrics.addAll(iterate.selecting);
            for (final AggregateMetric metric : metrics) {
                allPushes.addAll(metric.requires());
            }
            iterate.filter.ifPresent(filter -> allPushes.addAll(filter.requires()));
            final Map<QualifiedPush, Integer> metricIndexes = Maps.newHashMap();
            int numStats = 0;
            final Map<String, IntList> sessionMetricIndexes = Maps.newHashMap();
            for (final QualifiedPush push : allPushes) {
                final int index = numStats++;
                metricIndexes.put(push, index);
                final String sessionName = push.sessionName;
                sessions.get(sessionName).pushStats(push.pushes);
                sessionMetricIndexes.computeIfAbsent(sessionName, k -> new IntArrayList()).add(index);
            }
            for (final AggregateMetric metric : metrics) {
                metric.register(metricIndexes);
            }
            iterate.filter.ifPresent(filter -> filter.register(metricIndexes));
            Int2ObjectArrayMap<Queue<TermSelects>> pqs = new Int2ObjectArrayMap<>();
            if (iterate.topK.isPresent()) {
                final Comparator<TermSelects> comparator = Comparator.comparing(x -> Double.isNaN(x.topMetric) ? Double.NEGATIVE_INFINITY : x.topMetric);
                for (int i = 1; i <= numGroups; i++) {
                    pqs.put(i, BoundedPriorityQueue.newInstance(iterate.topK.get().limit, comparator));
                }
            } else {
                for (int i = 1; i <= numGroups; i++) {
                    pqs.put(i, new ArrayDeque<>());
                }
            }
            final AggregateMetric topKMetricOrNull;
            if (iterate.topK.isPresent()) {
                topKMetricOrNull = iterate.topK.get().metric;
            } else {
                topKMetricOrNull = null;
            }
            final AggregateFilter filterOrNull = iterate.filter.orElse(null);
            if (isIntField(iterate.field)) {
                iterateMultiInt(sessions, sessionMetricIndexes, iterate.field, new IntIterateCallback() {
                    @Override
                    public void term(long term, long[] stats, int group) {
                        if (filterOrNull != null && !filterOrNull.allow(term, stats, group)) {
                            return;
                        }
                        final double[] selectBuffer = new double[iterate.selecting.size()];
                        final double value;
                        if (topKMetricOrNull != null) {
                            value = topKMetricOrNull.apply(term, stats, group);
                        } else {
                            value = 0.0;
                        }
                        final List<AggregateMetric> selecting = iterate.selecting;
                        for (int i = 0; i < selecting.size(); i++) {
                            selectBuffer[i] = selecting.get(i).apply(term, stats, group);
                        }
                        pqs.get(group).offer(new TermSelects(true, null, term, selectBuffer, value, groupKeys.get(group)));
                    }
                });
            } else {
                iterateMultiString(sessions, sessionMetricIndexes, iterate.field, new StringIterateCallback() {
                    @Override
                    public void term(String term, long[] stats, int group) {
                        if (filterOrNull != null && !filterOrNull.allow(term, stats, group)) {
                            return;
                        }
                        final double[] selectBuffer = new double[iterate.selecting.size()];
                        final double value;
                        if (topKMetricOrNull != null) {
                            value = topKMetricOrNull.apply(term, stats, group);
                        } else {
                            value = 0.0;
                        }
                        final List<AggregateMetric> selecting = iterate.selecting;
                        for (int i = 0; i < selecting.size(); i++) {
                            selectBuffer[i] = selecting.get(i).apply(term, stats, group);
                        }
                        pqs.get(group).offer(new TermSelects(false, term, 0, selectBuffer, value, groupKeys.get(group)));
                    }
                });
            }
            final List<List<TermSelects>> allTermSelects = Lists.newArrayList();
            for (int group = 1; group <= numGroups; group++) {
                final Queue<TermSelects> pq = pqs.get(group);
                final List<TermSelects> listTermSelects = Lists.newArrayList();
                while (!pq.isEmpty()) {
                    listTermSelects.add(pq.poll());
                }
                allTermSelects.add(listTermSelects);
            }
            mapper.writeValue(out, allTermSelects);
            out.println();
            sessions.values().forEach(session -> {
                while (session.getNumStats() != 0) {
                    session.popStat();
                }
            });
        } else if (command instanceof Commands.FilterDocs) {
            final Commands.FilterDocs filterDocs = (Commands.FilterDocs) command;
            final int numGroupsTmp = numGroups;
            // TODO: Pass in the index name so that filters can be index=? filters.
            sessions.values().forEach(session -> unchecked(() -> filterDocs.docFilter.apply(session, numGroupsTmp)));
            out.println("{}");
        } else if (command instanceof Commands.ExplodeGroups) {
            final Commands.ExplodeGroups explodeGroups = (Commands.ExplodeGroups) command;
            if ((explodeGroups.intTerms == null) == (explodeGroups.stringTerms == null)) {
                throw new IllegalArgumentException("Exactly one type of term must be contained in ExplodeGroups.");
            }
            final boolean intType = explodeGroups.intTerms != null;
            final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[numGroups];
            int nextGroup = 1;
            final List<GroupKey> nextGroupKeys = Lists.newArrayList((GroupKey)null);
            for (int i = 0; i < numGroups; i++) {
                final int group = i + 1;
                final List<RegroupCondition> regroupConditionsList = Lists.newArrayList();
                if (intType) {
                    final LongArrayList terms = explodeGroups.intTerms.get(i);
                    for (final long term : terms) {
                        regroupConditionsList.add(new RegroupCondition(explodeGroups.field, true, term, null, false));
                        nextGroupKeys.add(new GroupKey(String.valueOf(term), nextGroupKeys.size(), groupKeys.get(group)));
                    }
                } else {
                    final List<String> terms = explodeGroups.stringTerms.get(i);
                    for (final String term : terms) {
                        regroupConditionsList.add(new RegroupCondition(explodeGroups.field, false, 0, term, false));
                        nextGroupKeys.add(new GroupKey(term, nextGroupKeys.size(), groupKeys.get(group)));
                    }
                }
                final int[] positiveGroups = new int[regroupConditionsList.size()];
                for (int j = 0; j < regroupConditionsList.size(); j++) {
                    positiveGroups[j] = nextGroup++;
                }
                final RegroupCondition[] conditions = regroupConditionsList.toArray(new RegroupCondition[regroupConditionsList.size()]);
                final int negativeGroup;
                if (explodeGroups.defaultGroupTerm.isPresent()) {
                    negativeGroup = nextGroup++;
                    nextGroupKeys.add(new GroupKey(explodeGroups.defaultGroupTerm.get(), nextGroupKeys.size(), groupKeys.get(group)));
                } else {
                    negativeGroup = 0;
                }
                rules[i] = new GroupMultiRemapRule(group, negativeGroup, positiveGroups, conditions);
            }
            System.out.println("Exploding. rules = [" + Arrays.toString(rules) + "], nextGroup = [" + nextGroup + "]");
            sessions.values().forEach(session -> unchecked(() -> session.regroup(rules)));
            numGroups = nextGroup - 1;
            groupKeys = nextGroupKeys;
            currentDepth += 1;
            System.out.println("Exploded. numGroups = " + numGroups + ", currentDepth = " + currentDepth);
            out.println("success");
        } else if (command instanceof Commands.GetGroupStats) {
            final Commands.GetGroupStats getGroupStats = (Commands.GetGroupStats) command;
            final List<GroupStats> results = getGroupStats(getGroupStats, Optional.of(groupKeys), sessions, numGroups);

            mapper.writeValue(out, results);
            out.println();
        } else if (command instanceof Commands.CreateGroupStatsLookup) {
            final Commands.CreateGroupStatsLookup createGroupStatsLookup = (Commands.CreateGroupStatsLookup) command;
            final int depth = currentDepth;
            final double[] stats = createGroupStatsLookup.stats;
            final SavedGroupStats savedStats = new SavedGroupStats(depth, stats);
            final String lookupName = String.valueOf(savedGroupStats.size());
            savedGroupStats.put(lookupName, savedStats);
            mapper.writeValue(out, Arrays.asList(lookupName));
            out.println();
        } else if (command instanceof Commands.GetGroupDistincts) {
            final Commands.GetGroupDistincts getGroupDistincts = (Commands.GetGroupDistincts) command;
            final String field = getGroupDistincts.field;
            final boolean isIntField = isIntField(field);
            final long[] groupCounts = new long[numGroups];
            if (isIntField) {
                iterateMultiInt(sessions, Collections.emptyMap(), field, (term, stats, group) -> groupCounts[group]++);
            } else {
                iterateMultiString(sessions, Collections.emptyMap(), field, (term, stats, group) -> groupCounts[group]++);
            }
            mapper.writeValue(out, groupCounts);
            out.println();
        } else if (command instanceof Commands.GetGroupPercentiles) {
            final Commands.GetGroupPercentiles getGroupPercentiles = (Commands.GetGroupPercentiles) command;
            final String field = getGroupPercentiles.field;
            final double[] percentiles = getGroupPercentiles.percentiles;
            final long[] counts = new long[numGroups + 1];
            sessions.values().forEach(s -> unchecked(() -> {
                s.pushStat("count()");
                final long[] stats = s.getGroupStats(0);
                for (int i = 0; i < stats.length; i++) {
                    counts[i] += stats[i];
                }
            }));
            final Map<String, IntList> metricMapping = Maps.newHashMap();
            int index = 0;
            for (final String sessionName : sessions.keySet()) {
                metricMapping.put(sessionName, IntLists.singleton(index++));
            }
            final double[][] requiredCounts = new double[counts.length][];
            for (int i = 1; i < counts.length; i++) {
                requiredCounts[i] = new double[percentiles.length];
                for (int j = 0; j < percentiles.length; j++) {
                    requiredCounts[i][j] = (percentiles[j] / 100.0) * (double)counts[i];
                }
            }
            final long[][] results = new long[percentiles.length][counts.length - 1];
            final long[] runningCounts = new long[counts.length];
            iterateMultiInt(sessions, metricMapping, field, (term, stats, group) -> {
                final long oldCount = runningCounts[group];
                final long termCount = LongStream.of(stats).sum();
                final long newCount = oldCount + termCount;

                final double[] groupRequiredCountsArray = requiredCounts[group];
                for (int i = 0; i < percentiles.length; i++) {
                    final double minRequired = groupRequiredCountsArray[i];
                    if (newCount >= minRequired && oldCount < minRequired) {
                        results[i][group - 1] = term;
                    }
                }

                runningCounts[group] = newCount;
            });

            sessions.values().forEach(ImhotepSession::popStat);

            mapper.writeValue(out, results);
            out.println();
        } else {
            throw new IllegalArgumentException("Invalid command: " + commandString);
        }
    }

    private boolean isIntField(String field) {
        final boolean allIntFields = intFields.values().stream().allMatch(x -> x.contains(field));
        final boolean anyIntFields = intFields.values().stream().anyMatch(x -> x.contains(field));
        if (allIntFields != anyIntFields) {
            throw new RuntimeException("[" + field + "] is an int field in some sessions but not others.");
        }
        return allIntFields;
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

    private void unchecked(RunnableWithException runnable) {
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

    private interface IntIterateCallback {
        public void term(long term, long[] stats, int group);
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
                final IntList sessionMetricIndexes = metricIndexes.get(sessionName);
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

    private interface StringIterateCallback {
        public void term(String term, long[] stats, int group);
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
                final IntList sessionMetricIndexes = metricIndexes.get(sessionName);
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

    public static List<GroupStats> getGroupStats(Commands.GetGroupStats getGroupStats, Optional<List<GroupKey>> groupKeys, Map<String, ImhotepSession> sessions, int numGroups) throws ImhotepOutOfMemoryException {
        System.out.println("getGroupStats = [" + getGroupStats + "], sessions = [" + sessions + "], numGroups = [" + numGroups + "]");
        final Set<QualifiedPush> pushesRequired = Sets.newHashSet();
        getGroupStats.metrics.forEach(metric -> pushesRequired.addAll(metric.requires()));
        final Map<QualifiedPush, Integer> metricIndexes = Maps.newHashMap();
        final Map<String, IntList> sessionMetricIndexes = Maps.newHashMap();
        int numStats = 0;
        for (final QualifiedPush push : pushesRequired) {
            final int index = numStats++;
            metricIndexes.put(push, index);
            final String sessionName = push.sessionName;
            sessions.get(sessionName).pushStats(push.pushes);
            sessionMetricIndexes.computeIfAbsent(sessionName, k -> new IntArrayList()).add(index);
        }

        getGroupStats.metrics.forEach(metric -> metric.register(metricIndexes));

        final long[][] allStats = new long[numStats][];
        sessionMetricIndexes.forEach((name, positions) -> {
            final ImhotepSession session = sessions.get(name);
            for (int i = 0; i < positions.size(); i++) {
                allStats[positions.get(i)] = session.getGroupStats(i);
            }
        });

        final List<AggregateMetric> selectedMetrics = getGroupStats.metrics;
        final double[][] results = new double[numGroups][selectedMetrics.size()];
        final long[] groupStatsBuf = new long[allStats.length];
        for (int group = 1; group <= numGroups; group++) {
            for (int j = 0; j < allStats.length; j++) {
                groupStatsBuf[j] = allStats[j][group];
            }
            for (int j = 0; j < selectedMetrics.size(); j++) {
                results[group - 1][j] = selectedMetrics.get(j).apply(0, groupStatsBuf, group);
            }
        }

        final List<GroupStats> groupStats = Lists.newArrayList();
        for (int i = 0; i < numGroups; i++) {
            final GroupKey groupKey;
            if (groupKeys.isPresent()) {
                groupKey = groupKeys.get().get(i + 1);
            } else {
                groupKey = null;
            }
            groupStats.add(new GroupStats(groupKey, results[i]));
        }

        sessions.values().forEach(session -> {
            while (session.getNumStats() > 0) {
                session.popStat();
            }
        });

        return groupStats;
    }

    public static class GroupStats {
        public final GroupKey key;
        public final double[] stats;

        public GroupStats(GroupKey key, double[] stats) {
            this.key = key;
            this.stats = stats;
        }
    }

    public static class GroupKey {
        public final String term;
        public final int index;
        public final GroupKey parent;

        private GroupKey(String term, int index, GroupKey parent) {
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

    private interface RunnableWithException {
        void run() throws Throwable;
    }

    private static class SavedGroupStats {
        public final int depth;
        public final double[] stats;

        private SavedGroupStats(int depth, double[] stats) {
            this.depth = depth;
            this.stats = stats;
        }
    }
}
