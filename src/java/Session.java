import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.common.datastruct.BoundedPriorityQueue;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.client.ImhotepClient;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * @author jwolfe
 */
public class Session {
    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }
    private static final Logger log = Logger.getLogger(Session.class);

    public List<String> fields = Lists.newArrayList();
    public List<GroupKey> groupKeys = Lists.newArrayList((GroupKey)null, (GroupKey)null);

    public final ImhotepSession session;
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
                    jgen.writeObjectField("key", value.groupKey.asList());
                }
                jgen.writeEndObject();
            }
        });
        mapper.registerModule(module);
        mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    }

    public Session(ImhotepSession session) {
        this.session = session;
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
        final ImhotepClient client = new ImhotepClient("/Users/jwolfe/hosts.txt");
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
                    final String dataset = sessionRequest.get("dataset").asText();
                    final String start = sessionRequest.get("start").asText();
                    final String end = sessionRequest.get("end").asText();
                    try (final ImhotepSession session = client.sessionBuilder(dataset, DateTime.parse(start), DateTime.parse(end)).build()) {
                        final Session session1 = new Session(session);
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

    public void evaluateCommand(String commandString, PrintWriter out) throws ImhotepOutOfMemoryException, IOException {
        final Object command = Commands.parseCommand(mapper.readTree(commandString));
        if (command instanceof Commands.Iterate) {
            final Commands.Iterate iterate = (Commands.Iterate) command;
            final Set<List<String>> allPushLists = Sets.newHashSet();
            final List<AggregateMetric> metrics = Lists.newArrayList();
            iterate.topK.ifPresent(topK -> metrics.add(topK.metric));
            metrics.addAll(iterate.selecting);
            for (final AggregateMetric metric : metrics) {
                allPushLists.addAll(metric.requires());
            }
            iterate.filter.ifPresent(filter -> allPushLists.addAll(filter.requires()));
            for (final AggregateMetric metric : metrics) {
                metric.preIterate(session, numGroups);
            }
            final Map<List<String>, Integer> metricIndexes = Maps.newHashMap();
            for (final List<String> pushList : allPushLists) {
                final int index = session.pushStats(pushList) - 1;
                metricIndexes.put(pushList, index);
            }
            for (final AggregateMetric metric : metrics) {
                metric.register(metricIndexes);
            }
            iterate.filter.ifPresent(filter -> filter.register(metricIndexes));
            final long[] statsBuff = new long[session.getNumStats()];
            Int2ObjectArrayMap<Queue<TermSelects>> pqs = new Int2ObjectArrayMap<>();
            if (iterate.topK.isPresent()) {
                final Comparator<TermSelects> comparator = Comparator.comparing(x -> x.topMetric);
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
            try (final FTGSIterator it = session.getFTGSIterator(new String[0], new String[]{iterate.field})) {
                while (it.nextField()) {
                    if (it.fieldIsIntType()) {
                        while (it.nextTerm()) {
                            final long term = it.termIntVal();
                            while (it.nextGroup()) {
                                final int group = it.group();
                                it.groupStats(statsBuff);
                                if (filterOrNull != null && !filterOrNull.allow(term, statsBuff, group)) {
                                    continue;
                                }
                                final double[] selectBuffer;
                                final double value;
                                if (topKMetricOrNull != null) {
                                    value = topKMetricOrNull.apply(term, statsBuff, group);
                                } else {
                                    value = 0.0;
                                }
                                selectBuffer = new double[iterate.selecting.size()];
                                final List<AggregateMetric> selecting = iterate.selecting;
                                for (int i = 0; i < selecting.size(); i++) {
                                    selectBuffer[i] = selecting.get(i).apply(term, statsBuff, group);
                                }
                                pqs.get(group).offer(new TermSelects(true, null, term, selectBuffer, value, groupKeys.get(group)));
                            }
                        }
                    } else {
                        while (it.nextTerm()) {
                            final String term = it.termStringVal();
                            while (it.nextGroup()) {
                                final int group = it.group();
                                it.groupStats(statsBuff);
                                if (filterOrNull != null && !filterOrNull.allow(term, statsBuff, group)) {
                                    continue;
                                }
                                final double[] selectBuffer = new double[iterate.selecting.size()];
                                final double value;
                                if (topKMetricOrNull != null) {
                                    value = topKMetricOrNull.apply(term, statsBuff, group);
                                } else {
                                    value = 0.0;
                                }
                                final List<AggregateMetric> selecting = iterate.selecting;
                                for (int i = 0; i < selecting.size(); i++) {
                                    selectBuffer[i] = selecting.get(i).apply(term, statsBuff, group);
                                }
                                pqs.get(group).offer(new TermSelects(false, term, 0, selectBuffer, value, groupKeys.get(group)));
                            }
                        }
                    }
                }
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
            while (session.getNumStats() != 0) {
                session.popStat();
            }
        } else if (command instanceof Commands.FilterDocs) {
            final Commands.FilterDocs filterDocs = (Commands.FilterDocs) command;
            filterDocs.docFilter.apply(session, numGroups);
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
                        nextGroupKeys.add(new GroupKey(String.valueOf(term), groupKeys.get(group)));
                    }
                } else {
                    final List<String> terms = explodeGroups.stringTerms.get(i);
                    for (final String term : terms) {
                        regroupConditionsList.add(new RegroupCondition(explodeGroups.field, false, 0, term, false));
                        nextGroupKeys.add(new GroupKey(term, groupKeys.get(group)));
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
                    nextGroupKeys.add(new GroupKey(explodeGroups.defaultGroupTerm.get(), groupKeys.get(group)));
                } else {
                    negativeGroup = 0;
                }
                rules[i] = new GroupMultiRemapRule(group, negativeGroup, positiveGroups, conditions);
            }
            System.out.println("Exploding");
            numGroups = session.regroup(rules);
            groupKeys = nextGroupKeys;
            System.out.println("Exploded");
            out.println("success");
        } else if (command instanceof Commands.GetGroupStats) {
            final Commands.GetGroupStats getGroupStats = (Commands.GetGroupStats) command;
            final double[][] results = getGroupStats(getGroupStats, session, numGroups);

            mapper.writeValue(out, results);
            out.println();
        } else {
            throw new IllegalArgumentException("Invalid command: " + commandString);
        }
    }

    public static double[][] getGroupStats(Commands.GetGroupStats getGroupStats, ImhotepSession session, int numGroups) throws ImhotepOutOfMemoryException {
        final int initialNumStats = session.getNumStats();
        final Set<List<String>> pushesRequired = Sets.newHashSet();
        getGroupStats.metrics.forEach(metric -> pushesRequired.addAll(metric.requires()));
        final Map<List<String>, Integer> metricIndexes = Maps.newHashMap();
        for (final List<String> push : pushesRequired) {
            metricIndexes.put(push, session.pushStats(push) - 1);
        }

        getGroupStats.metrics.forEach(metric -> metric.register(metricIndexes));

        final long[][] allStats = new long[session.getNumStats()][];
        for (int i = 0; i < allStats.length; i++) {
            allStats[i] = session.getGroupStats(i);
        }

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

        while (session.getNumStats() != initialNumStats) {
            session.popStat();
        }
        return results;
    }

    public static class GroupKey {
        public final String term;
        public final GroupKey parent;

        private GroupKey(String term, GroupKey parent) {
            this.term = term;
            this.parent = parent;
        }

        public List<String> asList() {
            final List<String> keys = Lists.newArrayList();
            GroupKey node = this;
            while (node != null) {
                keys.add(node.term);
                node = node.parent;
            }
            return Lists.reverse(keys);
        }
    }
}
