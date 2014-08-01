import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.common.datastruct.BoundedPriorityQueue;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author jwolfe
 */
public class Commands {
    private static final Logger log = Logger.getLogger(Commands.class);

    public static void main(String[] args) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        String[] commands = ("{\"command\":\"filterDocs\",\"filter\":{\"arg1\":{\"value\":\"x\",\"type\":\"atom\"},\"type\":\"metricEquals\",\"arg2\":{\"value\":\"y\",\"type\":\"atom\"}}}\n" +
                "{\"command\":\"iterate\",\"field\":\"country\",\"opts\":[]}\n" +
                "{\"command\":\"explodeGroups\",\"field\":\"country\",\"terms\":[]}\n" +
                "{\"command\":\"iterate\",\"field\":\"qnorm\",\"opts\":[{\"metrics\":[{\"m2\":{\"value\":[\"oji\"],\"type\":\"atom\"},\"m1\":{\"value\":[\"ojc\"],\"type\":\"atom\"},\"type\":\"division\"},{\"m2\":{\"value\":[\"sji\"],\"type\":\"atom\"},\"m1\":{\"value\":[\"sjc\"],\"type\":\"atom\"},\"type\":\"division\"}],\"type\":\"selecting\"},{\"filter\":{\"field\":\"qnorm\",\"value\":\"part time .*\",\"type\":\"regex\"},\"type\":\"filter\"}]}").split("\n");
        for (final String command : commands) {
            final JsonNode node = mapper.readTree(command);
            final Object o = parseCommand(node);
            System.out.println("o = " + o);
        }
    }

    public static void evaluateCommand(Object command, ImhotepSession session, int numGroups, BiConsumer<Integer, TermSelects> endConsumer) throws ImhotepOutOfMemoryException {
        if (command instanceof Iterate) {
            final Iterate iterate = (Iterate) command;
            final Set<List<String>> allPushLists = Sets.newHashSet();
            final List<AggregateMetric> metrics = Lists.newArrayList();
            iterate.topK.ifPresent(topK -> metrics.add(topK.metric));
            metrics.addAll(iterate.selecting);
            for (final AggregateMetric metric : metrics) {
                allPushLists.addAll(metric.requires());
            }
            iterate.filter.ifPresent(filter -> allPushLists.addAll(filter.requires()));
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
            Int2ObjectArrayMap<BoundedPriorityQueue<TermSelects>> pqs = null;
            if (iterate.topK.isPresent()) {
                pqs = new Int2ObjectArrayMap<>();
                final Comparator<TermSelects> comparator = Comparator.comparing(x -> x.topMetric);
                for (int i = 0; i < numGroups; i++) {
                    pqs.put(i, BoundedPriorityQueue.newInstance(iterate.topK.get().limit, comparator));
                }
            }
            final AggregateMetric topKMetricOrNull;
            if (iterate.topK.isPresent()) {
                topKMetricOrNull = iterate.topK.get().metric;
            } else {
                topKMetricOrNull = null;
            }
            final AggregateFilter filterOrNull = iterate.filter.orElse(null);
            try (final FTGSIterator it = session.getFTGSIterator(new String[0], new String[]{"q"})) {
                while (it.nextField()) {
                    if (it.fieldIsIntType()) {
                        while (it.nextTerm()) {
                            final long term = it.termIntVal();
                            System.out.print(term + "\t");
                            while (it.nextGroup()) {
                                final int group = it.group();
                                it.groupStats(statsBuff);
                                if (filterOrNull != null && !filterOrNull.allow(term, statsBuff)) {
                                    continue;
                                }
                                final double[] selectBuffer;
                                final double value;
                                if (pqs != null) {
                                    value = topKMetricOrNull.apply(term, statsBuff);
                                    final BoundedPriorityQueue<TermSelects> pq = pqs.get(group);
                                    if (pq.peek().topMetric > value) {
                                        continue;
                                    }
                                    selectBuffer = pq.poll().selects;
                                } else {
                                    value = 0.0;
                                    selectBuffer = new double[iterate.selecting.size()];
                                }
                                List<AggregateMetric> selecting = iterate.selecting;
                                for (int i = 0; i < selecting.size(); i++) {
                                    selectBuffer[i] = selecting.get(i).apply(term, statsBuff);
                                }
                                final TermSelects termSelect = new TermSelects(true, null, term, selectBuffer, value);
                                if (pqs != null) {
                                    pqs.get(group).offer(termSelect);
                                } else {
                                    endConsumer.accept(group, termSelect);
                                }
                            }
                        }
                    } else {
                        while (it.nextTerm()) {
                            final String term = it.termStringVal();
                            System.out.print(term + "\t");
                            while (it.nextGroup()) {
                                final int group = it.group();
                                it.groupStats(statsBuff);
                                if (filterOrNull != null && !filterOrNull.allow(term, statsBuff)) {
                                    continue;
                                }
                                final double[] selectBuffer;
                                final double value;
                                if (pqs != null) {
                                    value = topKMetricOrNull.apply(term, statsBuff);
                                    final BoundedPriorityQueue<TermSelects> pq = pqs.get(group);
                                    if (pq.peek().topMetric > value) {
                                        continue;
                                    }
                                    selectBuffer = pq.poll().selects;
                                } else {
                                    value = 0.0;
                                    selectBuffer = new double[iterate.selecting.size()];
                                }
                                List<AggregateMetric> selecting = iterate.selecting;
                                for (int i = 0; i < selecting.size(); i++) {
                                    selectBuffer[i] = selecting.get(i).apply(term, statsBuff);
                                }
                                final TermSelects termSelect = new TermSelects(false, term, 0, selectBuffer, value);
                                if (pqs != null) {
                                    pqs.get(group).offer(termSelect);
                                } else {
                                    endConsumer.accept(group, termSelect);
                                }
                            }
                        }
                    }
                }
            }
            while (session.getNumStats() != 0) {
                session.popStat();
            }
        } else if (command instanceof FilterDocs) {
            final FilterDocs filterDocs = (FilterDocs) command;
            filterDocs.docFilter.apply(session, numGroups);
        } else if (command instanceof ExplodeGroups) {
            final ExplodeGroups explodeGroups = (ExplodeGroups) command;
            if (explodeGroups.intTerms.isEmpty() == explodeGroups.stringTerms.isEmpty()) {
                throw new IllegalArgumentException("Exactly one type of term must be contained in ExplodeGroups.");
            }
            final boolean intType = !explodeGroups.intTerms.isEmpty();
            final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[numGroups];
            int nextGroup = 1;
            for (int i = 0; i < numGroups; i++) {
                final int group = i + 1;
                List<RegroupCondition> regroupConditionsList = Lists.newArrayList();
                if (intType) {
                    final LongArrayList terms = explodeGroups.intTerms.get(i);
                    for (final long term : terms) {
                        regroupConditionsList.add(new RegroupCondition(explodeGroups.field, true, term, null, false));
                    }
                } else {
                    final List<String> terms = explodeGroups.stringTerms.get(i);
                    for (final String term : terms) {
                        regroupConditionsList.add(new RegroupCondition(explodeGroups.field, false, 0, term, false));
                    }
                }
                final int[] positiveGroups = new int[regroupConditionsList.size()];
                for (int j = 0; j < regroupConditionsList.size(); j++) {
                    positiveGroups[j] = nextGroup++;
                }
                final RegroupCondition[] conditions = regroupConditionsList.toArray(new RegroupCondition[regroupConditionsList.size()]);
                rules[i] = new GroupMultiRemapRule(group, 0, positiveGroups, conditions);
            }
            numGroups = session.regroup(rules);
        } else if (command instanceof GetGroupStats) {
            final GetGroupStats getGroupStats = (GetGroupStats) command;
            final Set<List<String>> pushesRequired = Sets.newHashSet();
            getGroupStats.metrics.forEach(metric -> pushesRequired.addAll(metric.requires()));
            final Map<List<String>, Integer> metricIndexes = Maps.newHashMap();
            for (final List<String> push : pushesRequired) {
                metricIndexes.put(push, session.pushStats(push) - 1);
            }

            final long[][] allStats = new long[session.getNumStats()][];
            for (int i = 0; i < allStats.length; i++) {
                allStats[i] = session.getGroupStats(i);
            }

            final List<AggregateMetric> selectedMetrics = getGroupStats.metrics;
            final double[][] results = new double[numGroups][selectedMetrics.size()];
            final long[] groupStatsBuf = new long[allStats.length];
            for (int i = 0; i < numGroups; i++) {
                System.arraycopy(allStats[i], 0, groupStatsBuf, 0, groupStatsBuf.length);
                for (int j = 0; j < selectedMetrics.size(); j++) {
                    results[i][j] = selectedMetrics.get(j).apply(0, groupStatsBuf);
                }
            }

            while (session.getNumStats() != 0) {
                session.popStat();
            }
        }
    }

    public static Object parseCommand(JsonNode command) {
        switch (command.get("command").asText()) {
            case "iterate":
                return new Iterate(command.get("field").asText(), command.get("opts"));
            case "filterDocs":
                return new FilterDocs(DocFilter.fromJson(command.get("filter")));
            case "explodeGroups": {
                final String field = command.get("field").asText();
                if (command.has("strings")) {
                    final List<List<String>> allGroupTerms = Lists.newArrayList();
                    for (final JsonNode group : command.get("strings")) {
                        final List<String> groupTerms = Lists.newArrayListWithCapacity(group.size());
                        for (final JsonNode term : group) {
                            groupTerms.add(term.asText());
                        }
                        allGroupTerms.add(groupTerms);
                    }
                    return new ExplodeGroups(field, allGroupTerms, null);
                } else if (command.has("ints")) {
                    final List<LongArrayList> allGroupTerms = Lists.newArrayList();
                    for (final JsonNode group : command.get("strings")) {
                        final LongArrayList groupTerms = new LongArrayList(group.size());
                        for (final JsonNode term : group) {
                            groupTerms.add(term.asLong());
                        }
                        allGroupTerms.add(groupTerms);
                    }
                    return new ExplodeGroups(field, null, allGroupTerms);
                } else {
                    throw new IllegalArgumentException("uhh?:" + command);
                }
            }
            case "getGroupStats": {
                final List<AggregateMetric> metrics = Lists.newArrayListWithCapacity(command.size());
                for (final JsonNode metric : command.get("metrics")) {
                    metrics.add(AggregateMetric.fromJson(metric));
                }
                return new GetGroupStats(metrics);
            }
        }
        throw new RuntimeException("oops:" + command);
    }

    /**
     toJSON (Iterate field opts) = object
     [ "command" .= text "iterate"
     , "field" .= field
     , "opts" .= opts
     ]
     */
    public static class Iterate {
        public final String field;
        public final OptionalInt limit;
        public final Optional<TopK> topK;
        public final List<AggregateMetric> selecting = Lists.newArrayList();
        public final Optional<AggregateFilter> filter;


        public Iterate(String field, JsonNode options) {
            this.field = field;
            OptionalInt limit = OptionalInt.empty();
            Optional<TopK> topK = Optional.empty();
            Optional<AggregateFilter> filter = Optional.empty();
            for (final JsonNode option : options) {
                switch (option.get("type").asText()) {
                    case "filter": {
                        filter = Optional.of(AggregateFilter.fromJson(option.get("filter")));
                    }
                        break;
                    case "limit":
                        limit = OptionalInt.of(option.get("k").asInt());
                        break;
                    case "top": {
                        final int k = option.get("k").asInt();
                        final AggregateMetric metric = AggregateMetric.fromJson(option.get("metrics"));
                        topK = Optional.of(new TopK(k, metric));
                    }
                        break;
                    case "selecting":
                        for (final JsonNode metric : option.get("metrics")) {
                            selecting.add(AggregateMetric.fromJson(metric));
                        }
                        break;
                }
            }
            this.limit = limit;
            this.topK = topK;
            this.filter = filter;
        }

        private static class TopK {
            public final int limit;
            public final AggregateMetric metric;

            private TopK(int limit, AggregateMetric metric) {
                this.limit = limit;
                this.metric = metric;
            }
        }
    }

    /**
     toJSON (FilterDocs filterDef) = object
     [ "command" .= text "filterDocs",
     "filter" .= filterDef
     ]
     */
    public static class FilterDocs {
        public final DocFilter docFilter;

        public FilterDocs(DocFilter docFilter) {
            this.docFilter = docFilter;
        }
    }

    /**
     toJSON (ExplodeGroups terms) = object
     [ "command" .= text "explodeGroups"
     , "terms" .= terms
     ]
     */
    public static class ExplodeGroups {
        public final String field;
        public final List<List<String>> stringTerms;
        public final List<LongArrayList> intTerms;

        public ExplodeGroups(String field, List<List<String>> stringTerms, List<LongArrayList> intTerms) {
            this.field = field;
            this.stringTerms = stringTerms;
            this.intTerms = intTerms;
        }
    }

    /**
     toJSON (GetGroupStats metrics) = object
     [ "command" .= text "getGroupStats"
     , "metrics" .= metrics
     ]
     */
    public static class GetGroupStats {
        public final List<AggregateMetric> metrics;

        public GetGroupStats(List<AggregateMetric> metrics) {
            this.metrics = metrics;
        }
    }

    private static class TermSelects {
        public boolean isIntTerm;
        public String stringTerm;
        public long intTerm;

        public final double[] selects;
        public double topMetric;

        private TermSelects(boolean isIntTerm, String stringTerm, long intTerm, double[] selects, double topMetric) {
            this.stringTerm = stringTerm;
            this.intTerm = intTerm;
            this.isIntTerm = isIntTerm;
            this.selects = selects;
            this.topMetric = topMetric;
        }
    }
}
