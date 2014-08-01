import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

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

    public static Object parseCommand(JsonNode command) {
        switch (command.get("command").asText()) {
            case "iterate":
                return new Iterate(command.get("field").asText(), command.get("opts"));
            case "filterDocs":
                return new FilterDocs(DocFilter.fromJson(command.get("filter")));
            case "explodeGroups": {
                final List<String> stringTerms = Lists.newArrayList();
                final LongArrayList intTerms = new LongArrayList();
                final String field = command.get("field").asText();
                for (final JsonNode term : command.get("terms")) {
                    switch (term.get("type").asText()) {
                        case "string":
                            stringTerms.add(term.get("value").asText());
                            break;
                        case "int":
                            intTerms.add(term.get("value").asLong());
                            break;
                    }
                }
                return new ExplodeGroups(field, stringTerms, intTerms);
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
        public final List<String> stringTerms;
        public final LongArrayList intTerms;

        public ExplodeGroups(String field, List<String> stringTerms, LongArrayList intTerms) {
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
}
