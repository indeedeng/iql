import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * @author jwolfe
 */
public class Commands {
    private static final Logger log = Logger.getLogger(Commands.class);

    public static Object parseCommand(JsonNode command) {
        switch (command.get("command").asText()) {
            case "iterate":
                return new Iterate(command.get("field").asText(), command.get("opts"));
            case "filterDocs":
                return new FilterDocs(DocFilter.fromJson(command.get("filter")));
            case "explodeGroups": {
                final String field = command.get("field").asText();
                Optional<String> defaultName = Optional.empty();
                if (command.has("opts")) {
                    for (final JsonNode opt : command.get("opts")) {
                        switch (opt.get("type").asText()) {
                            case "addDefault":
                                defaultName = Optional.of(opt.get("name").asText());
                                break;
                        }
                    }
                }
                if (command.has("strings")) {
                    final List<List<String>> allGroupTerms = Lists.newArrayList();
                    for (final JsonNode group : command.get("strings")) {
                        final List<String> groupTerms = Lists.newArrayListWithCapacity(group.size());
                        for (final JsonNode term : group) {
                            groupTerms.add(term.asText());
                        }
                        allGroupTerms.add(groupTerms);
                    }
                    return new ExplodeGroups(field, allGroupTerms, null, defaultName);
                } else if (command.has("ints")) {
                    final List<LongArrayList> allGroupTerms = Lists.newArrayList();
                    for (final JsonNode group : command.get("strings")) {
                        final LongArrayList groupTerms = new LongArrayList(group.size());
                        for (final JsonNode term : group) {
                            groupTerms.add(term.asLong());
                        }
                        allGroupTerms.add(groupTerms);
                    }
                    return new ExplodeGroups(field, null, allGroupTerms, defaultName);
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
                        final AggregateMetric metric = AggregateMetric.fromJson(option.get("metric"));
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

        public static class TopK {
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
        public final Optional<String> defaultGroupTerm;

        public ExplodeGroups(String field, List<List<String>> stringTerms, List<LongArrayList> intTerms, Optional<String> defaultName) {
            this.field = field;
            this.stringTerms = stringTerms;
            this.intTerms = intTerms;
            defaultGroupTerm = defaultName;
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
