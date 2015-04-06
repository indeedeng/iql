import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.common.util.Pair;
import com.indeed.flamdex.query.Term;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;

/**
 * @author jwolfe
 */
public class Commands {
    private static final Logger log = Logger.getLogger(Commands.class);

    public static Object parseCommand(JsonNode command, Function<String, AggregateMetric.PerGroupConstant> namedMetricLookup) {
        switch (command.get("command").asText()) {
            case "iterate": {
                final List<AggregateMetric> selecting = Lists.newArrayList();
                Optional<Pair<Integer, Iterate.FieldLimitingMechanism>> fieldLimits = Optional.empty();
                final Iterate.FieldIterateOpts defaultOpts = new Iterate.FieldIterateOpts();

                final JsonNode globalOpts = command.get("opts");
                for (final JsonNode globalOpt : globalOpts) {
                    switch (globalOpt.get("type").asText()) {
                        case "selecting": {
                            for (final JsonNode metric : globalOpt.get("metrics")) {
                                selecting.add(AggregateMetric.fromJson(metric, namedMetricLookup));
                            }
                            break;
                        }
                        case "limitingFields": {
                            fieldLimits = Optional.of(Pair.of(
                                    globalOpt.get("numFields").asInt(),
                                    Iterate.FieldLimitingMechanism.valueOf(globalOpt.get("by").asText())
                            ));
                            break;
                        }
                        case "defaultedFieldOpts": {
                            defaultOpts.parseFrom(globalOpt.get("opts"), namedMetricLookup);
                            break;
                        }
                    }
                }

                final List<Iterate.FieldWithOptions> fieldsWithOpts = Lists.newArrayList();
                final JsonNode fields = command.get("fields");
                for (final JsonNode field : fields) {
                    final String fieldName = field.get(0).asText();
                    final JsonNode optsNode = field.get(1);

                    final Iterate.FieldIterateOpts fieldIterateOpts = defaultOpts.copy();
                    fieldIterateOpts.parseFrom(optsNode, namedMetricLookup);

                    fieldsWithOpts.add(new Iterate.FieldWithOptions(fieldName, fieldIterateOpts));
                }

                return new Iterate(fieldsWithOpts, fieldLimits, selecting);
            }
            case "filterDocs":
                return new FilterDocs(DocFilter.fromJson(command.get("filter")));
            case "explodeGroups": {
                final String field = command.get("field").asText();
                final Optional<String> defaultName = parseExplodeOpts(command.get("opts"));
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
                    for (final JsonNode group : command.get("ints")) {
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
                final List<AggregateMetric> metrics = Lists.newArrayListWithCapacity(command.get("metrics").size());
                for (final JsonNode metric : command.get("metrics")) {
                    metrics.add(AggregateMetric.fromJson(metric, namedMetricLookup));
                }
                boolean returnGroupKeys = false;
                for (final JsonNode opt : command.get("opts")) {
                    switch (opt.get("type").asText()) {
                        case "returnGroupKeys": {
                            returnGroupKeys = true;
                            break;
                        }
                    }
                }
                return new GetGroupStats(metrics, returnGroupKeys);
            }
            case "createGroupStatsLookup": {
                final JsonNode valuesNode = command.get("values");
                final double[] stats = new double[valuesNode.size() + 1];
                for (int i = 0; i < valuesNode.size(); i++) {
                    stats[i + 1] = valuesNode.get(i).asDouble();
                }
                final Optional<String> name;
                if (command.has("name")) {
                    name = Optional.ofNullable(command.get("name").textValue());
                } else {
                    name = Optional.empty();
                }
                return new CreateGroupStatsLookup(stats, name);
            }
            case "getGroupDistincts": {
                final Set<String> scope = Sets.newHashSet(Iterables.transform(command.get("scope"), JsonNode::asText));
                final String field = command.get("field").asText();
                final JsonNode filterNode = command.get("filter");
                final int windowSize = command.get("windowSize").asInt();

                final Optional<AggregateFilter> filter;
                if (filterNode.isNull()) {
                    filter = Optional.empty();
                } else {
                    filter = Optional.of(AggregateFilter.fromJson(filterNode, namedMetricLookup));
                }
                return new GetGroupDistincts(scope, field, filter, windowSize);
            }
            case "getGroupPercentiles": {
                final String field = command.get("field").asText();
                final Set<String> scope = Sets.newHashSet(Iterables.transform(command.get("scope"), JsonNode::asText));
                final JsonNode percentilesNode = command.get("percentiles");
                final double[] percentiles = new double[percentilesNode.size()];
                for (int i = 0; i < percentilesNode.size(); i++) {
                    percentiles[i] = percentilesNode.get(i).asDouble();
                }
                return new GetGroupPercentiles(scope, field, percentiles);
            }
            case "metricRegroup": {
                return new MetricRegroup(
                        DocMetric.fromJson(command.get("metric")),
                        command.get("min").asLong(),
                        command.get("max").asLong(),
                        command.get("interval").asLong()
                );
            }
            case "timeRegroup": {
                final Optional<String> timeField;
                if (command.has("timeField")) {
                    timeField = Optional.ofNullable(command.get("timeField").textValue());
                } else {
                    timeField = Optional.empty();
                }
                return new TimeRegroup(
                        command.get("value").asLong(),
                        command.get("unit").asText().charAt(0),
                        command.get("offsetMinutes").asLong(),
                        timeField);
            }
            case "getNumGroups": {
                return new GetNumGroups();
            }
            case "explodePerGroup": {
                final JsonNode fieldTermOpts = command.get("fieldTermOpts");
                final List<TermsWithExplodeOpts> termsWithExplodeOpts = Lists.newArrayListWithCapacity(fieldTermOpts.size() + 1);
                termsWithExplodeOpts.add(null);
                for (int i = 0; i < fieldTermOpts.size(); i++) {
                    final JsonNode pairNode = fieldTermOpts.get(i);
                    final JsonNode fieldTermsNode = pairNode.get(0);
                    final List<Term> terms = Lists.newArrayListWithCapacity(fieldTermsNode.size());
                    for (int j = 0; j < fieldTermsNode.size(); j++) {
                        final JsonNode fieldTermNode = fieldTermsNode.get(j);
                        final String field = fieldTermNode.get(0).asText();
                        final JsonNode termNode = fieldTermNode.get(1);
                        final Term term;
                        if (termNode.get("type").asText().equals("string")) {
                            term = Term.stringTerm(field, termNode.get("value").asText());
                        } else {
                            term = Term.intTerm(field, termNode.get("value").asLong());
                        }
                        terms.add(term);
                    }
                    final JsonNode explodeOpts = pairNode.get(1);
                    final Optional<String> defaultName = parseExplodeOpts(explodeOpts);
                    termsWithExplodeOpts.add(new TermsWithExplodeOpts(terms, defaultName));
                }
                return new ExplodePerGroup(termsWithExplodeOpts);
            }
            case "explodeDayOfWeek": {
                return new ExplodeDayOfWeek();
            }
            case "explodeSessionNames": {
                return new ExplodeSessionNames();
            }
        }
        throw new RuntimeException("oops:" + command);
    }

    private static Optional<String> parseExplodeOpts(JsonNode opts) {
        Optional<String> defaultName = Optional.empty();
        for (final JsonNode opt : opts) {
            switch (opt.get("type").asText()) {
                case "addDefault":
                    defaultName = Optional.of(opt.get("name").asText());
                    break;
            }
        }
        return defaultName;
    }

    /**
     toJSON (Iterate field opts) = object
     [ "command" .= text "iterate"
     , "field" .= field
     , "opts" .= opts
     ]
     */
    public static class Iterate {
        public final List<FieldWithOptions> fields;
        public final Optional<Pair<Integer, FieldLimitingMechanism>> fieldLimitingOpts;
        public final List<AggregateMetric> selecting;

        public Iterate(List<FieldWithOptions> fields, Optional<Pair<Integer, FieldLimitingMechanism>> fieldLimitingOpts, List<AggregateMetric> selecting) {
            this.fields = fields;
            this.fieldLimitingOpts = fieldLimitingOpts;
            this.selecting = selecting;
        }

        enum FieldLimitingMechanism {MinimalMin, MaximalMax}

        public static class TopK {
            public final int limit;
            public final AggregateMetric metric;

            private TopK(int limit, AggregateMetric metric) {
                this.limit = limit;
                this.metric = metric;
            }
        }

        public static class FieldWithOptions {
            public final String field;
            public final FieldIterateOpts opts;

            public FieldWithOptions(String field, FieldIterateOpts opts) {
                this.field = field;
                this.opts = opts;
            }
        }

        public static class FieldIterateOpts {
            OptionalInt limit = OptionalInt.empty();
            Optional<Iterate.TopK> topK = Optional.empty();
            Optional<AggregateFilter> filter = Optional.empty();

            public void parseFrom(JsonNode options, Function<String, AggregateMetric.PerGroupConstant> namedMetricLookup) {
                for (final JsonNode option : options) {
                    switch (option.get("type").asText()) {
                        case "filter": {
                            this.filter = Optional.of(AggregateFilter.fromJson(option.get("filter"), namedMetricLookup));
                            break;
                        }
                        case "limit": {
                            this.limit = OptionalInt.of(option.get("k").asInt());
                            break;
                        }
                        case "top": {
                            final int k = option.get("k").asInt();
                            final AggregateMetric metric = AggregateMetric.fromJson(option.get("metric"), namedMetricLookup);
                            this.topK = Optional.of(new Iterate.TopK(k, metric));
                            break;
                        }
                    }
                }
            }

            public FieldIterateOpts copy() {
                final FieldIterateOpts result = new FieldIterateOpts();
                result.limit = this.limit;
                result.topK = this.topK;
                result.filter = this.filter;
                return result;
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
        public final boolean returnGroupKeys;

        public GetGroupStats(List<AggregateMetric> metrics, boolean returnGroupKeys) {
            this.metrics = metrics;
            this.returnGroupKeys = returnGroupKeys;
        }
    }

    public static class CreateGroupStatsLookup {
        public final double[] stats;
        public final Optional<String> name;

        public CreateGroupStatsLookup(double[] stats, Optional<String> name) {
            this.stats = stats;
            this.name = name;
        }
    }

    public static class GetGroupDistincts {
        public final Set<String> scope;
        public final String field;
        public final Optional<AggregateFilter> filter;
        public final int windowSize;

        public GetGroupDistincts(Set<String> scope, String field, Optional<AggregateFilter> filter, int windowSize) {
            this.scope = scope;
            this.field = field;
            this.filter = filter;
            this.windowSize = windowSize;
        }
    }

    public static class GetGroupPercentiles {
        public final Set<String> scope;
        public final String field;
        public final double[] percentiles;

        public GetGroupPercentiles(Set<String> scope, String field, double[] percentiles) {
            this.scope = scope;
            this.field = field;
            this.percentiles = percentiles;
        }
    }

    public static class MetricRegroup {
        public final DocMetric metric;
        public final long min;
        public final long max;
        public final long interval;

        public MetricRegroup(DocMetric metric, long min, long max, long interval) {
            this.metric = metric;
            this.min = min;
            this.max = max;
            this.interval = interval;
        }
    }

    public static class TimeRegroup {
        public final long value;
        public final char unit;
        public final long offsetMinutes;
        public final Optional<String> timeField;

        public TimeRegroup(long value, char unit, long offsetMinutes, Optional<String> timeField) {
            this.value = value;
            this.unit = unit;
            this.offsetMinutes = offsetMinutes;
            this.timeField = timeField;
        }
    }

    public static class GetNumGroups {
    }

    public static class TermsWithExplodeOpts {
        public final List<Term> terms;
        public final Optional<String> defaultName;

        public TermsWithExplodeOpts(List<Term> terms, Optional<String> defaultName) {
            this.terms = terms;
            this.defaultName = defaultName;
        }
    }

    public static class ExplodePerGroup {
        public final List<TermsWithExplodeOpts> termsWithExplodeOpts;

        public ExplodePerGroup(List<TermsWithExplodeOpts> termsWithExplodeOpts) {
            this.termsWithExplodeOpts = termsWithExplodeOpts;
        }
    }

    public static class ExplodeDayOfWeek {
    }

    public static class ExplodeSessionNames {
    }
}
