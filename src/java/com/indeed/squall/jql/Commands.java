package com.indeed.squall.jql;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.indeed.common.util.Pair;
import com.indeed.flamdex.query.Term;
import com.indeed.squall.jql.commands.GetGroupStats;
import com.indeed.squall.jql.commands.Iterate;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * @author jwolfe
 */
public class Commands {
    private static final Logger log = Logger.getLogger(Commands.class);

    public static Object parseCommand(JsonNode command, Function<String, AggregateMetric.PerGroupConstant> namedMetricLookup) {
        switch (command.get("command").textValue()) {
            case "iterate": {
                final List<AggregateMetric> selecting = Lists.newArrayList();
                final Iterate.FieldIterateOpts defaultOpts = new Iterate.FieldIterateOpts();
                final Optional<Pair<Integer, Iterate.FieldLimitingMechanism>> fieldLimits = parseIterateOpts(namedMetricLookup, selecting, defaultOpts, command.get("opts"));

                final List<Iterate.FieldWithOptions> fieldsWithOpts = Lists.newArrayList();
                final JsonNode fields = command.get("fields");
                for (final JsonNode field : fields) {
                    final String fieldName = field.get(0).textValue();
                    final JsonNode optsNode = field.get(1);

                    final Iterate.FieldIterateOpts fieldIterateOpts = defaultOpts.copy();
                    fieldIterateOpts.parseFrom(optsNode, namedMetricLookup);

                    fieldsWithOpts.add(new Iterate.FieldWithOptions(fieldName, fieldIterateOpts));
                }

                return new Iterate(fieldsWithOpts, fieldLimits, selecting);
            }
            case "filterDocs": {
                final Map<String, List<String>> perDatasetFilterMetric = Maps.newHashMap();
                final JsonNode filters = command.get("perDatasetFilter");
                filters.fieldNames().forEachRemaining(filterName -> {
                    final List<String> pushes = Lists.newArrayList();
                    final JsonNode filterList = filters.get(filterName);
                    for (int i = 0; i < filterList.size(); i++) {
                        pushes.add(filterList.get(i).textValue());
                    }
                    perDatasetFilterMetric.put(filterName, pushes);
                });
                return new FilterDocs(perDatasetFilterMetric);
            }
            case "explodeGroups": {
                final String field = command.get("field").textValue();
                final Optional<String> defaultName = parseExplodeOpts(command.get("opts"));
                if (command.has("strings")) {
                    final List<List<String>> allGroupTerms = Lists.newArrayList();
                    for (final JsonNode group : command.get("strings")) {
                        final List<String> groupTerms = Lists.newArrayListWithCapacity(group.size());
                        for (final JsonNode term : group) {
                            groupTerms.add(term.textValue());
                        }
                        allGroupTerms.add(groupTerms);
                    }
                    return new ExplodeGroups(field, allGroupTerms, null, defaultName);
                } else if (command.has("ints")) {
                    final List<LongArrayList> allGroupTerms = Lists.newArrayList();
                    for (final JsonNode group : command.get("ints")) {
                        final LongArrayList groupTerms = new LongArrayList(group.size());
                        for (final JsonNode term : group) {
                            groupTerms.add(term.longValue());
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
                    switch (opt.get("type").textValue()) {
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
                    stats[i + 1] = valuesNode.get(i).doubleValue();
                }
                final Optional<String> name = getOptionalName(command);
                return new CreateGroupStatsLookup(stats, name);
            }
            case "getGroupDistincts": {
                final Set<String> scope = Sets.newHashSet(Iterables.transform(command.get("scope"), JsonNode::textValue));
                final String field = command.get("field").textValue();
                final JsonNode filterNode = command.get("filter");
                final int windowSize = command.get("windowSize").intValue();

                final Optional<AggregateFilter> filter;
                if (filterNode.isNull()) {
                    filter = Optional.empty();
                } else {
                    filter = Optional.of(AggregateFilter.fromJson(filterNode, namedMetricLookup));
                }
                return new GetGroupDistincts(scope, field, filter, windowSize);
            }
            case "getGroupPercentiles": {
                final String field = command.get("field").textValue();
                final Set<String> scope = Sets.newHashSet(Iterables.transform(command.get("scope"), JsonNode::textValue));
                final JsonNode percentilesNode = command.get("percentiles");
                final double[] percentiles = new double[percentilesNode.size()];
                for (int i = 0; i < percentilesNode.size(); i++) {
                    percentiles[i] = percentilesNode.get(i).doubleValue();
                }
                return new GetGroupPercentiles(scope, field, percentiles);
            }
            case "metricRegroup": {
                final Map<String, List<String>> perDatasetMetric = Maps.newHashMap();
                final JsonNode metrics = command.get("perDatasetMetric");
                metrics.fieldNames().forEachRemaining(filterName -> {
                    final List<String> pushes = Lists.newArrayList();
                    final JsonNode metricList = metrics.get(filterName);
                    for (int i = 0; i < metricList.size(); i++) {
                        pushes.add(metricList.get(i).textValue());
                    }
                    perDatasetMetric.put(filterName, pushes);
                });
                return new MetricRegroup(
                        perDatasetMetric,
                        command.get("min").longValue(),
                        command.get("max").longValue(),
                        command.get("interval").longValue()
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
                        command.get("value").longValue(),
                        command.get("unit").textValue().charAt(0),
                        command.get("offsetMinutes").longValue(),
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
                        final String field = fieldTermNode.get(0).textValue();
                        final JsonNode termNode = fieldTermNode.get(1);
                        final Term term;
                        if (termNode.get("type").textValue().equals("string")) {
                            term = Term.stringTerm(field, termNode.get("value").textValue());
                        } else {
                            term = Term.intTerm(field, termNode.get("value").longValue());
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
            case "iterateAndExplode": {
                final String field = command.get("field").textValue();
                final Optional<String> explodeDefaultName = parseExplodeOpts(command.get("explodeOpts"));

                final List<AggregateMetric> selecting = Lists.newArrayList();
                final Iterate.FieldIterateOpts fieldOpts = new Iterate.FieldIterateOpts();
                final Optional<Pair<Integer, Iterate.FieldLimitingMechanism>> fieldLimits = parseIterateOpts(namedMetricLookup, selecting, fieldOpts, command.get("iterOpts"));

                return new IterateAndExplode(field, selecting, fieldOpts, fieldLimits, explodeDefaultName);
            }
            case "computeAndCreateGroupStatsLookup": {
                final Object computation = parseCommand(command.get("computation"), namedMetricLookup);
                if (computation instanceof GetGroupDistincts) {
                } else if (computation instanceof GetGroupPercentiles) {
                    final GetGroupPercentiles getGroupPercentiles = (GetGroupPercentiles) computation;
                    if (getGroupPercentiles.percentiles.length != 1) {
                        throw new IllegalArgumentException("Cannot handle multi-percentile GetGroupPercentiles in ComputeAndCreateGroupStatsLookup");
                    }
                } else if (computation instanceof GetGroupStats) {
                    final GetGroupStats getGroupStats = (GetGroupStats) computation;
                    if (getGroupStats.metrics.size() != 1) {
                        throw new IllegalArgumentException("Cannot handle multiple metrics in ComputeAndCreateGroupStatsLookup");
                    }
                } else {
                    throw new IllegalArgumentException("Can only do group distincts, group percentiles, or group stats!");
                }
                return new ComputeAndCreateGroupStatsLookup(computation, getOptionalName(command));
            }
            case "explodeByAggregatePercentile": {
                final String field = command.get("field").textValue();
                final AggregateMetric metric = AggregateMetric.fromJson(command.get("metric"), namedMetricLookup);
                final int numBuckets = command.get("numBuckets").intValue();
                return new ExplodeByAggregatePercentile(field, metric, numBuckets);
            }
            case "explodePerDocPercentile": {
                final String field = command.get("field").textValue();
                final int numBuckets = command.get("numBuckets").intValue();
                return new ExplodePerDocPercentile(field, numBuckets);
            }
        }
        throw new RuntimeException("oops:" + command);
    }

    private static Optional<String> getOptionalName(JsonNode command) {
        final Optional<String> name;
        if (command.has("name")) {
            name = Optional.ofNullable(command.get("name").textValue());
        } else {
            name = Optional.empty();
        }
        return name;
    }

    private static Optional<Pair<Integer, Iterate.FieldLimitingMechanism>> parseIterateOpts(Function<String, AggregateMetric.PerGroupConstant> namedMetricLookup, List<AggregateMetric> selecting, Iterate.FieldIterateOpts defaultOpts, JsonNode globalOpts) {
        Optional<Pair<Integer, Iterate.FieldLimitingMechanism>> fieldLimits = Optional.empty();
        for (final JsonNode globalOpt : globalOpts) {
            switch (globalOpt.get("type").textValue()) {
                case "selecting": {
                    for (final JsonNode metric : globalOpt.get("metrics")) {
                        selecting.add(AggregateMetric.fromJson(metric, namedMetricLookup));
                    }
                    break;
                }
                case "limitingFields": {
                    fieldLimits = Optional.of(Pair.of(
                            globalOpt.get("numFields").intValue(),
                            Iterate.FieldLimitingMechanism.valueOf(globalOpt.get("by").textValue())
                    ));
                    break;
                }
                case "defaultedFieldOpts": {
                    defaultOpts.parseFrom(globalOpt.get("opts"), namedMetricLookup);
                    break;
                }
            }
        }
        return fieldLimits;
    }

    private static Optional<String> parseExplodeOpts(JsonNode opts) {
        if (opts == null) {
            return Optional.empty();
        }
        Optional<String> defaultName = Optional.empty();
        for (final JsonNode opt : opts) {
            switch (opt.get("type").textValue()) {
                case "addDefault":
                    defaultName = Optional.of(opt.get("name").textValue());
                    break;
            }
        }
        return defaultName;
    }

    public static class FilterDocs {
        public final Map<String, List<String>> perDatasetFilterMetric;

        public FilterDocs(Map<String, List<String>> perDatasetFilterMetric) {
            final Map<String, List<String>> copy = Maps.newHashMap();
            for (final Map.Entry<String, List<String>> entry : perDatasetFilterMetric.entrySet()) {
                copy.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
            }
            this.perDatasetFilterMetric = copy;
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
        public final ImmutableMap<String, ImmutableList<String>> perDatasetMetric;
        public final long min;
        public final long max;
        public final long interval;

        public MetricRegroup(Map<String, List<String>> perDatasetMetric, long min, long max, long interval) {
            final ImmutableMap.Builder<String, ImmutableList<String>> copy = ImmutableMap.builder();
            perDatasetMetric.forEach((k,v) -> copy.put(k, ImmutableList.copyOf(v)));
            this.perDatasetMetric = copy.build();
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

    public static class IterateAndExplode {
        public final String field;
        public final List<AggregateMetric> selecting;
        public final Iterate.FieldIterateOpts fieldOpts;
        public final Optional<Pair<Integer, Iterate.FieldLimitingMechanism>> fieldLimits;
        public final Optional<String> explodeDefaultName;

        public IterateAndExplode(String field, List<AggregateMetric> selecting, Iterate.FieldIterateOpts fieldOpts, Optional<Pair<Integer, Iterate.FieldLimitingMechanism>> fieldLimits, Optional<String> explodeDefaultName) {
            this.field = field;
            this.selecting = selecting;
            this.fieldOpts = fieldOpts;
            this.fieldLimits = fieldLimits;
            this.explodeDefaultName = explodeDefaultName;
        }
    }

    public static class ComputeAndCreateGroupStatsLookup {
        public final Object computation;
        public final Optional<String> name;

        public ComputeAndCreateGroupStatsLookup(Object computation, Optional<String> name) {
            this.computation = computation;
            this.name = name;
        }
    }

    public static class ExplodeByAggregatePercentile {
        public final String field;
        public final AggregateMetric metric;
        public final int numBuckets;

        public ExplodeByAggregatePercentile(String field, AggregateMetric metric, int numBuckets) {
            this.field = field;
            this.metric = metric;
            this.numBuckets = numBuckets;
        }
    }

    public static class ExplodePerDocPercentile {
        public final String field;
        public final int numBuckets;

        public ExplodePerDocPercentile(String field, int numBuckets) {
            this.field = field;
            this.numBuckets = numBuckets;
        }
    }
}
