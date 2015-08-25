package com.indeed.squall.iql2.execution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.common.util.Pair;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.group.ImhotepChooser;
import com.indeed.squall.iql2.execution.actions.Action;
import com.indeed.squall.iql2.execution.actions.IntOrAction;
import com.indeed.squall.iql2.execution.actions.MetricAction;
import com.indeed.squall.iql2.execution.actions.QueryAction;
import com.indeed.squall.iql2.execution.actions.RegexAction;
import com.indeed.squall.iql2.execution.actions.SampleAction;
import com.indeed.squall.iql2.execution.actions.StringOrAction;
import com.indeed.squall.iql2.execution.actions.UnconditionalAction;
import com.indeed.squall.iql2.execution.commands.ApplyFilterActions;
import com.indeed.squall.iql2.execution.commands.Command;
import com.indeed.squall.iql2.execution.commands.ComputeAndCreateGroupStatsLookup;
import com.indeed.squall.iql2.execution.commands.ComputeAndCreateGroupStatsLookups;
import com.indeed.squall.iql2.execution.commands.CreateGroupStatsLookup;
import com.indeed.squall.iql2.execution.commands.ExplodeByAggregatePercentile;
import com.indeed.squall.iql2.execution.commands.ExplodeDayOfWeek;
import com.indeed.squall.iql2.execution.commands.ExplodeMonthOfYear;
import com.indeed.squall.iql2.execution.commands.ExplodePerDocPercentile;
import com.indeed.squall.iql2.execution.commands.ExplodePerGroup;
import com.indeed.squall.iql2.execution.commands.ExplodeSessionNames;
import com.indeed.squall.iql2.execution.commands.ExplodeTimeBuckets;
import com.indeed.squall.iql2.execution.commands.FilterDocs;
import com.indeed.squall.iql2.execution.commands.GetFieldMax;
import com.indeed.squall.iql2.execution.commands.GetFieldMin;
import com.indeed.squall.iql2.execution.commands.GetGroupDistincts;
import com.indeed.squall.iql2.execution.commands.GetGroupPercentiles;
import com.indeed.squall.iql2.execution.commands.GetGroupStats;
import com.indeed.squall.iql2.execution.commands.GetNumGroups;
import com.indeed.squall.iql2.execution.commands.IterateAndExplode;
import com.indeed.squall.iql2.execution.commands.MetricRegroup;
import com.indeed.squall.iql2.execution.commands.RegroupIntoLastSiblingWhere;
import com.indeed.squall.iql2.execution.commands.RegroupIntoParent;
import com.indeed.squall.iql2.execution.commands.SampleFields;
import com.indeed.squall.iql2.execution.commands.SimpleIterate;
import com.indeed.squall.iql2.execution.commands.SumAcross;
import com.indeed.squall.iql2.execution.commands.TimePeriodRegroup;
import com.indeed.squall.iql2.execution.commands.misc.FieldIterateOpts;
import com.indeed.squall.iql2.execution.commands.misc.FieldLimitingMechanism;
import com.indeed.squall.iql2.execution.compat.Consumer;
import com.indeed.squall.iql2.execution.metrics.aggregate.AggregateMetric;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.DurationFieldType;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.Seconds;
import org.joda.time.Weeks;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author jwolfe
 */

public class SimpleSession {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private Grouping groups;
    private final DateTime start;
    private final DateTime end;

    public SimpleSession(final List<Document> documents, DateTime start, DateTime end) {
        this.start = start;
        this.end = end;
        this.groups = Grouping.from(documents);
    }

    public void handleCommand(final Command command, final Consumer<String> out) throws IOException {
        if (command instanceof SimpleIterate) {
            final SimpleIterate simpleIterate = (SimpleIterate) command;
            this.simpleIterate(simpleIterate.field, simpleIterate.opts, simpleIterate.selecting, simpleIterate.streamResult, new Consumer<IterateFTGS>() {
                @Override
                public void accept(IterateFTGS iterateFTGS) {
                    // TODO: Somehow output the FTGS to the output
                    throw new UnsupportedOperationException();
                }
            });
        } else if (command instanceof FilterDocs) {
            final FilterDocs filterDocs = (FilterDocs) command;
            this.filterDocs(filterDocs.perDatasetFilterMetric);
        } else if (command instanceof GetGroupStats) {
            final GetGroupStats getGroupStats = (GetGroupStats) command;
            this.getGroupStats(getGroupStats.metrics, getGroupStats.returnGroupKeys, out);
        } else if (command instanceof CreateGroupStatsLookup) {
            final CreateGroupStatsLookup createGroupStatsLookup = (CreateGroupStatsLookup) command;
            this.createGroupStatsLookup(createGroupStatsLookup.stats, createGroupStatsLookup.name);
        } else if (command instanceof GetGroupDistincts) {
            final GetGroupDistincts getGroupDistincts = (GetGroupDistincts) command;
            this.getGroupDistincts(getGroupDistincts.scope, getGroupDistincts.field, getGroupDistincts.filter, getGroupDistincts.windowSize, out);
        } else if (command instanceof GetGroupPercentiles) {
            final GetGroupPercentiles getGroupPercentiles = (GetGroupPercentiles) command;
            this.getGroupPercentiles(getGroupPercentiles.scope, getGroupPercentiles.field, getGroupPercentiles.percentiles, out);
        } else if (command instanceof MetricRegroup) {
            final MetricRegroup metricRegroup = (MetricRegroup) command;
            this.metricRegroup(metricRegroup.perDatasetMetric, metricRegroup.min, metricRegroup.max, metricRegroup.interval, metricRegroup.excludeGutters);
        } else if (command instanceof GetNumGroups) {
            out.accept(String.valueOf(groups.getNumGroups()));
        } else if (command instanceof ExplodePerGroup) {
            final ExplodePerGroup explodePerGroup = (ExplodePerGroup) command;
            this.explodePerGroup(explodePerGroup.termsWithExplodeOpts);
        } else if (command instanceof ExplodeDayOfWeek) {
            this.explodeDayOfWeek();
        } else if (command instanceof ExplodeSessionNames) {
            throw new UnsupportedOperationException();
        } else if (command instanceof IterateAndExplode) {
            final IterateAndExplode iterateAndExplode = (IterateAndExplode) command;
            this.iterateAndExplode(iterateAndExplode.field, iterateAndExplode.selecting, iterateAndExplode.fieldOpts, iterateAndExplode.explodeDefaultName);
        } else if (command instanceof ComputeAndCreateGroupStatsLookup) {
            throw new UnsupportedOperationException();
        } else if (command instanceof ComputeAndCreateGroupStatsLookups) {
            throw new UnsupportedOperationException();
        } else if (command instanceof ExplodeByAggregatePercentile) {
            throw new UnsupportedOperationException();
        } else if (command instanceof ExplodePerDocPercentile) {
            final ExplodePerDocPercentile explodePerDocPercentile = (ExplodePerDocPercentile) command;
            this.explodePerDocPercentile(explodePerDocPercentile.field, explodePerDocPercentile.numBuckets);
        } else if (command instanceof SumAcross) {
            final SumAcross sumAcross = (SumAcross) command;
            this.sumAcross(sumAcross.scope, sumAcross.field, sumAcross.metric, sumAcross.filter, out);
        } else if (command instanceof RegroupIntoParent) {
            final RegroupIntoParent regroupIntoParent = (RegroupIntoParent) command;
            this.regroupIntoParent(regroupIntoParent.mergeType);
        } else if (command instanceof RegroupIntoLastSiblingWhere) {
            final RegroupIntoLastSiblingWhere regroupIntoLastSiblingWhere = (RegroupIntoLastSiblingWhere) command;
            this.regroupIntoLastSiblingWhere(regroupIntoLastSiblingWhere.filter, regroupIntoLastSiblingWhere.mergeType);
        } else if (command instanceof ExplodeMonthOfYear) {
            this.explodeMonthOfYear();
        } else if (command instanceof ExplodeTimeBuckets) {
            final ExplodeTimeBuckets explodeTimeBuckets = (ExplodeTimeBuckets) command;
            this.explodeTimeBuckets(explodeTimeBuckets.numBuckets, explodeTimeBuckets.timeField, explodeTimeBuckets.timeFormat);
        } else if (command instanceof TimePeriodRegroup) {
            final TimePeriodRegroup timePeriodRegroup = (TimePeriodRegroup) command;
            this.timePeriodRegroup(timePeriodRegroup.periodMillis, timePeriodRegroup.timeField, timePeriodRegroup.timeFormat);
            out.accept("TimePeriodRegrouped");
        } else if (command instanceof SampleFields) {
            final SampleFields sampleFields = (SampleFields) command;
            this.sampleFields(sampleFields.perDatasetSamples);
        } else if (command instanceof ApplyFilterActions) {
            final ApplyFilterActions applyFilterActions = (ApplyFilterActions) command;
            for (final Action action : applyFilterActions.actions) {
                this.applyAction(action);
            }
            out.accept("Applied filters");
        } else if (command instanceof GetFieldMax) {
            final GetFieldMax getFieldMax = (GetFieldMax) command;
            this.getFieldMax(getFieldMax.scope, getFieldMax.field, out);
        } else if (command instanceof GetFieldMin) {
            final GetFieldMin getFieldMin = (GetFieldMin) command;
            this.getFieldMin(getFieldMin.scope, getFieldMin.field, out);
        } else {
            throw new UnsupportedOperationException("Unhandled: " + command);
        }
    }

    private void applyAction(final Action action) {
        final Predicate<Document> predicate;
        final int target, positive, negative;
        final Set<String> scope;
        if (action instanceof IntOrAction) {
            final IntOrAction intOrAction = (IntOrAction) action;
            target = intOrAction.targetGroup;
            positive = intOrAction.positiveGroup;
            negative = intOrAction.negativeGroup;
            scope = intOrAction.scope;
            predicate = new Predicate<Document>() {
                public boolean apply(Document input) {
                    for (final long v : input.getIntField(intOrAction.field)) {
                        if (intOrAction.terms.contains(v)) {
                            return true;
                        }
                    }
                    return false;
                }
            };
        } else if (action instanceof MetricAction) {
            final MetricAction metricAction = (MetricAction) action;
            target = metricAction.targetGroup;
            positive = metricAction.positiveGroup;
            negative = metricAction.negativeGroup;
            scope = metricAction.scope;
            predicate = new Predicate<Document>() {
                @Override
                public boolean apply(Document input) {
                    final List<String> metric = metricAction.perDatasetPushes.get(input.dataset);
                    final long value = input.computeMetric(metric);
                    return value == 1;
                }
            };
        } else if (action instanceof QueryAction) {
            final QueryAction queryAction = (QueryAction) action;
            target = queryAction.targetGroup;
            positive = queryAction.positiveGroup;
            negative = queryAction.negativeGroup;
            scope = queryAction.scope;
            predicate = new Predicate<Document>() {
                @Override
                public boolean apply(Document input) {
                    return input.queryMatches(queryAction.perDatasetQuery.get(input.dataset));
                }
            };
        } else if (action instanceof RegexAction) {
            final RegexAction regexAction = (RegexAction) action;
            target = regexAction.targetGroup;
            positive = regexAction.positiveGroup;
            negative = regexAction.negativeGroup;
            scope = regexAction.scope;
            final Pattern pattern = Pattern.compile(regexAction.regex);
            predicate = new Predicate<Document>() {
                @Override
                public boolean apply(Document input) {
                    for (final String s : input.getStringField(regexAction.field)) {
                        if (pattern.matcher(s).matches()) {
                            return true;
                        }
                    }
                    for (final long v : input.getIntField(regexAction.field)) {
                        if (pattern.matcher(String.valueOf(v)).matches()) {
                            return true;
                        }
                    }
                    return false;
                }
            };
        } else if (action instanceof SampleAction) {
            final SampleAction sampleAction = (SampleAction) action;
            target = sampleAction.targetGroup;
            positive = sampleAction.positiveGroup;
            negative = sampleAction.negativeGroup;
            scope = sampleAction.scope;
            final ImhotepChooser chooser = new ImhotepChooser(sampleAction.seed, 1.0 - sampleAction.probability);
            predicate = new Predicate<Document>() {
                @Override
                public boolean apply(Document input) {
                    for (final long v : input.getIntField(sampleAction.field)) {
                        if (!chooser.choose(String.valueOf(v))) {
                            return false;
                        }
                    }
                    for (final String s : input.getStringField(sampleAction.field)) {
                        if (!chooser.choose(s)) {
                            return false;
                        }
                    }
                    return true;
                }
            };
        } else if (action instanceof StringOrAction) {
            final StringOrAction stringOrAction = (StringOrAction) action;
            target = stringOrAction.targetGroup;
            positive = stringOrAction.positiveGroup;
            negative = stringOrAction.negativeGroup;
            scope = stringOrAction.scope;
            predicate = new Predicate<Document>() {
                public boolean apply(Document input) {
                    for (final String v : input.getStringField(stringOrAction.field)) {
                        if (stringOrAction.terms.contains(v)) {
                            return true;
                        }
                    }
                    return false;
                }
            };
        } else if (action instanceof UnconditionalAction) {
            final UnconditionalAction unconditionalAction = (UnconditionalAction) action;
            target = unconditionalAction.targetGroup;
            positive = unconditionalAction.newGroup;
            negative = 0;
            scope = unconditionalAction.scope;
            predicate = Predicates.alwaysTrue();
        } else {
            throw new UnsupportedOperationException("Unhandled: " + action);
        }
        groups = groups.createNew(new Grouping.GroupingCallback() {
            @Override
            public void handle(int group, Document document) {
                if (group != target || !scope.contains(document.dataset)) {
                    addToGroup(group);
                    return;
                }
                if (predicate.apply(document)) {
                    addToGroup(positive);
                } else {
                    addToGroup(negative);
                }
            }
        });
    }

    private void filterDocs(final Map<String, List<String>> perDatasetFilterMetric) {
        groups = groups.createNew(new Grouping.GroupingCallback() {
            @Override
            public void handle(int group, Document document) {
                final List<String> metric = perDatasetFilterMetric.get(document.dataset);
                final long value = document.computeMetric(metric);
                if (value == 1) {
                    addToGroup(group);
                } else if (value == 0) {
                    discard();
                } else {
                    throw new IllegalStateException("0-1 metric was not 0-1 metric! metric = [" + metric + "], value = [" + value + "]");
                }
            }
        });
    }

    private void getGroupStats(final List<AggregateMetric> metrics, final boolean returnGroupKeys, Consumer<String> out) throws JsonProcessingException {
        if (returnGroupKeys) {
            throw new UnsupportedOperationException("Don't yet support group keys");
        }
        final double[][] selects = groups.select(Predicates.<Document>alwaysTrue(), metrics);
        final List<Session.GroupStats> groupStatses = Lists.newArrayListWithCapacity(groups.getNumGroups());
        for (int i = 1; i <= groups.getNumGroups(); i++) {
            final double[] stats = new double[metrics.size()];
            for (int j = 0; j < metrics.size(); j++) {
                stats[j] = selects[j][i];
            }
            groupStatses.add(new Session.GroupStats(null, stats));
        }
        out.accept(OBJECT_MAPPER.writeValueAsString(groupStatses));
    }

    private void createGroupStatsLookup(final double[] stats, final Optional<String> name) {
        groups.createGroupStatsLookup(stats, name);
    }

    private void getGroupDistincts(final Set<String> scope, final String field, final Optional<AggregateFilter> filter, final int windowSize, final Consumer<String> out) throws JsonProcessingException {
        final Map<Integer, Set<String>> groupDistincts = new HashMap<>();
        groups.process(new Grouping.ProcessCallback() {
            @Override
            public void handle(final int group, final Document document) {
                if (!scope.contains(document.dataset)) {
                    return;
                }

                if (!groupDistincts.containsKey(group)) {
                    groupDistincts.put(group, new HashSet<String>());
                }

                final Set<String> distincts = groupDistincts.get(group);
                distincts.addAll(document.getStringField(field));
                for (final long term : document.getIntField(field)) {
                    distincts.add(String.valueOf(term));
                }
            }
        });
        final long[] distincts = new long[groups.getNumGroups()];
        for (int i = 1; i <= distincts.length; i++) {
            if (groupDistincts.containsKey(i)) {
                distincts[i - 1] = groupDistincts.get(i).size();
            }
        }
        out.accept(OBJECT_MAPPER.writeValueAsString(distincts));
    }

    private static class IterateFTGS {
        public final com.indeed.flamdex.query.Term term;
        public final int group;
        public final double sortMetric;
        public final double[] selects;

        private IterateFTGS(final Term term, final int group, final double sortMetric, final double[] selects) {
            this.term = term;
            this.group = group;
            this.sortMetric = sortMetric;
            this.selects = selects;
        }
    }

    private void simpleIterate(final String field, final FieldIterateOpts opts, final List<AggregateMetric> selecting, final boolean streamResult, Consumer<IterateFTGS> out) {
        if (streamResult && opts.topK.isPresent()) {
            throw new IllegalArgumentException("Cannot have streamResult and a topK");
        }

        final List<Term> terms = groups.getFieldTerms(field);
        final int numGroups = groups.getNumGroups();

        final List<AggregateMetric> actualSelecting = new ArrayList<>();
        if (opts.topK.isPresent()) {
            actualSelecting.add(opts.topK.get().metric);
        }

        final List<IterateFTGS> unsortedResults = new ArrayList<>();
        for (final Term term : terms) {
            final Predicate<Document> hasTerm = new Predicate<Document>() {
                @Override
                public boolean apply(final Document input) {
                    return input.hasTerm(term);
                }
            };
            final double[][] selects = groups.select(hasTerm, actualSelecting);
            final boolean[] applicable;
            if (opts.filter.isPresent()) {
                applicable = groups.selectFilter(hasTerm, opts.filter.get());
            } else {
                applicable = new boolean[groups.getNumGroups() + 1];
                Arrays.fill(applicable, true);
                applicable[0] = false;
            }

            for (int group = 1; group <= numGroups; group++) {
                if (applicable[group]) {
                    final double[] ftgsSelects = new double[selecting.size()];
                    final int start = opts.topK.isPresent() ? 1 : 0;
                    for (int i = start; i < selects.length; i++) {
                        ftgsSelects[i] = selects[start + i][group];
                    }
                    final double sortMetric;
                    if (opts.topK.isPresent()) {
                        sortMetric = selects[0][group];
                    } else {
                        sortMetric = 0.0;
                    }
                    unsortedResults.add(new IterateFTGS(term, group, sortMetric, ftgsSelects));
                }
            }
        }

        Collections.sort(unsortedResults, new Comparator<IterateFTGS>() {
            @Override
            public int compare(final IterateFTGS o1, final IterateFTGS o2) {
                if (streamResult) {
                    final int r = compareTerms(o1.term, o2.term);
                    if (r != 0) {
                        return r;
                    }
                    return Ints.compare(o1.group, o2.group);
                } else {
                    int r = Ints.compare(o1.group, o2.group);
                    if (r != 0) {
                        return r;
                    }
                    r = Doubles.compare(o1.sortMetric, o2.sortMetric);
                    if (r != 0) {
                        return r;
                    }
                    r = compareTerms(o1.term, o2.term);
                    if (r != 0) {
                        return r;
                    }
                    return 0;
                }
            }
        });

        if (opts.limit.isPresent()) {
            out = new LimitingOut<>(out, opts.limit.get());
        }

        for (final IterateFTGS result : unsortedResults) {
            out.accept(result);
        }
    }

    private static int compareTerms(final Term t1, final Term t2) {
        int r = Booleans.compare(t1.isIntField(), t2.isIntField());
        if (r != 0) {
            return r;
        }
        if (t1.isIntField()) {
            r = Longs.compare(t1.getTermIntVal(), t2.getTermIntVal());
        } else {
            r = t1.getTermStringVal().compareTo(t2.getTermStringVal());
        }
        return r;
    }

    private void getGroupPercentiles(final Set<String> scope, final String field, final double[] percentiles, final Consumer<String> out) throws JsonProcessingException {
        final LongArrayList values = new LongArrayList(); // Who needs non-naive solutions?
        groups.process(new Grouping.ProcessCallback() {
            @Override
            public void handle(final int group, final Document document) {
                if (scope.contains(document.dataset)) {
                    // TODO: What to do about multi-value..?
                    values.addAll(document.getIntField(field));
                }
            }
        });
        Collections.sort(values);
        final long[] result = new long[percentiles.length];
        for (int i = 0; i < percentiles.length; i++) {
            result[i] = values.getLong((int) Math.ceil(percentiles[i] * values.size()));
        }
        out.accept(OBJECT_MAPPER.writeValueAsString(result));
    }

    private void metricRegroup(final Map<String, ? extends List<String>> perDatasetMetric, final long min, final long max, final long interval, final boolean excludeGutters) {
        final int bucketsPerGroup = (excludeGutters ? 0 : 2) + (int) Math.ceil(((double) max - min) / interval);
        groups = groups.createNew(new Grouping.GroupingCallback() {
            @Override
            public void handle(final int group, final Document document) {
                final List<String> metric = perDatasetMetric.get(document.dataset);
                final long value = document.computeMetric(metric);
                if (excludeGutters && ((value < min) || (value > max))) {
                    discard();
                }
                final long offset = (value - min) / interval;
                addToGroup(1 + (bucketsPerGroup * (group - 1)) + (int) offset);
            }
        });
    }

    private void explodePerGroup(final List<Commands.TermsWithExplodeOpts> termsWithExplodeOpts) {
        final int[] groupStarts = new int[groups.getNumGroups() + 1];
        int group = 1;
        int groupStart = 1;
        for (final Commands.TermsWithExplodeOpts termsWithExplodeOpt : termsWithExplodeOpts) {
            groupStarts[group] = groupStart;
            group += 1;
            groupStart += termsWithExplodeOpt.terms.size() + (termsWithExplodeOpt.defaultName.isPresent() ? 1 : 0);
        }
        groups = groups.createNew(new Grouping.GroupingCallback() {
            @Override
            public void handle(int group, Document document) {
                final Commands.TermsWithExplodeOpts terms = termsWithExplodeOpts.get(group - 1);
                for (int i = 0; i < terms.terms.size(); i++) {
                    final Term term = terms.terms.get(i);
                    if (document.hasTerm(term)) {
                        addToGroup(groupStarts[group] + i);
                        return;
                    }
                }
                if (terms.defaultName.isPresent()) {
                    addToGroup(groupStarts[group] + terms.terms.size() + 1);
                } else {
                    discard();
                }
            }
        });
    }

    private void explodeDayOfWeek() {
        groups = groups.createNew(new Grouping.GroupingCallback() {
            @Override
            public void handle(int group, Document document) {
                final int intraGroup = new DateTime(document.timestamp).getDayOfWeek();
                addToGroup(1 + (group - 1) * 7 + intraGroup);
            }
        });
    }

    private void iterateAndExplode(final String field, final List<AggregateMetric> selecting, final FieldIterateOpts fieldOpts, final Optional<String> explodeDefaultName) {
        final Map<Integer, List<IterateFTGS>> perGroupIterates = new Int2ObjectOpenHashMap<>();
        this.simpleIterate(field, fieldOpts, selecting, false, new Consumer<IterateFTGS>() {
            @Override
            public void accept(IterateFTGS input) {
                if (!perGroupIterates.containsKey(input.group)) {
                    perGroupIterates.put(input.group, new ArrayList<IterateFTGS>());
                }
                perGroupIterates.get(input.group).add(input);
            }
        });
        final List<Commands.TermsWithExplodeOpts> explodes = new ArrayList<>();
        for (int i = 1; i <= groups.getNumGroups(); i++) {
            final List<IterateFTGS> iterateFTGSes = perGroupIterates.get(i);
            final List<Term> terms;
            if (iterateFTGSes == null) {
                terms = Collections.emptyList();
            } else {
                terms = new ArrayList<>();
                for (final IterateFTGS ftgs : iterateFTGSes) {
                    terms.add(ftgs.term);
                }
            }
            explodes.add(new Commands.TermsWithExplodeOpts(terms, explodeDefaultName));
        }
        this.explodePerGroup(explodes);
    }

    private void explodePerDocPercentile(final String field, final int numBuckets) {
        final Map<Integer, List<List<Document>>> foo = new HashMap<>();
        for (final Map.Entry<Integer, ImmutableList<Document>> grouping : groups.groupings.entrySet()) {
            final ArrayList<Document> sorted = Lists.newArrayList(grouping.getValue());
            Collections.sort(sorted, new Comparator<Document>() {
                @Override
                public int compare(Document o1, Document o2) {
                    return o1.getIntField(field).get(0).compareTo(o2.getIntField(field).get(0));
                }
            });
            foo.put(grouping.getKey(), Lists.partition(sorted, (int) Math.ceil((double)sorted.size() / numBuckets)));
        }
        final Map<Document, Integer> docToGroup = new HashMap<>();
        int nextGroup = 1;
        for (int i = 0; i <= groups.getNumGroups(); i++) {
            final List<List<Document>> lists = foo.get(i);
            if (lists != null) {
                for (final List<Document> list : lists) {
                    for (final Document doc : list) {
                        docToGroup.put(doc, nextGroup);
                    }
                    nextGroup += 1;
                }
            }
        }
        groups = groups.createNew(new Grouping.GroupingCallback() {
            @Override
            public void handle(int group, Document document) {
                addToGroup(docToGroup.get(document));
            }
        });
    }

    private void sumAcross(final Set<String> scope, final String field, final AggregateMetric metric, final Optional<AggregateFilter> filter, Consumer<String> out) throws JsonProcessingException {
        final List<Term> fieldTerms = groups.getFieldTerms(field);
        final double[] sums = new double[groups.getNumGroups() + 1];
        for (final Term term : fieldTerms) {
            final Predicate<Document> predicate = new Predicate<Document>() {
                public boolean apply(Document input) {
                    return scope.contains(input.dataset) && input.hasTerm(term);
                }
            };
            final boolean[] keeps = groups.selectFilter(predicate, filter.or(new AggregateFilter.Constant(true)));
            final double[] values = groups.select(predicate, Collections.singletonList(metric))[0];
            for (int i = 0; i < keeps.length; i++) {
                if (keeps[i]) {
                    sums[i] += values[i];
                }
            }
        }
        out.accept(OBJECT_MAPPER.writeValueAsString(Arrays.copyOfRange(sums, 1, sums.length)));
    }

    private void regroupIntoParent(final GroupLookupMergeType mergeType) {
        groups = groups.mergeIntoParent(mergeType);
    }

    private void regroupIntoLastSiblingWhere(final AggregateFilter filter, final GroupLookupMergeType mergeType) {
        groups = groups.regroupIntoLastSibling(filter, mergeType);
    }

    private void explodeMonthOfYear() {
        final DateTime startMonth = this.start.withDayOfMonth(0);
        final int numBuckets = (int) Math.ceil(((double)Months.monthsBetween(startMonth, end.withDayOfMonth(0)).getMonths() + 1) / (long) 1);
        groups = groups.createNew(new Grouping.GroupingCallback() {
            @Override
            public void handle(int group, Document document) {
                final int intraGroup = (int) ((long) Months.monthsBetween(startMonth, new DateTime(document.timestamp).withDayOfMonth(0)).getMonths());
                addToGroup(1 + (group - 1) * numBuckets + intraGroup);
            }
        });
    }

    private void explodeTimeBuckets(final int numBuckets, final Optional<String> timeField, final Optional<String> timeFormat) {
        final long earliestStart = start.getMillis();
        final long latestEnd = end.getMillis();
        final long bucketSize = (latestEnd - earliestStart) / numBuckets;
        timePeriodRegroup(bucketSize, timeField, timeFormat);
    }

    private void timePeriodRegroup(final long periodMillis, final Optional<String> timeField, final Optional<String> timeFormat) {
        final long delta = end.getMillis() - start.getMillis();
        final int numBuckets = (int) Math.ceil((double) delta / periodMillis);
        groups = groups.createNew(new Grouping.GroupingCallback() {
            @Override
            public void handle(int group, Document document) {
                final int intraGroup = (int) ((document.timestamp / 1000 - start.getMillis() / 1000) / (periodMillis / 1000));
                addToGroup(1 + (group - 1) * numBuckets + intraGroup);
            }
        });
    }

    private void sampleFields(final Map<String, List<SampleFields.SampleDefinition>> perDatasetDefinitions) {
        final Map<String, Predicate<Document>> perDatasetChooser = new HashMap<>();
        for (final Map.Entry<String, List<SampleFields.SampleDefinition>> entry : perDatasetDefinitions.entrySet()) {
            final List<Predicate<Document>> predicates = Lists.newArrayList();
            for (final SampleFields.SampleDefinition defn : entry.getValue()) {
                final ImhotepChooser chooser = new ImhotepChooser(defn.seed, 1.0 - defn.fraction);
                predicates.add(new Predicate<Document>() {
                    @Override
                    public boolean apply(Document input) {
                        for (final long v : input.getIntField(defn.field)) {
                            if (!chooser.choose(String.valueOf(v))) {
                                return false;
                            }
                        }
                        for (final String v : input.getStringField(defn.field)) {
                            if (!chooser.choose(v)) {
                                return false;
                            }
                        }
                        return true;
                    }
                });
            }
            perDatasetChooser.put(entry.getKey(), Predicates.and(predicates));
        }
        groups = groups.createNew(new Grouping.GroupingCallback() {
            @Override
            public void handle(int group, Document document) {
                if (perDatasetChooser.get(document.dataset).apply(document)) {
                    addToGroup(group);
                } else {
                    discard();
                }
            }
        });
        // TODO: Don't add a level to the groups tree?
    }

    private void getFieldMax(final Set<String> scope, final String field, Consumer<String> out) throws JsonProcessingException {
        final long[] results = new long[groups.getNumGroups()];
        Arrays.fill(results, Long.MIN_VALUE);
        groups.process(new Grouping.ProcessCallback() {
            @Override
            public void handle(int group, Document document) {
                if (scope.contains(document.dataset)) {
                    for (final long value : document.getIntField(field)) {
                        results[group] = Math.max(results[group], value);
                    }
                }
            }
        });
        out.accept(OBJECT_MAPPER.writeValueAsString(results));
    }

    private void getFieldMin(final Set<String> scope, final String field, Consumer<String> out) throws JsonProcessingException {
        final long[] results = new long[groups.getNumGroups()];
        Arrays.fill(results, Long.MAX_VALUE);
        groups.process(new Grouping.ProcessCallback() {
            @Override
            public void handle(int group, Document document) {
                if (scope.contains(document.dataset)) {
                    for (final long value : document.getIntField(field)) {
                        results[group] = Math.min(results[group], value);
                    }
                }
            }
        });
        out.accept(OBJECT_MAPPER.writeValueAsString(results));
    }
}
