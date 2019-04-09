/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.iql2.language.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.indeed.iql.metadata.DatasetsMetadata;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.actions.Action;
import com.indeed.iql2.language.commands.ApplyFilterActions;
import com.indeed.iql2.language.commands.ApplyGroupFilter;
import com.indeed.iql2.language.commands.Command;
import com.indeed.iql2.language.commands.ComputeAndCreateGroupStatsLookup;
import com.indeed.iql2.language.commands.ComputeAndCreateGroupStatsLookups;
import com.indeed.iql2.language.commands.FieldIterateOpts;
import com.indeed.iql2.language.commands.GroupByFieldinPlaceholderCommand;
import com.indeed.iql2.language.commands.IterateAndExplode;
import com.indeed.iql2.language.commands.MetricRegroup;
import com.indeed.iql2.language.commands.RandomMetricRegroup;
import com.indeed.iql2.language.commands.RegroupFieldIn;
import com.indeed.iql2.language.commands.SimpleIterate;
import com.indeed.iql2.language.commands.TimePeriodRegroup;
import com.indeed.iql2.language.commands.TopK;
import com.indeed.iql2.language.precomputed.Precomputed;
import com.indeed.iql2.language.query.Dataset;
import com.indeed.iql2.language.query.Query;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.util.core.Pair;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLists;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public interface ExecutionStep {

    List<Command> commands();
    // TODO: this is never used
    ExecutionStep traverse1(Function<AggregateMetric, AggregateMetric> f);

    @EqualsAndHashCode
    @ToString
    class ComputePrecomputed implements ExecutionStep {
        public final List<Dataset> datasets;
        public final Precomputed computation;
        public final String name;

        public ComputePrecomputed(final List<Dataset> datasets, final Precomputed computation, final String name) {
            this.datasets = datasets;
            this.computation = computation;
            this.name = name;
        }

        @Override
        public List<Command> commands() {
            final Precomputed.Precomputation precomputation = computation.commands(datasets);
            final List<Command> result = new ArrayList<>();
            result.addAll(precomputation.beforeCommands);
            result.add(new ComputeAndCreateGroupStatsLookup(precomputation.computationCommand, name));
            result.addAll(precomputation.afterCommands);
            return result;
        }

        @Override
        public ExecutionStep traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new ComputePrecomputed(datasets, computation.traverse1(f), name);
        }
    }

    @EqualsAndHashCode
    @ToString
    class ComputeManyPrecomputed implements ExecutionStep {
        public final List<Dataset> datasets;
        public final List<Pair<Precomputed, String>> computations;

        public ComputeManyPrecomputed(final List<Dataset> datasets, final List<Pair<Precomputed, String>> computations) {
            this.datasets = datasets;
            this.computations = computations;
        }

        @Override
        public List<Command> commands() {
            final List<Pair<Command, String>> precomputeds = new ArrayList<>();

            for (final Pair<Precomputed, String> computation : computations) {
                final Precomputed.Precomputation precomputation = computation.getFirst().commands(datasets);
                if (!precomputation.afterCommands.isEmpty() || !precomputation.beforeCommands.isEmpty()) {
                    return naiveExecutionCommands();
                }
                precomputeds.add(Pair.of(precomputation.computationCommand, computation.getSecond()));
            }

            return Collections.singletonList(new ComputeAndCreateGroupStatsLookups(precomputeds));
        }

        private List<Command> naiveExecutionCommands() {
            final List<Command> commands = new ArrayList<>();
            for (final Pair<Precomputed, String> computation : computations) {
                commands.addAll(new ComputePrecomputed(datasets, computation.getFirst(), computation.getSecond()).commands());
            }
            return commands;
        }

        @Override
        public ExecutionStep traverse1(Function<AggregateMetric, AggregateMetric> f) {
            final List<Pair<Precomputed, String>> computations = new ArrayList<>();
            for (final Pair<Precomputed, String> computation : this.computations) {
                computations.add(Pair.of(computation.getFirst().traverse1(f), computation.getSecond()));
            }
            return new ComputeManyPrecomputed(datasets, computations);
        }
    }

    @EqualsAndHashCode
    @ToString
    class ExplodeAndRegroup implements ExecutionStep {
        public final FieldSet field;
        public final Optional<AggregateFilter> filter;
        public final Optional<TopK> topK;
        public final boolean withDefault;

        public ExplodeAndRegroup(final FieldSet field, final Optional<AggregateFilter> filter, final Optional<TopK> topK, final boolean withDefault) {
            this.field = field;
            this.filter = filter;
            this.topK = topK;
            this.withDefault = withDefault;
        }

        @Override
        public List<Command> commands() {
            final FieldIterateOpts opts = new FieldIterateOpts();
            if (filter.isPresent()) {
                opts.filter = Optional.of(filter.get());
            }
            opts.topK = topK;
            final Optional<String> withDefaultName;
            if (withDefault) {
                withDefaultName = Optional.of("DEFAULT");
            } else {
                withDefaultName = Optional.empty();
            }
            final Command command = new IterateAndExplode(field, opts, withDefaultName);
            return Collections.singletonList(command);
        }

        @Override
        public ExecutionStep traverse1(Function<AggregateMetric, AggregateMetric> f) {
            final Optional<AggregateFilter> filter;
            if (this.filter.isPresent()) {
                filter = Optional.of(this.filter.get().traverse1(f));
            } else {
                filter = Optional.empty();
            }
            final Optional<TopK> topK = this.topK.map(x -> x.traverse1(f));
            return new ExplodeAndRegroup(field, filter, topK, withDefault);
        }
    }

    @EqualsAndHashCode
    @ToString
    class ExplodeFieldIn implements ExecutionStep {
        public final FieldSet field;
        public final List<String> stringTerms;
        public final LongList intTerms;
        public final boolean isIntField;
        public final boolean withDefault;

        public ExplodeFieldIn(final FieldSet field, final List<String> stringTerms, final LongList intTerms, final boolean isIntField, final boolean withDefault) {
            this.field = field;
            this.stringTerms = stringTerms;
            this.intTerms = intTerms;
            this.isIntField = isIntField;
            this.withDefault = withDefault;
        }

        public static ExplodeFieldIn intExplode(FieldSet field, LongList terms, boolean withDefault) {
            return new ExplodeFieldIn(field, Collections.emptyList(), terms, true, withDefault);
        }

        public static ExplodeFieldIn stringExplode(FieldSet field, List<String> terms, boolean withDefault) {
            return new ExplodeFieldIn(field, terms, LongLists.EMPTY_LIST, false, withDefault);
        }

        @Override
        public List<Command> commands() {
            return Collections.singletonList(new RegroupFieldIn(field, stringTerms, intTerms, isIntField, withDefault));
        }

        @Override
        public ExecutionStep traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }
    }

    @EqualsAndHashCode
    @ToString
    class ExplodeMetric implements ExecutionStep {
        private final Map<String, DocMetric> perDatasetMetric;
        private final long lowerBound;
        private final long upperBound;
        private final long interval;
        private final Set<String> scope;
        private final boolean excludeGutters;
        private final boolean withDefault;
        private final boolean fromPredicate;

        public ExplodeMetric(final Map<String, DocMetric> perDatasetMetric, final long lowerBound, final long upperBound, final long interval, final Set<String> scope, final boolean excludeGutters, final boolean withDefault, final boolean fromPredicate) {
            this.perDatasetMetric = perDatasetMetric;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.interval = interval;
            this.scope = scope;
            this.excludeGutters = excludeGutters;
            this.withDefault = withDefault;
            this.fromPredicate = fromPredicate;
        }

        @Override
        public List<Command> commands() {
            return Collections.singletonList(new MetricRegroup(ImmutableMap.copyOf(Maps.filterKeys(perDatasetMetric, scope::contains)), lowerBound, upperBound, interval, excludeGutters, withDefault, fromPredicate));
        }

        @Override
        public ExecutionStep traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }
    }

    @EqualsAndHashCode
    @ToString
    class ExplodeTimePeriod implements ExecutionStep {
        private final long periodMillis;
        private final Optional<FieldSet> timeField;
        private final Optional<String> timeFormat;
        private final boolean isRelative;

        public ExplodeTimePeriod(final long periodMillis, final Optional<FieldSet> timeField, final Optional<String> timeFormat, final boolean isRelative) {
            this.periodMillis = periodMillis;
            this.timeField = timeField;
            this.timeFormat = timeFormat;
            this.isRelative = isRelative;
        }

        @Override
        public List<Command> commands() {
            return Collections.singletonList(new TimePeriodRegroup(periodMillis, timeField, timeFormat, isRelative));
        }

        @Override
        public ExecutionStep traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }
    }

    class ExplodeDayOfWeek implements ExecutionStep {
        @Override
        public List<Command> commands() {
            return Collections.singletonList(new com.indeed.iql2.language.commands.ExplodeDayOfWeek());
        }

        @Override
        public ExecutionStep traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public String toString() {
            return "ExplodeDayOfWeek{}";
        }
    }

    @EqualsAndHashCode
    @ToString
    class ExplodeMonthOfYear implements ExecutionStep {
        private final Optional<FieldSet> timeField;
        private final Optional<String> timeFormat;

        public ExplodeMonthOfYear(final Optional<FieldSet> timeField, final Optional<String> timeFormat) {
            this.timeField = timeField;
            this.timeFormat = timeFormat;
        }

        @Override
        public List<Command> commands() {
            return Collections.singletonList(new com.indeed.iql2.language.commands.ExplodeMonthOfYear(timeField, timeFormat));
        }

        @Override
        public ExecutionStep traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }
    }

    class ExplodeSessionNames implements ExecutionStep {
        @Override
        public List<Command> commands() {
            return Collections.singletonList(new com.indeed.iql2.language.commands.ExplodeSessionNames());
        }

        @Override
        public ExecutionStep traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }

        @Override
        public String toString() {
            return "ExplodeSessionNames{}";
        }
    }

    @EqualsAndHashCode
    @ToString
    class IterateStats implements ExecutionStep {
        private final FieldSet field;
        private final Optional<AggregateFilter> filter;
        private final Optional<Integer> queryLimit;
        private final Optional<TopK> topK;

        private final Optional<Set<String>> stringTermSubset;
        private final Optional<Set<Long>> intTermSubset;

        private final List<AggregateMetric> stats;
        private final List<Optional<String>> formatStrings;

        public IterateStats(final FieldSet field, final Optional<AggregateFilter> filter, final Optional<Integer> queryLimit, final Optional<TopK> topK, final Optional<Set<String>> stringTermSubset, final Optional<Set<Long>> intTermSubset, final List<AggregateMetric> stats, final List<Optional<String>> formatStrings) {
            this.field = field;
            this.filter = filter;
            this.topK = topK;
            this.queryLimit = queryLimit;
            this.stringTermSubset = stringTermSubset;
            this.intTermSubset = intTermSubset;
            this.stats = stats;
            this.formatStrings = formatStrings;
        }

        @Override
        public List<Command> commands() {
            final FieldIterateOpts opts = new FieldIterateOpts();
            if (topK.isPresent()) {
                opts.topK = topK;
            }
            opts.limit = queryLimit;
            opts.filter = filter;
            opts.intTermSubset = intTermSubset;
            opts.stringTermSubset = stringTermSubset;
            final SimpleIterate simpleIterate = new SimpleIterate(field, opts, stats, formatStrings);
            return Collections.singletonList(simpleIterate);
        }

        @Override
        public ExecutionStep traverse1(Function<AggregateMetric, AggregateMetric> f) {
            final Optional<AggregateFilter> filter;
            if (this.filter.isPresent()) {
                filter = Optional.of(this.filter.get().traverse1(f));
            } else {
                filter = Optional.empty();
            }
            final Optional<TopK> topK = this.topK.map(x -> x.traverse1(f));
            final List<AggregateMetric> stats = new ArrayList<>();
            for (final AggregateMetric stat : this.stats) {
                stats.add(f.apply(stat));
            }
            return new IterateStats(field, filter, queryLimit, topK, stringTermSubset, intTermSubset, stats, formatStrings);
        }
    }

    @EqualsAndHashCode
    @ToString
    class GetGroupStats implements ExecutionStep {
        public final List<AggregateMetric> stats;
        public final List<Optional<String>> formatStrings;

        public GetGroupStats(final List<AggregateMetric> stats, final List<Optional<String>> formatStrings) {
            this.stats = stats;
            this.formatStrings = formatStrings;
        }

        @Override
        public List<Command> commands() {
            return Collections.singletonList(new com.indeed.iql2.language.commands.GetGroupStats(stats, formatStrings, true));
        }

        @Override
        public ExecutionStep traverse1(Function<AggregateMetric, AggregateMetric> f) {
            final List<AggregateMetric> stats = new ArrayList<>();
            for (final AggregateMetric stat : this.stats) {
                stats.add(f.apply(stat));
            }
            return new GetGroupStats(stats, formatStrings);
        }
    }

    @EqualsAndHashCode
    @ToString
    class ExplodePerDocPercentile implements ExecutionStep {
        private final FieldSet field;
        private final int numBuckets;

        public ExplodePerDocPercentile(final FieldSet field, final int numBuckets) {
            this.field = field;
            this.numBuckets = numBuckets;
        }

        @Override
        public List<Command> commands() {
            return Collections.singletonList(new com.indeed.iql2.language.commands.ExplodePerDocPercentile(field, numBuckets));
        }

        @Override
        public ExecutionStep traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }
    }

    @EqualsAndHashCode
    @ToString
    class FilterActions implements ExecutionStep {
        private final ImmutableList<Action> actions;

        public FilterActions(final ImmutableList<Action> actions) {
            this.actions = actions;
        }

        @Override
        public List<Command> commands() {
            return Collections.singletonList(new ApplyFilterActions(actions));
        }

        @Override
        public ExecutionStep traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }
    }

    @EqualsAndHashCode
    @ToString
    class FilterGroups implements ExecutionStep {
        private final AggregateFilter filter;

        public FilterGroups(final AggregateFilter filter) {
            this.filter = filter;
        }

        @Override
        public List<Command> commands() {
            return Collections.singletonList(new ApplyGroupFilter(filter));
        }

        @Override
        public ExecutionStep traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return new FilterGroups(filter.traverse1(f));
        }
    }

    @EqualsAndHashCode
    @ToString
    class ExplodeRandom implements ExecutionStep {
        private final FieldSet field;
        private final int k;
        private final String salt;

        public ExplodeRandom(final FieldSet field, final int k, final String salt) {
            this.field = field;
            this.k = k;
            this.salt = salt;
        }

        @Override
        public List<Command> commands() {
            return Collections.singletonList(new com.indeed.iql2.language.commands.ExplodeRandom(field, k, salt));
        }

        @Override
        public ExecutionStep traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }
    }

    @EqualsAndHashCode
    @ToString
    class ExplodeRandomMetric implements ExecutionStep {
        private final Map<String, DocMetric> perDatasetMetric;
        private final Set<String> scope;
        private final int k;
        private final String salt;

        public ExplodeRandomMetric(final Map<String, DocMetric> perDatasetMetric, final Set<String> scope, final int k, final String salt) {
            this.perDatasetMetric = perDatasetMetric;
            this.scope = scope;
            this.k = k;
            this.salt = salt;
        }

        @Override
        public List<Command> commands() {
            return Collections.singletonList(new RandomMetricRegroup(ImmutableMap.copyOf(Maps.filterKeys(perDatasetMetric, scope::contains)), k, salt));
        }

        @Override
        public ExecutionStep traverse1(Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }
    }

    /**
     * Exists so that we can transform a Query with subqueries into a
     * List&lt;Command&gt; in order to validate it.
     */
    @EqualsAndHashCode
    @ToString
    class GroupByFieldInQueryPlaceholderExecutionStep implements ExecutionStep {
        private final FieldSet field;
        private final Query query;
        private final boolean isNegated;
        private final boolean withDefault;
        @ToString.Exclude
        @EqualsAndHashCode.Exclude
        private final DatasetsMetadata datasetsMetadata;

        public GroupByFieldInQueryPlaceholderExecutionStep(final FieldSet field, final Query query, final boolean isNegated, final boolean withDefault, final DatasetsMetadata datasetsMetadata) {
            this.field = field;
            this.query = query;
            this.isNegated = isNegated;
            this.withDefault = withDefault;
            this.datasetsMetadata = datasetsMetadata;
        }

        @Override
        public List<Command> commands() {
            return Collections.singletonList(new GroupByFieldinPlaceholderCommand(field, query, isNegated, withDefault, datasetsMetadata));
        }

        @Override
        public ExecutionStep traverse1(final Function<AggregateMetric, AggregateMetric> f) {
            return this;
        }
    }
}
