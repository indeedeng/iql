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

package com.indeed.iql2.language.passes;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.indeed.iql.exceptions.IqlKnownException;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.FieldExtremeType;
import com.indeed.iql2.language.GroupByEntry;
import com.indeed.iql2.language.execution.ExecutionStep;
import com.indeed.iql2.language.precomputed.Precomputed;
import com.indeed.iql2.language.query.Dataset;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.query.Query;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class ExtractPrecomputed {
    private ExtractPrecomputed() {
    }

    public static Extracted extractPrecomputed(final Query query, final boolean extractTotals) {
        final Processor processor = new Processor(1, 1, query.extractDatasetNames());
        final List<GroupByEntry> groupBys = new ArrayList<>();
        for (int i = 0; i < query.groupBys.size(); i++) {
            final GroupByEntry groupBy = query.groupBys.get(i);
            processor.setDepth(i + 1);
            processor.setStartDepth(i + 1);
            processor.setMaxDepth(i + 1);
            final Optional<AggregateFilter> filter;
            final Optional<String> alias;
            if (groupBy.groupBy instanceof GroupBy.GroupByField) {
                filter = ((GroupBy.GroupByField) groupBy.groupBy).filter;
            } else {
                filter = groupBy.filter;
            }
            alias = groupBy.alias;
            if (!filter.isPresent()) {
                groupBys.add(groupBy.traverse1(processor));
            } else {
                boolean existed = hasPostcomputed(filter);
                if (!existed) {
                    groupBys.add(groupBy.traverse1(processor));
                } else {
                    processor.setComputationType(ComputationType.PostComputation);
                    final Optional<AggregateFilter> newFilter = Optional.of(filter.get().traverse1(processor));
                    if (!(groupBy.groupBy instanceof GroupBy.GroupByField)) {
                        groupBys.add(new GroupByEntry(groupBy.groupBy.traverse1(processor), newFilter, alias));
                    } else {
                        final GroupBy.GroupByField groupByField = (GroupBy.GroupByField) groupBy.groupBy;
                        final GroupBy.GroupByField newGroupByField = new GroupBy.GroupByField(
                                groupByField.field, Optional.empty(), groupByField.topK,
                                groupByField.withDefault);
                        groupBys.add(new GroupByEntry(newGroupByField.traverse1(processor), newFilter, alias));
                    }
                    processor.setComputationType(ComputationType.PreComputation);
                }
            }
        }
        List<AggregateMetric> totals = null;
        if (extractTotals) {
            // Extracting totals only if there are some regroupings.
            // Otherwise totals are the same as stats.
            totals = new ArrayList<>();
            processor.setDepth(0);
            processor.setStartDepth(1);
            processor.setMaxDepth(1);

            for (final AggregateMetric select : query.selects) {
                if ((select instanceof AggregateMetric.Percentile) ||
                        (select instanceof AggregateMetric.Distinct)) {
                    // percentile and distinct are processed with ftgs in IQL1
                    // and don't appear in totals.
                    continue;
                }
                final AggregateMetric processed = processor.apply(select);
                totals.add(processed);
            }
        }
        final List<AggregateMetric> selects = new ArrayList<>();
        processor.setDepth(groupBys.size());
        processor.setStartDepth(groupBys.size());
        processor.setMaxDepth(groupBys.size() + 1);
        for (int i = 0; i < query.selects.size(); i++) {
            final AggregateMetric select = query.selects.get(i);
            selects.add(processor.apply(select));
        }
        return new Extracted(new Query(query.datasets, query.filter, groupBys, selects, query.formatStrings, query.options, query.rowLimit, query.useLegacy).copyPosition(query), processor.computedNames, Optional.ofNullable(totals));
    }

    public static Map<Integer, List<ComputationInfo>> computationStages(Map<ComputationInfo, String> extracted) {
        final Map<Integer, List<ComputationInfo>> result = new Int2ObjectOpenHashMap<>();
        for (final ComputationInfo info : extracted.keySet()) {
            if (!result.containsKey(info.depth)) {
                result.put(info.depth, new ArrayList<>());
            }
            result.get(info.depth).add(info);
        }
        return result;
    }

    public static List<ExecutionStep> querySteps(Extracted extracted) {
        final Query query = extracted.query;
        final Map<ComputationType, Map<ComputationInfo, String>> computedNames = extracted.computedNames;
        final Map<Integer, List<ComputationInfo>> depthToPreComputation = computationStages(computedNames.get(ComputationType.PreComputation));
        final Map<Integer, List<ComputationInfo>> depthToPostComputation = computationStages(computedNames.get(ComputationType.PostComputation));
        final Map<Integer, ExecutionStep> groupBySteps = new HashMap<>();
        final Map<Integer, ExecutionStep.FilterGroups> postGroupByFilter = new HashMap<>();
        final List<GroupByEntry> groupBys = query.groupBys;
        final List<Dataset> datasets = query.getDatasets();
        for (int i = 0; i < groupBys.size(); i++) {
            final GroupByEntry groupByEntry = groupBys.get(i);
            final ExecutionStep step = groupByEntry.groupBy.executionStep(datasets);
            groupBySteps.put(i, step);
            if (groupByEntry.filter.isPresent()) {
                postGroupByFilter.put(i, new ExecutionStep.FilterGroups(groupByEntry.filter.get()));
            }
        }
        final List<ExecutionStep> resultSteps = new ArrayList<>();
        if (!groupBySteps.isEmpty() || !depthToPreComputation.isEmpty() || !depthToPostComputation.isEmpty()) {
            final int max = Ordering.natural().max(Iterables.concat(groupBySteps.keySet(),
                    depthToPreComputation.keySet(), depthToPostComputation.keySet()));
            for (int i = 0; i <= max; i++) {
                if (depthToPreComputation.containsKey(i)) {
                    for (final ComputationInfo computationInfo : depthToPreComputation.get(i)) {
                        resultSteps.add(new ExecutionStep.ComputePrecomputed(query.getDatasetsFromScope(computationInfo.scope), computationInfo.precomputed, computedNames.get(ComputationType.PreComputation).get(computationInfo)));
                    }
                }
                if (groupBySteps.containsKey(i)) {
                    resultSteps.add(groupBySteps.get(i));
                }
                if (depthToPostComputation.containsKey(i)) {
                    for (final ComputationInfo computationInfo : depthToPostComputation.get(i)) {
                        resultSteps.add(new ExecutionStep.ComputePrecomputed(query.getDatasetsFromScope(computationInfo.scope), computationInfo.precomputed, computedNames.get(ComputationType.PostComputation).get(computationInfo)));
                    }
                }
                if (postGroupByFilter.containsKey(i)) {
                    resultSteps.add(postGroupByFilter.get(i));
                }
            }
        }
        // IQL-606: it doesn't push "count()" in subqueries, add a GetGroupStats manually to ensure
        // the last command translated is SimpleIterate or GetGroupStats
        resultSteps.add(new ExecutionStep.GetGroupStats(query.selects, query.formatStrings));
        return resultSteps;
    }

    private static boolean hasPostcomputed(final Optional<AggregateFilter> filter) {
        AtomicBoolean existed = new AtomicBoolean(false);
        filter.get().transform(new Function<AggregateMetric, AggregateMetric>() {
            @Nullable
            @Override
            public AggregateMetric apply(@Nullable final AggregateMetric input) {
                if (input.requiresFTGS()) {
                    existed.set(true);
                }
                return input;
            }
        }, Function.identity(), Function.identity(), Function.identity(), Function.identity());
        return existed.get();
    }

    private static class Processor implements Function<AggregateMetric, AggregateMetric> {
        private int depth;
        private int startDepth;
        private int maxDepth;
        private Set<String> scope;
        private int nextName = 0;
        private ComputationType computationType;

        private final Map<ComputationType, Map<ComputationInfo, String>> computedNames = new HashMap<>();

        Processor(int depth, int startDepth, Set<String> scope) {
            this.depth = depth;
            this.startDepth = startDepth;
            this.scope = scope;
            for (ComputationType computationType : ComputationType.values()) {
                computedNames.put(computationType, new HashMap<>());
            }
            computationType = ComputationType.PreComputation;
        }

        public AggregateMetric apply(AggregateMetric input) {
            if (input instanceof AggregateMetric.Parent) {
                final AggregateMetric.Parent parent = (AggregateMetric.Parent) input;
                final int prevDepth = this.depth;
                this.setDepth(prevDepth - 1);
                final AggregateMetric result = apply(parent.metric);
                this.setDepth(prevDepth);
                return result;
            } else if (input instanceof AggregateMetric.Distinct) {
                final AggregateMetric.Distinct distinct = (AggregateMetric.Distinct) input;
                final Optional<AggregateFilter> filter;
                if (distinct.filter.isPresent()) {
                    final int prevDepth = this.depth;
                    this.setDepth(prevDepth + 1);
                    final int prevStartDepth = this.startDepth;
                    // New context, depth should be startDepth for the top level within the distinct.
                    this.setStartDepth(prevDepth + 1);
                    filter = Optional.of(distinct.filter.get().traverse1(this));
                    this.setDepth(prevDepth);
                    this.setStartDepth(prevStartDepth);
                } else {
                    filter = Optional.empty();
                }
                return handlePrecomputed(new Precomputed.PrecomputedDistinct(distinct.field, filter, distinct.windowSize));
            } else if (input instanceof AggregateMetric.Percentile) {
                final AggregateMetric.Percentile percentile = (AggregateMetric.Percentile) input;
                return handlePrecomputed(new Precomputed.PrecomputedPercentile(percentile.field, percentile.percentile));
            } else if (input instanceof AggregateMetric.Qualified) {
                final AggregateMetric.Qualified qualified = (AggregateMetric.Qualified) input;
                final Set<String> oldScope = ImmutableSet.copyOf(scope);
                final Set<String> newScope = Sets.newHashSet(qualified.scope);
                if (!oldScope.containsAll(newScope)) {
                    throw new IqlKnownException.ParseErrorException("Cannot have a sub-scope that is not a subset of the outer scope. oldScope = [" + oldScope + "], newScope = [" + newScope + "]");
                }
                setScope(newScope);
                final AggregateMetric result = apply(qualified.metric);
                setScope(oldScope);
                return result;
            } else if (input instanceof AggregateMetric.DocStats) {
                // This code is super hacky and serves the purpose of detecting all qualified
                // datasets in a given metric in order to restrict it to just that dataset
                // if necessary.
                // As well as to handle PARENT() and such.
                final AggregateMetric.DocStats docStats = (AggregateMetric.DocStats) input;
                final DocMetric docMetric = docStats.docMetric;
                if (startDepth == depth) {
                    final Set<String> pushScope;
                    final Set<String> docMetricQualifications = ExtractQualifieds.extractDocMetricDatasets(docMetric);
                    if (docMetricQualifications.isEmpty()) {
                        pushScope = scope;
                    } else if (docMetricQualifications.size() == 1) {
                        pushScope = docMetricQualifications;
                    } else {
                        throw new IqlKnownException.ParseErrorException("Doc Metric cannot have multiple different qualifications! metric = [" + docMetric + "], qualifications = [" + docMetricQualifications + "]");
                    }
                    final List<AggregateMetric> metrics = new ArrayList<>(pushScope.size());
                    for (final String dataset : pushScope) {
                        final AggregateMetric.DocStatsPushes metric = new AggregateMetric.DocStatsPushes(dataset, docMetric);
                        metrics.add(metric);
                    }
                    return AggregateMetric.Add.create(metrics).copyPosition(input);
                } else {
                    return handlePrecomputed(new Precomputed.PrecomputedRawStats(docMetric));
                }
            } else if (input instanceof AggregateMetric.SumAcross) {
                final AggregateMetric.SumAcross sumAcross = (AggregateMetric.SumAcross) input;
                if (sumAcross.groupBy instanceof GroupBy.GroupByMetric) {
                    final GroupBy.GroupByMetric groupByMetric = (GroupBy.GroupByMetric) sumAcross.groupBy;
                    if (!groupByMetric.excludeGutters) {
                        throw new IllegalArgumentException("SUM_OVER(BUCKET(), metric) with gutters is very likely not what you want. It will sum over all documents.");
                    }
                }
                if (sumAcross.groupBy instanceof GroupBy.GroupByField && !((GroupBy.GroupByField) sumAcross.groupBy).isLimitPresent()) {
                    final GroupBy.GroupByField groupBy = (GroupBy.GroupByField) sumAcross.groupBy;
                    return handlePrecomputed(new Precomputed.PrecomputedSumAcross(groupBy.field, apply(sumAcross.metric), groupBy.filter.map(x -> x.traverse1(this))));
                } else if (sumAcross.groupBy.isTotal()) {
                    return handlePrecomputed(new Precomputed.PrecomputedSumAcrossGroupBy(sumAcross.groupBy.traverse1(this), apply(sumAcross.metric)));
                } else {
                    final GroupBy totalized = sumAcross.groupBy.makeTotal();
                    if (!totalized.isTotal()) {
                        throw new IllegalStateException("groupBy.makeTotal() returned non-total GroupBy!");
                    }
                    return handlePrecomputed(new Precomputed.PrecomputedSumAcrossGroupBy(totalized.traverse1(this), apply(new AggregateMetric.IfThenElse(new AggregateFilter.IsDefaultGroup(), new AggregateMetric.Constant(0), sumAcross.metric))));
                }
            } else if (input instanceof AggregateMetric.FieldMin) {
                final AggregateMetric.FieldMin fieldMin = (AggregateMetric.FieldMin) input;
                return handlePrecomputed(
                        new Precomputed.PrecomputedFieldExtremeValue(
                                fieldMin.field,
                                apply(getOrDefaultToAggregateAvg(fieldMin.metric)),
                                fieldMin.filter.map(x -> x.traverse1(this)),
                                FieldExtremeType.FIELD_MIN
                        )
                );
            } else if (input instanceof AggregateMetric.FieldMax) {
                final AggregateMetric.FieldMax fieldMax = (AggregateMetric.FieldMax) input;
                return handlePrecomputed(
                        new Precomputed.PrecomputedFieldExtremeValue(
                                fieldMax.field,
                                apply(getOrDefaultToAggregateAvg(fieldMax.metric)),
                                fieldMax.filter.map(x -> x.traverse1(this)),
                                FieldExtremeType.FIELD_MAX
                        )
                );
            } else if (input instanceof AggregateMetric.DivideByCount) {
                final AggregateMetric docMetric = apply(((AggregateMetric.DivideByCount) input).metric);
                final Set<String> datasets = new HashSet<>();
                docMetric.transform(new Function<AggregateMetric, AggregateMetric>() {
                    @Nullable
                    @Override
                    public AggregateMetric apply(@Nullable final AggregateMetric metric) {
                        if (metric instanceof AggregateMetric.DocStatsPushes) {
                            datasets.add(((AggregateMetric.DocStatsPushes) metric).dataset);
                        } else if (metric instanceof AggregateMetric.GroupStatsLookup) {
                            for (Map.Entry<ComputationType, Map<ComputationInfo, String>> computations : computedNames.entrySet()) {
                                for (Map.Entry<ComputationInfo, String> computationEntry : computations.getValue().entrySet()) {
                                    if (computationEntry.getValue().equals(((AggregateMetric.GroupStatsLookup) metric).name)) {
                                        datasets.addAll(computationEntry.getKey().scope);
                                    }
                                }
                            }
                        }
                        return metric;
                    }
                }, Function.identity(), Function.identity(), Function.identity(), Function.identity());
                if (datasets.isEmpty()) {
                    throw new IllegalArgumentException("Averaging over no documents is undefined");
                }
                final List<AggregateMetric> metrics = new ArrayList<>(datasets.size());
                for (final String dataset : datasets) {
                    final AggregateMetric.DocStatsPushes metric = new AggregateMetric.DocStatsPushes(
                            dataset, new DocMetric.Qualified(dataset, new DocMetric.Count())
                    );
                    metrics.add(metric);
                }
                final AggregateMetric countMetric = AggregateMetric.Add.create(metrics);
                return new AggregateMetric.Divide(docMetric, countMetric).copyPosition(input);
            } else {
                return input.traverse1(this);
            }
        }

        private AggregateMetric getOrDefaultToAggregateAvg(
                final Optional<AggregateMetric> metric
        ) {
            return metric.<AggregateMetric>map(AggregateMetric.Negate::new).orElseGet(() -> new AggregateMetric.Constant(0));
        }

        private AggregateMetric handlePrecomputed(Precomputed precomputed) {
            final int depth;
            if (computationType == ComputationType.PreComputation) {
                // we do the pre computation after filter but before the next group by, so it should be in the next depth.
                depth = this.depth;
            } else {
                // we do the post aggregation after FTGS and before filter, so it should be in the same depth with GROUP BY
                depth = this.depth - 1;
            }
            final Set<String> scope = this.scope;

            if (depth < 0) {
                throw new IqlKnownException.ParseErrorException("Depth reached negative when processing metric: " + precomputed);
            }
            if (depth > maxDepth) {
                throw new IllegalStateException("Required computation in the future: " + precomputed);
            }

            final ComputationInfo computationInfo = new ComputationInfo(precomputed, depth, scope);
            final String name;
            if (!computedNames.get(computationType).containsKey(computationInfo)) {
                name = generateName();
                computedNames.get(computationType).put(computationInfo, name);
            } else {
                name = computedNames.get(computationType).get(computationInfo);
            }
            return new AggregateMetric.GroupStatsLookup(name);
        }

        private String generateName() {
            final String name = "v" + nextName;
            nextName += 1;
            return name;
        }

        public void setDepth(int depth) {
            this.depth = depth;
        }

        public void setStartDepth(int startDepth) {
            this.startDepth = startDepth;
        }

        public void setScope(Set<String> scope) {
            this.scope = scope;
        }

        public void setMaxDepth(int maxDepth) {
            this.maxDepth = maxDepth;
        }

        public void setComputationType(ComputationType computationType) {
            this.computationType = computationType;
        }
    }

    public static class Extracted {
        public final Query query;
        public final Map<ComputationType, Map<ComputationInfo, String>> computedNames;
        public final Optional<List<AggregateMetric>> totals;

        public Extracted(final Query query,
                         final Map<ComputationType, Map<ComputationInfo, String>> computedNames,
                         final Optional<List<AggregateMetric>> totals) {
            this.query = query;
            this.computedNames = computedNames;
            this.totals = totals;
        }

        @Override
        public String toString() {
            return "Extracted{" +
                    "query=" + query +
                    ", computedNames=" + computedNames +
                    ", totalNames=" + totals +
                    '}';
        }
    }

    @EqualsAndHashCode
    @ToString
    private static class ComputationInfo {
        private final Precomputed precomputed;
        private final int depth;
        private final Set<String> scope;

        ComputationInfo(final Precomputed precomputed, final int depth, final Set<String> scope) {
            this.precomputed = precomputed;
            this.depth = depth;
            this.scope = scope;
        }
    }

    private enum ComputationType {
        PreComputation,
        PostComputation
    }
}
