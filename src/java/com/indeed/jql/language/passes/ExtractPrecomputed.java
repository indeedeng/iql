package com.indeed.jql.language.passes;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.indeed.jql.language.AggregateFilter;
import com.indeed.jql.language.AggregateMetric;
import com.indeed.jql.language.DocFilter;
import com.indeed.jql.language.DocMetric;
import com.indeed.jql.language.execution.ExecutionStep;
import com.indeed.jql.language.precomputed.Precomputed;
import com.indeed.jql.language.query.GroupBy;
import com.indeed.jql.language.query.Query;
import com.indeed.jql.language.util.Optionals;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ExtractPrecomputed {
    public static Extracted extractPrecomputed(Query query) {
        final Processor processor = new Processor(1, 1, query.extractDatasetNames());
        final List<GroupBy> groupBys = new ArrayList<>();
        for (int i = 0; i < query.groupBys.size(); i++) {
            final GroupBy groupBy = query.groupBys.get(i);
            processor.setDepth(i + 1);
            processor.setStartDepth(i + 1);
            groupBys.add(groupBy.traverse1(processor));
        }
        final List<AggregateMetric> selects = new ArrayList<>();
        processor.setDepth(groupBys.size());
        processor.setStartDepth(groupBys.size());
        processor.setMaxDepth(groupBys.size() + 1);
        for (int i = 0; i < query.selects.size(); i++) {
            final AggregateMetric select = query.selects.get(i);
            selects.add(processor.apply(select));
        }
        return new Extracted(new Query(query.datasets, query.filter, groupBys, selects), processor.precomputedNames);
    }

    public static Map<Integer, List<PrecomputedInfo>> computationStages(Map<PrecomputedInfo, String> extracted) {
        final Map<Integer, List<PrecomputedInfo>> result = new Int2ObjectOpenHashMap<>();
        for (final PrecomputedInfo info : extracted.keySet()) {
            if (!result.containsKey(info.depth)) {
                result.put(info.depth, new ArrayList<PrecomputedInfo>());
            }
            result.get(info.depth).add(info);
        }
        return result;
    }

    public static List<ExecutionStep> querySteps(Extracted extracted) {
        final Query query = extracted.query;
        final Map<PrecomputedInfo, String> precomputedNames = extracted.precomputedNames;
        final Map<Integer, List<PrecomputedInfo>> depthToPrecomputations = computationStages(precomputedNames);
        final Map<Integer, ExecutionStep> groupBySteps = new HashMap<>();
        final List<GroupBy> groupBys = query.groupBys;
        final Set<String> scope = query.extractDatasetNames();
        for (int i = 0; i < groupBys.size(); i++) {
            final ExecutionStep step = groupBys.get(i).executionStep(scope);
            groupBySteps.put(i, step);
        }
        final List<ExecutionStep> resultSteps = new ArrayList<>();
        if (query.filter.isPresent()) {
            final DocFilter filter = query.filter.get();
            resultSteps.add(new ExecutionStep.FilterDocs(filter, scope));
        }
        if (!groupBySteps.isEmpty() || !depthToPrecomputations.isEmpty()) {
            final int max = Ordering.natural().max(Iterables.concat(groupBySteps.keySet(), depthToPrecomputations.keySet()));
            for (int i = 0; i <= max; i++) {
                if (depthToPrecomputations.containsKey(i)) {
                    for (final PrecomputedInfo precomputedInfo : depthToPrecomputations.get(i)) {
                        resultSteps.add(new ExecutionStep.ComputePrecomputed(precomputedInfo.scope, precomputedInfo.precomputed, precomputedNames.get(precomputedInfo)));
                    }
                }
                if (groupBySteps.containsKey(i)) {
                    resultSteps.add(groupBySteps.get(i));
                }
            }
        }
        if (!query.selects.isEmpty()) {
            resultSteps.add(new ExecutionStep.GetGroupStats(query.selects));
        }
        return resultSteps;
    }

    private static class Processor implements Function<AggregateMetric, AggregateMetric> {
        private int depth;
        private int startDepth;
        private int maxDepth;
        private Set<String> scope;
        private int nextName = 0;

        private final Map<PrecomputedInfo, String> precomputedNames = new HashMap<>();

        public Processor(int depth, int startDepth, Set<String> scope) {
            this.depth = depth;
            this.startDepth = startDepth;
            this.scope = scope;
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
                    this.setStartDepth(startDepth + 1);
                    filter = Optional.of(distinct.filter.get().traverse1(this));
                    this.setDepth(prevDepth);
                    this.setStartDepth(prevStartDepth);
                } else {
                    filter = Optional.absent();
                }
                return handlePrecomputed(new Precomputed.PrecomputedDistinct(distinct.field, filter, distinct.windowSize));
            } else if (input instanceof AggregateMetric.Percentile) {
                final AggregateMetric.Percentile percentile = (AggregateMetric.Percentile) input;
                return handlePrecomputed(new Precomputed.PrecomputedPercentile(percentile.field, percentile.percentile));
            } else if (input instanceof AggregateMetric.Qualified) {
                final AggregateMetric.Qualified qualified = (AggregateMetric.Qualified) input;
                final Set<String> oldScope = ImmutableSet.copyOf(this.scope);
                setScope(Sets.newHashSet(qualified.scope));
                final AggregateMetric result = apply(qualified.metric);
                setScope(oldScope);
                return result;
            } else if (input instanceof AggregateMetric.DocStats || input instanceof AggregateMetric.ImplicitDocStats) {
                final DocMetric docMetric;
                if (input instanceof AggregateMetric.DocStats) {
                    final AggregateMetric.DocStats stats = (AggregateMetric.DocStats) input;
                    docMetric = stats.metric;
                } else {
                    final AggregateMetric.ImplicitDocStats implicitDocStats = (AggregateMetric.ImplicitDocStats) input;
                    docMetric = new DocMetric.Field(implicitDocStats.field);
                }
                if (startDepth == depth) {
                    AggregateMetric aggregateMetric = null;
                    for (final String dataset : scope) {
                        final AggregateMetric.DocStatsPushes metric = new AggregateMetric.DocStatsPushes(dataset, docMetric.getPushes(dataset));
                        if (aggregateMetric == null) {
                            aggregateMetric = metric;
                        } else {
                            aggregateMetric = new AggregateMetric.Add(metric, aggregateMetric);
                        }
                    }
                    return aggregateMetric;
                } else {
                    return handlePrecomputed(new Precomputed.PrecomputedRawStats(docMetric));
                }
            } else if (input instanceof AggregateMetric.SumAcross) {
                final AggregateMetric.SumAcross sumAcross = (AggregateMetric.SumAcross) input;
                if (sumAcross.groupBy instanceof GroupBy.GroupByField) {
                    final GroupBy.GroupByField groupBy = (GroupBy.GroupByField) sumAcross.groupBy;
                    return handlePrecomputed(new Precomputed.PrecomputedSumAcross(groupBy.field, apply(sumAcross.metric), Optionals.traverse1(groupBy.filter, this)));
                } else {
                    return handlePrecomputed(new Precomputed.PrecomputedSumAcrossGroupBy(sumAcross.groupBy.traverse1(this), apply(sumAcross.metric)));
                }
            } else {
                return input.traverse1(this);
            }
        }

        public void setDepth(int depth) {
            this.depth = depth;
        }

        public void setStartDepth(int startDepth) {
            this.startDepth = startDepth;
        }

        private AggregateMetric handlePrecomputed(Precomputed precomputed) {
            final int depth = this.depth;
            final Set<String> scope = this.scope;

            if (depth < 0) {
                throw new IllegalArgumentException("Depth reached negative when processing metric: " + precomputed);
            }
            if (depth > maxDepth) {
                throw new IllegalStateException("Required computation in the future: " + precomputed);
            }

            final PrecomputedInfo precomputedInfo = new PrecomputedInfo(precomputed, depth, scope);
            final String name;
            if (!precomputedNames.containsKey(precomputedInfo)) {
                name = generateName();
                precomputedNames.put(precomputedInfo, name);
            } else {
                name = precomputedNames.get(precomputedInfo);
            }
            return new AggregateMetric.GroupStatsLookup(name);
        }

        private String generateName() {
            final String name = "v" + nextName;
            nextName += 1;
            return name;
        }

        public void setScope(Set<String> scope) {
            this.scope = scope;
        }

        public void setMaxDepth(int maxDepth) {
            this.maxDepth = maxDepth;
        }
    }

    public static class Extracted {
        public final Query query;
        private final Map<PrecomputedInfo, String> precomputedNames;

        private Extracted(Query query, Map<PrecomputedInfo, String> precomputedNames) {
            this.query = query;
            this.precomputedNames = precomputedNames;
        }

        @Override
        public String toString() {
            return "Extracted{" +
                    "query=" + query +
                    ", precomputedNames=" + precomputedNames +
                    '}';
        }
    }

    private static class PrecomputedInfo {
        private final Precomputed precomputed;
        private final int depth;
        private final Set<String> scope;

        private PrecomputedInfo(Precomputed precomputed, int depth, Set<String> scope) {
            this.precomputed = precomputed;
            this.depth = depth;
            this.scope = scope;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrecomputedInfo that = (PrecomputedInfo) o;
            return Objects.equals(depth, that.depth) &&
                    Objects.equals(precomputed, that.precomputed) &&
                    Objects.equals(scope, that.scope);
        }

        @Override
        public int hashCode() {
            return Objects.hash(precomputed, depth, scope);
        }

        @Override
        public String toString() {
            return "PrecomputedInfo{" +
                    "precomputed=" + precomputed +
                    ", depth=" + depth +
                    ", scope=" + scope +
                    '}';
        }
    }
}
