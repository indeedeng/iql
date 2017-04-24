package com.indeed.squall.iql2.language.passes;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.indeed.squall.iql2.language.AggregateFilter;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.DocFilter;
import com.indeed.squall.iql2.language.DocMetric;
import com.indeed.squall.iql2.language.GroupByMaybeHaving;
import com.indeed.squall.iql2.language.execution.ExecutionStep;
import com.indeed.squall.iql2.language.precomputed.Precomputed;
import com.indeed.squall.iql2.language.query.GroupBy;
import com.indeed.squall.iql2.language.query.Query;
import com.indeed.squall.iql2.language.util.Optionals;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExtractPrecomputed {
    public static Extracted extractPrecomputed(Query query) {
        final Processor processor = new Processor(1, 1, query.extractDatasetNames());
        final List<GroupByMaybeHaving> groupBys = new ArrayList<>();
        for (int i = 0; i < query.groupBys.size(); i++) {
            final GroupByMaybeHaving groupBy = query.groupBys.get(i);
            processor.setDepth(i + 1);
            processor.setStartDepth(i + 1);
            processor.setMaxDepth(i + 1);
            final Optional<AggregateFilter> filter;
            if (groupBy.groupBy instanceof GroupBy.GroupByField) {
                filter = ((GroupBy.GroupByField) groupBy.groupBy).filter;
            } else {
                filter = groupBy.filter;
            }
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
                        groupBys.add(new GroupByMaybeHaving(groupBy.groupBy.traverse1(processor), newFilter));
                    } else {
                        final GroupBy.GroupByField groupByField = (GroupBy.GroupByField) groupBy.groupBy;
                        final GroupBy.GroupByField newGroupByField = new GroupBy.GroupByField(
                                groupByField.field, Optional.absent(), groupByField.limit, groupByField.metric,
                                groupByField.withDefault, groupByField.forceNonStreaming);
                        groupBys.add(new GroupByMaybeHaving(newGroupByField.traverse1(processor), newFilter));
                    }
                    processor.setComputationType(ComputationType.PreComputation);
                }
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
        return new Extracted(new Query(query.datasets, query.filter, groupBys, selects, query.formatStrings, query.rowLimit), processor.computedNames);
    }

    public static Map<Integer, List<ComputationInfo>> computationStages(Map<ComputationInfo, String> extracted) {
        final Map<Integer, List<ComputationInfo>> result = new Int2ObjectOpenHashMap<>();
        for (final ComputationInfo info : extracted.keySet()) {
            if (!result.containsKey(info.depth)) {
                result.put(info.depth, new ArrayList<ComputationInfo>());
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
        final List<GroupByMaybeHaving> groupBys = query.groupBys;
        final Set<String> scope = query.extractDatasetNames();
        for (int i = 0; i < groupBys.size(); i++) {
            final GroupByMaybeHaving groupByMaybeHaving = groupBys.get(i);
            final ExecutionStep step = groupByMaybeHaving.groupBy.executionStep(scope);
            groupBySteps.put(i, step);
            if (groupByMaybeHaving.filter.isPresent()) {
                postGroupByFilter.put(i, new ExecutionStep.FilterGroups(groupByMaybeHaving.filter.get()));
            }
        }
        final List<ExecutionStep> resultSteps = new ArrayList<>();
        if (query.filter.isPresent()) {
            final DocFilter filter = query.filter.get();
            resultSteps.add(new ExecutionStep.FilterDocs(filter, scope));
        }
        if (!groupBySteps.isEmpty() || !depthToPreComputation.isEmpty() || !depthToPostComputation.isEmpty()) {
            final int max = Ordering.natural().max(Iterables.concat(groupBySteps.keySet(),
                    depthToPreComputation.keySet(), depthToPostComputation.keySet()));
            for (int i = 0; i <= max; i++) {
                if (depthToPreComputation.containsKey(i)) {
                    for (final ComputationInfo computationInfo : depthToPreComputation.get(i)) {
                        resultSteps.add(new ExecutionStep.ComputePrecomputed(computationInfo.scope, computationInfo.precomputed, computedNames.get(ComputationType.PreComputation).get(computationInfo)));
                    }
                }
                if (groupBySteps.containsKey(i)) {
                    resultSteps.add(groupBySteps.get(i));
                }
                if (depthToPostComputation.containsKey(i)) {
                    for (final ComputationInfo computationInfo : depthToPostComputation.get(i)) {
                        resultSteps.add(new ExecutionStep.ComputePrecomputed(computationInfo.scope, computationInfo.precomputed, computedNames.get(ComputationType.PostComputation).get(computationInfo)));
                    }
                }
                if (postGroupByFilter.containsKey(i)) {
                    resultSteps.add(postGroupByFilter.get(i));
                }
            }
        }
        if (!query.selects.isEmpty()) {
            resultSteps.add(new ExecutionStep.GetGroupStats(query.selects, query.formatStrings));
        }
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
        }, Functions.identity(), Functions.identity(), Functions.identity(), Functions.identity());
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

        public Processor(int depth, int startDepth, Set<String> scope) {
            this.depth = depth;
            this.startDepth = startDepth;
            this.scope = scope;
            for (ComputationType computationType : ComputationType.values()) {
                computedNames.put(computationType, new HashMap<>());
            }
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
                return handlePrecomputed(new Precomputed.PrecomputedDistinct(distinct.field.unwrap(), filter, distinct.windowSize));
            } else if (input instanceof AggregateMetric.Percentile) {
                final AggregateMetric.Percentile percentile = (AggregateMetric.Percentile) input;
                return handlePrecomputed(new Precomputed.PrecomputedPercentile(percentile.field.unwrap(), percentile.percentile));
            } else if (input instanceof AggregateMetric.Qualified) {
                final AggregateMetric.Qualified qualified = (AggregateMetric.Qualified) input;
                final Set<String> oldScope = ImmutableSet.copyOf(this.scope);
                final Set<String> newScope = Sets.newHashSet(qualified.scope);
                if (!oldScope.containsAll(newScope)) {
                    throw new IllegalStateException("Cannot have a sub-scope that is a subset of the outer scope. oldScope = [" + oldScope + "], newScope = [" + newScope + "]");
                }
                setScope(newScope);
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
                    docMetric = implicitDocStats.docMetric;
                }
                if (startDepth == depth) {
                    AggregateMetric aggregateMetric = null;
                    final Set<String> pushScope;
                    final Set<String> docMetricQualifications = ExtractQualifieds.extractDocMetricDatasets(docMetric);
                    if (docMetricQualifications.isEmpty()) {
                        pushScope = scope;
                    } else if (docMetricQualifications.size() == 1) {
                        pushScope = docMetricQualifications;
                    } else {
                        throw new IllegalArgumentException("Doc Metric cannot have multiple different qualifications! metric = [" + docMetric + "], qualifications = [" + docMetricQualifications + "]");
                    }
                    for (final String dataset : pushScope) {
                        final AggregateMetric.DocStatsPushes metric = new AggregateMetric.DocStatsPushes(dataset, new DocMetric.PushableDocMetric(docMetric));
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
                if (sumAcross.groupBy instanceof GroupBy.GroupByMetric) {
                    final GroupBy.GroupByMetric groupByMetric = (GroupBy.GroupByMetric) sumAcross.groupBy;
                    if (!groupByMetric.excludeGutters) {
                        throw new IllegalArgumentException("SUM_OVER(BUCKET(), metric) with gutters is very likely not what you want. It will sum over all documents.");
                    }
                }
                if (sumAcross.groupBy instanceof GroupBy.GroupByField && !((GroupBy.GroupByField) sumAcross.groupBy).limit.isPresent()) {
                    final GroupBy.GroupByField groupBy = (GroupBy.GroupByField) sumAcross.groupBy;
                    return handlePrecomputed(new Precomputed.PrecomputedSumAcross(groupBy.field.unwrap(), apply(sumAcross.metric), Optionals.traverse1(groupBy.filter, this)));
                } else if (sumAcross.groupBy.isTotal()) {
                    return handlePrecomputed(new Precomputed.PrecomputedSumAcrossGroupBy(sumAcross.groupBy.traverse1(this), apply(sumAcross.metric)));
                } else {
                    final GroupBy totalized = sumAcross.groupBy.makeTotal();
                    if (!totalized.isTotal()) {
                        throw new IllegalStateException("groupBy.makeTotal() returned non-total GroupBy!");
                    }
                    return handlePrecomputed(new Precomputed.PrecomputedSumAcrossGroupBy(totalized.traverse1(this), apply(new AggregateMetric.IfThenElse(new AggregateFilter.IsDefaultGroup(), new AggregateMetric.Constant(0), sumAcross.metric))));
                }
            } else if (input instanceof AggregateMetric.FieldMin){
                final AggregateMetric.FieldMin fieldMin = (AggregateMetric.FieldMin) input;
                return handlePrecomputed(new Precomputed.PrecomputedFieldMin(fieldMin.field.unwrap()));
            } else if (input instanceof AggregateMetric.FieldMax) {
                final AggregateMetric.FieldMax fieldMax = (AggregateMetric.FieldMax) input;
                return handlePrecomputed(new Precomputed.PrecomputedFieldMax(fieldMax.field.unwrap()));
            } else if (input instanceof AggregateMetric.Bootstrap) {
                final AggregateMetric.Bootstrap bootstrap = (AggregateMetric.Bootstrap) input;
                final List<String> lookups = new ArrayList<>();
                for (final String vararg : bootstrap.varargs) {
                    if ("\"all\"".equals(vararg)) {
                        for (int i = 0; i < bootstrap.numBootstraps; i++) {
                            lookups.add(bootstrap.seed + "[" + bootstrap.numBootstraps + "].values[" + i + "]");
                        }
                    } else {
                        lookups.add(bootstrap.seed + "[" + bootstrap.numBootstraps + "]." + vararg);
                    }
                }
                final Optional<AggregateFilter> filter;
                if (bootstrap.filter.isPresent()) {
                    filter = Optional.of(bootstrap.filter.get().traverse1(this));
                } else {
                    filter = Optional.absent();
                }
                final Precomputed.PrecomputedBootstrap precomputedBootstrap = new Precomputed.PrecomputedBootstrap(bootstrap.field.unwrap(), filter, bootstrap.seed, bootstrap.metric.traverse1(this), bootstrap.numBootstraps, bootstrap.varargs);
                final ComputationInfo computationInfo = new ComputationInfo(precomputedBootstrap, depth, scope);
                final String name = bootstrap.seed + "[" + bootstrap.numBootstraps + "]";
                computedNames.get(computationType).put(computationInfo, name);
                return new AggregateMetric.GroupStatsMultiLookup(lookups);
            } else if (input instanceof AggregateMetric.DivideByCount) {
                final AggregateMetric docMetric = apply(((AggregateMetric.DivideByCount)input).metric);
                final Set<String> datasets = new HashSet<>();
                docMetric.transform(new Function<AggregateMetric, AggregateMetric>() {
                    @Nullable
                    @Override
                    public AggregateMetric apply(@Nullable final AggregateMetric metric) {
                        if (metric instanceof AggregateMetric.DocStatsPushes) {
                            datasets.add(((AggregateMetric.DocStatsPushes) metric).dataset);
                        } else if (metric instanceof AggregateMetric.GroupStatsLookup) {
                            for (Map.Entry<PrecomputedInfo, String> precomputedEntry : precomputedNames.entrySet()) {
                                if (precomputedEntry.getValue().equals(((AggregateMetric.GroupStatsLookup)metric).name)) {
                                    datasets.addAll(precomputedEntry.getKey().scope);
                                }
                            }
                        }
                        return metric;
                    }
                }, Functions.identity(), Functions.identity(), Functions.identity(), Functions.identity());
                if (datasets.isEmpty()) {
                    throw new IllegalArgumentException("Averaging over no documents is undefined");
                }
                AggregateMetric countMetric = null;
                for (String dataset : datasets) {
                    final AggregateMetric.DocStatsPushes metric = new AggregateMetric.DocStatsPushes(
                            dataset, new DocMetric.PushableDocMetric(new DocMetric.Qualified(dataset, new DocMetric.Count())));
                    if (countMetric == null) {
                        countMetric = metric;
                    } else {
                        countMetric = new AggregateMetric.Add(countMetric, metric);
                    }
                }
                return new AggregateMetric.Divide(docMetric, countMetric);
            } else {
                return input.traverse1(this);
            }
        }

        private AggregateMetric handlePrecomputed(Precomputed precomputed) {
            final int depth;
            if (computationType == ComputationType.PreComputation) {
                // we do the pre computation after filter but before the next group by, so it should be in the next depth.
                depth = this.depth;
            } else {
                // we do the post aggregation after FTGS and before filter, so it should be in the same depth with GROUP BY
                depth = this.depth-1;
            }
            final Set<String> scope = this.scope;

            if (depth < 0) {
                throw new IllegalArgumentException("Depth reached negative when processing metric: " + precomputed);
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
        private final Map<ComputationType, Map<ComputationInfo, String>> computedNames;

        public Extracted(final Query query, final Map<ComputationType, Map<ComputationInfo, String>> computedNames) {
            this.query = query;
            this.computedNames = computedNames;
        }

        @Override
        public String toString() {
            return "Extracted{" +
                    "query=" + query +
                    ", computedNames=" + computedNames +
                    '}';
        }
    }

    private static class ComputationInfo {
        private final Precomputed precomputed;
        private final int depth;
        private final Set<String> scope;

        private ComputationInfo(Precomputed precomputed, int depth, Set<String> scope) {
            this.precomputed = precomputed;
            this.depth = depth;
            this.scope = scope;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ComputationInfo that = (ComputationInfo) o;
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
            return "ComputationInfo{" +
                    "precomputed=" + precomputed +
                    ", depth=" + depth +
                    ", scope=" + scope +
                    '}';
        }
    }

    private enum ComputationType {
        PreComputation,
        PostComputation;
    }
}
