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

package com.indeed.iql2.language.execution.passes;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.execution.ExecutionStep;
import com.indeed.iql2.language.precomputed.Precomputed;
import com.indeed.iql2.language.query.Dataset;
import com.indeed.iql2.language.query.GroupBy;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.util.core.Pair;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class GroupIterations {
    public static List<ExecutionStep> apply(List<ExecutionStep> steps) {
        final List<ExecutionStep> result = new ArrayList<>();
        final List<ExecutionStep.ComputePrecomputed> runOfPrecomputeds = new ArrayList<>();
        for (final ExecutionStep step : steps) {
            if (step instanceof ExecutionStep.ComputePrecomputed) {
                runOfPrecomputeds.add((ExecutionStep.ComputePrecomputed) step);
            } else {
                if (!runOfPrecomputeds.isEmpty()) {
                    result.addAll(handleGroup(runOfPrecomputeds));
                    runOfPrecomputeds.clear();
                }
                result.add(step);
            }
        }
        if (!runOfPrecomputeds.isEmpty()) {
            result.addAll(handleGroup(runOfPrecomputeds));
            runOfPrecomputeds.clear();
        }
        return result;
    }

    private static List<ExecutionStep> handleGroup(List<ExecutionStep.ComputePrecomputed> runOfPrecomputeds) {
        final List<Grouping> groupings = findGroupings(runOfPrecomputeds);
        final List<ExecutionStep> result = new ArrayList<>();
        for (final Grouping grouping : groupings) {
            if (grouping.precomputationInfos.size() == 1) {
                final PrecomputationInfo info = grouping.precomputationInfos.get(0);
                result.add(new ExecutionStep.ComputePrecomputed(grouping.datasets, info.precomputation, info.name));
            } else {
                final List<Pair<Precomputed, String>> pairList = new ArrayList<>();
                for (final PrecomputationInfo info : grouping.precomputationInfos) {
                    pairList.add(Pair.of(info.precomputation, info.name));
                }
                result.add(new ExecutionStep.ComputeManyPrecomputed(grouping.datasets, pairList));
            }
        }
        return result;
    }

    private static List<Grouping> findGroupings(List<ExecutionStep.ComputePrecomputed> precomputeds) {
        final List<Grouping>[] bestGrouping = new List[1];

        final boolean noDependencies = precomputeds.stream().allMatch(x -> findNamedDependencies(x.computation).isEmpty());
        if (noDependencies) {
            return groupByContexts(precomputeds)
                    .entrySet()
                    .stream()
                    .map(e -> Grouping.from(e.getKey(), e.getValue()))
                    .collect(Collectors.toList());
        }

        recursivelyConsiderAllOrders(new ArrayList<>(), precomputeds, bestGrouping);
        if (bestGrouping[0] == null) {
            throw new IllegalStateException("No groupings?!");
        }
        return bestGrouping[0];
    }

    private static void recursivelyConsiderAllOrders(List<Grouping> soFar, List<ExecutionStep.ComputePrecomputed> precomputeds, List<Grouping>[] bestGrouping) {
        final int bestGroupingSize = (bestGrouping[0] == null) ? Integer.MAX_VALUE : bestGrouping[0].size();
        if (precomputeds.isEmpty()) {
            if (soFar.size() < bestGroupingSize) {
                bestGrouping[0] = new ArrayList<>(soFar);
            }
        } else {
            if ((soFar.size() + 1) >= bestGroupingSize) {
                // no sense to iterate further.
                return;
            }
            final List<ExecutionStep.ComputePrecomputed> usable = new ArrayList<>();
            for (final ExecutionStep.ComputePrecomputed precomputed : precomputeds) {
                if (soFar.containsAll(findNamedDependencies(precomputed.computation))) {
                    usable.add(precomputed);
                }
            }

            final Map<PrecomputedContext, List<ExecutionStep.ComputePrecomputed>> contextMembers = groupByContexts(usable);

            for (final Map.Entry<PrecomputedContext, List<ExecutionStep.ComputePrecomputed>> entry : contextMembers.entrySet()) {
                final Grouping grouping = Grouping.from(entry.getKey(), entry.getValue());
                soFar.add(grouping);
                recursivelyConsiderAllOrders(soFar, Lists.newArrayList(Iterables.filter(precomputeds, new Predicate<ExecutionStep.ComputePrecomputed>() {
                    @Override
                    public boolean apply(ExecutionStep.ComputePrecomputed input) {
                        return !entry.getValue().contains(input);
                    }
                })), bestGrouping);
                soFar.remove(soFar.size() - 1);
            }
        }
    }

    private static Map<PrecomputedContext, List<ExecutionStep.ComputePrecomputed>> groupByContexts(final List<ExecutionStep.ComputePrecomputed> usable) {
        final Map<PrecomputedContext, List<ExecutionStep.ComputePrecomputed>> contextMembers = new HashMap<>();
        for (final ExecutionStep.ComputePrecomputed elem : usable) {
            final PrecomputedContext ctx = PrecomputedContext.create(elem);
            List<ExecutionStep.ComputePrecomputed> members = contextMembers.get(ctx);
            if (members == null) {
                members = new ArrayList<>();
                contextMembers.put(ctx, members);
            }
            members.add(elem);
        }
        return contextMembers;
    }

    private static Set<String> findNamedDependencies(Precomputed precomputed) {
        final Set<String> dependencies = new HashSet<>();
        precomputed.transform(
                Functions.<Precomputed>identity(),
                new Function<AggregateMetric, AggregateMetric>() {
                    @Override
                    public AggregateMetric apply(AggregateMetric input) {
                        if (input instanceof AggregateMetric.GroupStatsLookup) {
                            final AggregateMetric.GroupStatsLookup groupStatsLookup = (AggregateMetric.GroupStatsLookup) input;
                            dependencies.add(groupStatsLookup.name);
                        }
                        return input;
                    }
                },
                Functions.<DocMetric>identity(),
                Functions.<AggregateFilter>identity(),
                Functions.<DocFilter>identity(),
                Functions.<GroupBy>identity()
        );
        return dependencies;
    }

    @Data
    private static class PrecomputedContext {
        private final List<Dataset> datasets;
        private final Optional<FieldSet> field;

        // TODO: Make this a visitor to avoid being able to forget handling new ones
        public static PrecomputedContext create(ExecutionStep.ComputePrecomputed computePrecomputed) {
            final List<Dataset> datasets = computePrecomputed.datasets;
            final Precomputed computation = computePrecomputed.computation;
            if (computation instanceof Precomputed.PrecomputedDistinct) {
                final Precomputed.PrecomputedDistinct precomputedDistinct = (Precomputed.PrecomputedDistinct) computation;
                return new PrecomputedContext(datasets, Optional.of(precomputedDistinct.field));
            } else if (computation instanceof Precomputed.PrecomputedPercentile) {
                final Precomputed.PrecomputedPercentile precomputedPercentile = (Precomputed.PrecomputedPercentile) computation;
                return new PrecomputedContext(datasets, Optional.of(precomputedPercentile.field));
            } else if (computation instanceof Precomputed.PrecomputedRawStats) {
                return new PrecomputedContext(datasets, Optional.absent());
            } else if (computation instanceof Precomputed.PrecomputedSumAcross) {
                final Precomputed.PrecomputedSumAcross precomputedSumAcross = (Precomputed.PrecomputedSumAcross) computation;
                return new PrecomputedContext(datasets, Optional.of(precomputedSumAcross.field));
            } else if (computation instanceof Precomputed.PrecomputedSumAcrossGroupBy) {
                final Precomputed.PrecomputedSumAcrossGroupBy precomputedSumAcrossGroupBy = (Precomputed.PrecomputedSumAcrossGroupBy) computation;
                return new PrecomputedContext(datasets, Optional.absent());
            } else if (computation instanceof Precomputed.PrecomputedFieldExtremeValue) {
                final Precomputed.PrecomputedFieldExtremeValue precomputedFieldExtremeValue = (Precomputed.PrecomputedFieldExtremeValue) computation;
                return new PrecomputedContext(datasets, Optional.of(precomputedFieldExtremeValue.field));
            } else {
                throw new IllegalStateException("Failed to handle: [" + computePrecomputed + "]'s computation: [" + computation + "]");
            }
        }
    }

    private static class Grouping {
        private final List<Dataset> datasets;
        private final List<PrecomputationInfo> precomputationInfos;

        private Grouping(List<Dataset> datasets, List<PrecomputationInfo> precomputationInfos) {
            this.datasets = datasets;
            this.precomputationInfos = precomputationInfos;
        }

        public static Grouping from(PrecomputedContext ctx, List<ExecutionStep.ComputePrecomputed> precomputeds) {
            final List<PrecomputationInfo> infos = new ArrayList<>();
            for (final ExecutionStep.ComputePrecomputed precomputed : precomputeds) {
                infos.add(new PrecomputationInfo(precomputed.computation, precomputed.name));
            }
            return new Grouping(ctx.datasets, infos);
        }
    }

    private static class PrecomputationInfo {
        private final Precomputed precomputation;
        private final String name;

        private PrecomputationInfo(Precomputed precomputation, String name) {
            this.precomputation = precomputation;
            this.name = name;
        }
    }

}
