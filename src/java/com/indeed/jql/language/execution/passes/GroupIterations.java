package com.indeed.jql.language.execution.passes;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.indeed.jql.language.AggregateFilter;
import com.indeed.jql.language.AggregateMetric;
import com.indeed.jql.language.DocFilter;
import com.indeed.jql.language.DocMetric;
import com.indeed.jql.language.execution.ExecutionStep;
import com.indeed.jql.language.precomputed.Precomputed;
import com.indeed.util.core.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
                result.add(new ExecutionStep.ComputePrecomputed(grouping.scope, info.precomputation, info.name));
            } else {
                final List<Pair<Precomputed, String>> pairList = new ArrayList<>();
                for (final PrecomputationInfo info : grouping.precomputationInfos) {
                    pairList.add(Pair.of(info.precomputation, info.name));
                }
                result.add(new ExecutionStep.ComputeManyPrecomputed(grouping.scope, pairList));
            }
        }
        return result;
    }

    private static List<Grouping> findGroupings(List<ExecutionStep.ComputePrecomputed> precomputeds) {
        final ArrayList<List<Grouping>> resultStore = new ArrayList<>();
        recursivleyConsiderAllOrders(new ArrayList<Grouping>(), new HashSet<String>(), precomputeds, resultStore);
        if (resultStore.isEmpty()) {
            throw new IllegalStateException("No groupings?!");
        }
        List<Grouping> bestGrouping = resultStore.get(0);
        for (final List<Grouping> grouping : resultStore) {
            if (grouping.size() < bestGrouping.size()) {
                bestGrouping = grouping;
            }
        }
        return bestGrouping;
    }

    private static void recursivleyConsiderAllOrders(List<Grouping> soFar, Set<String> handledDeps, List<ExecutionStep.ComputePrecomputed> precomputeds, List<List<Grouping>> resultStore) {
        if (precomputeds.isEmpty()) {
            resultStore.add(new ArrayList<>(soFar));
        } else {
            final List<ExecutionStep.ComputePrecomputed> usable = new ArrayList<>();
            for (final ExecutionStep.ComputePrecomputed precomputed : precomputeds) {
                if (soFar.containsAll(findNamedDependencies(precomputed.computation))) {
                    usable.add(precomputed);
                }
            }

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

            for (final Map.Entry<PrecomputedContext, List<ExecutionStep.ComputePrecomputed>> entry : contextMembers.entrySet()) {
                final Grouping grouping = Grouping.from(entry.getKey(), entry.getValue());
                final Set<String> depsAdded = grouping.names();
                soFar.add(grouping);
                handledDeps.addAll(depsAdded);
                recursivleyConsiderAllOrders(soFar, handledDeps, Lists.newArrayList(Iterables.filter(precomputeds, new Predicate<ExecutionStep.ComputePrecomputed>() {
                    @Override
                    public boolean apply(ExecutionStep.ComputePrecomputed input) {
                        return !entry.getValue().contains(input);
                    }
                })), resultStore);
                handledDeps.removeAll(depsAdded);
                soFar.remove(soFar.size() - 1);
            }
        }
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
                Functions.<DocFilter>identity()
        );
        return dependencies;
    }

    private static class PrecomputedContext {
        private final Set<String> scope;
        private final Optional<String> field;

        private PrecomputedContext(Set<String> scope, Optional<String> field) {
            this.scope = scope;
            this.field = field;
        }

        public static PrecomputedContext create(ExecutionStep.ComputePrecomputed computePrecomputed) {
            final Set<String> scope = computePrecomputed.scope;
            final Precomputed computation = computePrecomputed.computation;
            if (computation instanceof Precomputed.PrecomputedDistinct) {
                final Precomputed.PrecomputedDistinct precomputedDistinct = (Precomputed.PrecomputedDistinct) computation;
                return new PrecomputedContext(scope, Optional.of(precomputedDistinct.field));
            } else if (computation instanceof Precomputed.PrecomputedPercentile) {
                final Precomputed.PrecomputedPercentile precomputedPercentile = (Precomputed.PrecomputedPercentile) computation;
                return new PrecomputedContext(scope, Optional.of(precomputedPercentile.field));
            } else if (computation instanceof Precomputed.PrecomputedRawStats) {
                return new PrecomputedContext(scope, Optional.<String>absent());
            } else {
                throw new IllegalStateException("Failed to handle: [" + computePrecomputed + "]'s computation: [" + computation + "]");
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrecomputedContext that = (PrecomputedContext) o;
            return Objects.equals(scope, that.scope) &&
                    Objects.equals(field, that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(scope, field);
        }
    }

    private static class Grouping {
        private final Set<String> scope;
        private final List<PrecomputationInfo> precomputationInfos;

        private Grouping(Set<String> scope, List<PrecomputationInfo> precomputationInfos) {
            this.scope = scope;
            this.precomputationInfos = precomputationInfos;
        }

        public static Grouping from(PrecomputedContext ctx, List<ExecutionStep.ComputePrecomputed> precomputeds) {
            final List<PrecomputationInfo> infos = new ArrayList<>();
            for (final ExecutionStep.ComputePrecomputed precomputed : precomputeds) {
                infos.add(new PrecomputationInfo(precomputed.computation, precomputed.name));
            }
            return new Grouping(ctx.scope, infos);
        }

        public Set<String> names() {
            final Set<String> names = new HashSet<>();
            for (final PrecomputationInfo info : precomputationInfos) {
                names.add(info.name);
            }
            return names;
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
