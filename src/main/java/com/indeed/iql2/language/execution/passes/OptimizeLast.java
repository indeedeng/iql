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
import com.google.common.collect.Sets;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.DocFilter;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.execution.ExecutionStep;
import com.indeed.iql2.language.query.GroupBy;
import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;


public class OptimizeLast {
    public static List<ExecutionStep> optimize(List<ExecutionStep> steps, Optional<Integer> queryLimit) {
        steps = Collections.unmodifiableList(steps);
        if (steps.size() >= 1) {
            // IQL-606: translate the last ExplodeAndRegroup(without GetGroupStats following) into IterateStats
            final ExecutionStep last = steps.get(steps.size() - 1);
            if (last instanceof ExecutionStep.ExplodeAndRegroup) {
                return optimizeExplodeAndRegroup(
                        steps,
                        (ExecutionStep.ExplodeAndRegroup) last,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        queryLimit,
                        1);
            }

            if (steps.size() > 1) {
                final ExecutionStep penultimate = steps.get(steps.size() - 2);

                if (last instanceof ExecutionStep.GetGroupStats
                        && penultimate instanceof ExecutionStep.ExplodeAndRegroup) {
                    final ExecutionStep.GetGroupStats getGroupStats = (ExecutionStep.GetGroupStats) last;
                    return optimizeExplodeAndRegroup(
                            steps,
                            (ExecutionStep.ExplodeAndRegroup) penultimate,
                            getGroupStats.stats,
                            getGroupStats.formatStrings,
                            queryLimit,
                            2);
                } else if (last instanceof ExecutionStep.GetGroupStats && penultimate instanceof ExecutionStep.ExplodeFieldIn) {
                    final ExecutionStep.ExplodeFieldIn explodeFieldIn = (ExecutionStep.ExplodeFieldIn) penultimate;
                    final ExecutionStep.GetGroupStats getGroupStats = (ExecutionStep.GetGroupStats) last;
                    if (!explodeFieldIn.withDefault) {
                        final Optional<Set<Long>> intTermSubset;
                        if (!explodeFieldIn.intTerms.isEmpty()) {
                            intTermSubset = Optional.<Set<Long>>of(new LongAVLTreeSet(explodeFieldIn.intTerms));
                        } else {
                            intTermSubset = Optional.absent();
                        }

                        final Optional<Set<String>> stringTermSubset;
                        if (!explodeFieldIn.stringTerms.isEmpty()) {
                            stringTermSubset = Optional.<Set<String>>of(Sets.newTreeSet(explodeFieldIn.stringTerms));
                        } else {
                            stringTermSubset = Optional.absent();
                        }

                        final List<ExecutionStep> newSteps = new ArrayList<>();
                        newSteps.addAll(steps.subList(0, steps.size() - 2));
                        newSteps.add(new ExecutionStep.IterateStats(
                                explodeFieldIn.field,
                                Optional.<AggregateFilter>absent(),
                                Optional.<Long>absent(),
                                queryLimit,
                                Optional.<AggregateMetric>absent(),
                                stringTermSubset,
                                intTermSubset,
                                fixForIteration(getGroupStats.stats),
                                getGroupStats.formatStrings
                        ));
                        return newSteps;
                    }
                }
            }
        }
        return steps;
    }

    private static List<AggregateMetric> fixForIteration(List<AggregateMetric> stats) {
        final List<AggregateMetric> result = new ArrayList<>();
        for (final AggregateMetric stat : stats) {
            result.add(stat.transform(
                    PROCESS_METRIC,
                    Functions.<DocMetric>identity(),
                    Functions.<AggregateFilter>identity(),
                    Functions.<DocFilter>identity(),
                    Functions.<GroupBy>identity()
            ));
        }
        return result;
    }

    private static final List<ExecutionStep> optimizeExplodeAndRegroup(
            final List<ExecutionStep> steps,
            final ExecutionStep.ExplodeAndRegroup explodeAndRegroup,
            final List<AggregateMetric> metricsInGetGroupStats,
            final List<Optional<String>> formatStringsInGetGroupStats,
            final Optional<Integer> queryLimit,
            final int stepsToRemove) {
        final boolean selectIsOrdered = Iterables.any(metricsInGetGroupStats, new Predicate<AggregateMetric>() {
            @Override
            public boolean apply(AggregateMetric input) {
                return input.isOrdered();
            }
        });
        // TODO: Make query execution sort on .metric whether or not there's a limit, make .metric optional. Then change this to care if .metric.isPresent() also.
        // TODO: Figure out wtf the above TODO means.
        final boolean hasDefault = explodeAndRegroup.withDefault;
        if (!selectIsOrdered && !hasDefault) { // If there's a filter and something that depends on order, we can't merge them.
            List<ExecutionStep> newSteps = new ArrayList<>(steps);
            int i = 0;
            while (i++ < stepsToRemove) {
                newSteps.remove(newSteps.size() -1);
            }
            newSteps.add(new ExecutionStep.IterateStats(
                    explodeAndRegroup.field,
                    explodeAndRegroup.filter,
                    explodeAndRegroup.limit,
                    queryLimit,
                    explodeAndRegroup.metric,
                    Optional.<Set<String>>absent(),
                    Optional.<Set<Long>>absent(),
                    fixForIteration(metricsInGetGroupStats),
                    formatStringsInGetGroupStats
            ));
            return newSteps;
        }
        return steps;
    }

    private static final Function<AggregateMetric,AggregateMetric> PROCESS_METRIC = new Function<AggregateMetric, AggregateMetric>() {
        @Override
        public AggregateMetric apply(AggregateMetric input) {
            if (input instanceof AggregateMetric.Running) {
                final AggregateMetric.Running running = (AggregateMetric.Running) input;
                return new AggregateMetric.Running(running.offset - 1, running.metric);
            } else if (input instanceof AggregateMetric.Lag) {
                final AggregateMetric.Lag lag = (AggregateMetric.Lag) input;
                return new AggregateMetric.IterateLag(lag.lag, lag.metric);
            } else {
                return input;
            }
        }
    };
}
