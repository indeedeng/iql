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

import com.google.common.collect.Sets;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.execution.ExecutionStep;
import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;


public class OptimizeLast {
    private OptimizeLast() {
    }

    public static List<ExecutionStep> optimize(List<ExecutionStep> steps, Optional<Integer> queryLimit) {
        steps = Collections.unmodifiableList(steps);
        if (steps.size() > 1) {
            final ExecutionStep last = steps.get(steps.size() - 1);
            final ExecutionStep penultimate = steps.get(steps.size() - 2);
            if (last instanceof ExecutionStep.GetGroupStats
                    && penultimate instanceof ExecutionStep.ExplodeAndRegroup) {
                final ExecutionStep.GetGroupStats getGroupStats = (ExecutionStep.GetGroupStats) last;
                final boolean selectIsOrdered = getGroupStats.stats.stream().anyMatch(AggregateMetric::isOrdered);
                final ExecutionStep.ExplodeAndRegroup explodeAndRegroup = (ExecutionStep.ExplodeAndRegroup) penultimate;
                // TODO: Make query execution sort on .metric whether or not there's a limit, make .metric optional. Then change this to care if .metric.isPresent() also.
                // TODO: Figure out wtf the above TODO means.
                final boolean hasDefault = explodeAndRegroup.withDefault;
                if (!selectIsOrdered && !hasDefault) { // If there's a filter and something that depends on order, we can't merge them.
                    final List<ExecutionStep> newSteps = new ArrayList<>();
                    newSteps.addAll(steps);
                    newSteps.remove(newSteps.size() - 1);
                    newSteps.remove(newSteps.size() - 1);
                    newSteps.add(new ExecutionStep.IterateStats(
                            explodeAndRegroup.field,
                            explodeAndRegroup.filter,
                            queryLimit,
                            explodeAndRegroup.topK,
                            Optional.empty(),
                            Optional.empty(),
                            fixForIteration(getGroupStats.stats),
                            getGroupStats.formatStrings
                            ));
                    return newSteps;
                }
            } else if (last instanceof ExecutionStep.GetGroupStats && penultimate instanceof ExecutionStep.ExplodeFieldIn) {
                final ExecutionStep.ExplodeFieldIn explodeFieldIn = (ExecutionStep.ExplodeFieldIn) penultimate;
                final ExecutionStep.GetGroupStats getGroupStats = (ExecutionStep.GetGroupStats) last;
                if (!explodeFieldIn.withDefault) {
                    final Optional<Set<Long>> intTermSubset;
                    if (!explodeFieldIn.intTerms.isEmpty()) {
                        intTermSubset = Optional.of(new LongAVLTreeSet(explodeFieldIn.intTerms));
                    } else {
                        intTermSubset = Optional.empty();
                    }

                    final Optional<Set<String>> stringTermSubset;
                    if (!explodeFieldIn.stringTerms.isEmpty()) {
                        stringTermSubset = Optional.of(Sets.newTreeSet(explodeFieldIn.stringTerms));
                    } else {
                        stringTermSubset = Optional.empty();
                    }

                    final List<ExecutionStep> newSteps = new ArrayList<>();
                    newSteps.addAll(steps.subList(0, steps.size() - 2));
                    newSteps.add(new ExecutionStep.IterateStats(
                            explodeFieldIn.field,
                            Optional.empty(),
                            queryLimit,
                            Optional.empty(),
                            stringTermSubset,
                            intTermSubset,
                            fixForIteration(getGroupStats.stats),
                            getGroupStats.formatStrings
                    ));
                    return newSteps;
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
                    Function.identity(),
                    Function.identity(),
                    Function.identity(),
                    Function.identity()
            ));
        }
        return result;
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
