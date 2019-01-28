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
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.execution.ExecutionStep;
import com.indeed.iql2.language.precomputed.Precomputed;
import com.indeed.util.core.Pair;

import java.util.ArrayList;
import java.util.List;

// Some metrics require precomputation but then still have AggregateMetric.Running instances
// which will have an incorrect depth.
// This rewrite is a hack that fixes that issue with the static depth hack by finding all instances of
// such metrics, and then searching within them for Running metrics and replacing them with a lower depth.
// TODO: Replace this entire thing with dependency graph computations
public class FixFtgsMetricRunning {

    private FixFtgsMetricRunning() {
    }

    public static final Function<AggregateMetric, AggregateMetric> DECREMENT_RUNNING = new Function<AggregateMetric, AggregateMetric>() {
        @Override
        public AggregateMetric apply(AggregateMetric input) {
            if (input instanceof AggregateMetric.Running) {
                final AggregateMetric.Running running = (AggregateMetric.Running) input;
                return new AggregateMetric.Running(running.offset - 1, this.apply(running.metric));
            } else {
                return input.traverse1(this);
            }
        }
    };

    public static List<ExecutionStep> apply(List<ExecutionStep> steps) {
        final List<ExecutionStep> results = new ArrayList<>();
        for (final ExecutionStep step : steps) {
            if (step instanceof ExecutionStep.ComputePrecomputed) {
                final ExecutionStep.ComputePrecomputed computePrecomputed = (ExecutionStep.ComputePrecomputed) step;
                final Precomputed computation = computePrecomputed.computation.transform(x -> x, DECREMENT_RUNNING, x -> x, x -> x, x -> x, x -> x);
                results.add(new ExecutionStep.ComputePrecomputed(computePrecomputed.scope, computation, computePrecomputed.name));
            } else if (step instanceof ExecutionStep.ComputeManyPrecomputed) {
                final ExecutionStep.ComputeManyPrecomputed computeManyPrecomputed = (ExecutionStep.ComputeManyPrecomputed) step;
                final List<Pair<Precomputed, String>> computations = new ArrayList<>();
                for (final Pair<Precomputed, String> computation : computeManyPrecomputed.computations) {
                    computations.add(Pair.of(computation.getFirst().transform(x -> x, DECREMENT_RUNNING, x -> x, x -> x, x -> x, x -> x), computation.getSecond()));
                }
                results.add(new ExecutionStep.ComputeManyPrecomputed(computeManyPrecomputed.scope, computations));
            } else {
                results.add(step);
            }
        }
        return results;
    }
}
