package com.indeed.jql.language.execution.passes;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.indeed.jql.language.AggregateFilter;
import com.indeed.jql.language.AggregateMetric;
import com.indeed.jql.language.DocFilter;
import com.indeed.jql.language.DocMetric;
import com.indeed.jql.language.execution.ExecutionStep;
import com.indeed.jql.language.query.GroupBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class OptimizeLast {
    public static List<ExecutionStep> optimize(List<ExecutionStep> steps) {
        steps = Collections.unmodifiableList(steps);
        if (steps.size() > 1) {
            if (steps.get(steps.size() - 1) instanceof ExecutionStep.GetGroupStats
                    && steps.get(steps.size() - 2) instanceof ExecutionStep.ExplodeAndRegroup) {
                final ExecutionStep.GetGroupStats getGroupStats = (ExecutionStep.GetGroupStats) steps.get(steps.size() - 1);
                final ExecutionStep.ExplodeAndRegroup explodeAndRegroup = (ExecutionStep.ExplodeAndRegroup) steps.get(steps.size() - 2);
                final List<ExecutionStep> newSteps = new ArrayList<>();
                newSteps.addAll(steps);
                newSteps.remove(newSteps.size() - 1);
                newSteps.remove(newSteps.size() - 1);
                newSteps.add(new ExecutionStep.IterateStats(
                        explodeAndRegroup.field,
                        explodeAndRegroup.filter,
                        explodeAndRegroup.limit,
                        explodeAndRegroup.metric,
                        fixForIteration(getGroupStats.stats)
                ));
                return newSteps;
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
