package com.indeed.squall.iql2.execution.metrics.aggregate;

import com.google.common.collect.Sets;
import com.indeed.squall.iql2.execution.AggregateFilter;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.Session;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class IfThenElse implements AggregateMetric {
    private final AggregateFilter condition;
    private final AggregateMetric trueCase;
    private final AggregateMetric falseCase;

    public IfThenElse(AggregateFilter condition, AggregateMetric trueCase, AggregateMetric falseCase) {
        this.condition = condition;
        this.trueCase = trueCase;
        this.falseCase = falseCase;
    }

    @Override
    public Set<QualifiedPush> requires() {
        final Set<QualifiedPush> required = Sets.newHashSet();
        required.addAll(condition.requires());
        required.addAll(trueCase.requires());
        required.addAll(falseCase.requires());
        return required;
    }

    @Override
    public void register(Map<QualifiedPush, Integer> metricIndexes, List<Session.GroupKey> groupKeys) {
        condition.register(metricIndexes, groupKeys);
        trueCase.register(metricIndexes, groupKeys);
        falseCase.register(metricIndexes, groupKeys);
    }

    @Override
    public double[] getGroupStats(long[][] stats, int numGroups) {
        final boolean[] conditionResults = condition.getGroupStats(stats, numGroups);
        final double[] trueResults = trueCase.getGroupStats(stats, numGroups);
        final double[] falseResults = falseCase.getGroupStats(stats, numGroups);

        final double[] result = new double[numGroups + 1];
        for (int i = 0; i <= numGroups; i++) {
            result[i] = conditionResults[i] ? trueResults[i] : falseResults[i];
        }
        return result;
    }

    @Override
    public double apply(String term, long[] stats, int group) {
        final boolean cond = condition.allow(term, stats, group);
        if (cond) {
            return trueCase.apply(term, stats, group);
        } else {
            return falseCase.apply(term, stats, group);
        }
    }

    @Override
    public double apply(long term, long[] stats, int group) {
        final boolean cond = condition.allow(term, stats, group);
        if (cond) {
            return trueCase.apply(term, stats, group);
        } else {
            return falseCase.apply(term, stats, group);
        }
    }
}
