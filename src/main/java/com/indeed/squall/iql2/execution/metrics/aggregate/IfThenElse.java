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

package com.indeed.squall.iql2.execution.metrics.aggregate;

import com.google.common.collect.Sets;
import com.indeed.squall.iql2.execution.AggregateFilter;
import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;

import java.util.Map;
import java.util.Set;

public class IfThenElse implements AggregateMetric {
    private final AggregateFilter condition;
    private final AggregateMetric trueCase;
    private final AggregateMetric falseCase;

    public IfThenElse(final AggregateFilter condition,
                      final AggregateMetric trueCase,
                      final AggregateMetric falseCase) {
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
    public void register(final Map<QualifiedPush, Integer> metricIndexes,
                         final GroupKeySet groupKeySet) {
        condition.register(metricIndexes, groupKeySet);
        trueCase.register(metricIndexes, groupKeySet);
        falseCase.register(metricIndexes, groupKeySet);
    }

    @Override
    public double[] getGroupStats(final long[][] stats, final int numGroups) {
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
    public double apply(final String term, final long[] stats, final int group) {
        final boolean cond = condition.allow(term, stats, group);
        if (cond) {
            return trueCase.apply(term, stats, group);
        } else {
            return falseCase.apply(term, stats, group);
        }
    }

    @Override
    public double apply(final long term, final long[] stats, final int group) {
        final boolean cond = condition.allow(term, stats, group);
        if (cond) {
            return trueCase.apply(term, stats, group);
        } else {
            return falseCase.apply(term, stats, group);
        }
    }

    @Override
    public boolean needGroup() {
        return condition.needGroup() || trueCase.needGroup() || falseCase.needGroup();
    }

    @Override
    public boolean needStats() {
        return condition.needStats() || trueCase.needStats() || falseCase.needStats();
    }
}
