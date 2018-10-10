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

package com.indeed.iql2.execution.metrics.aggregate;

import com.indeed.imhotep.metrics.aggregate.AggregateStatTree;
import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class PerGroupConstant implements AggregateMetric {
    public final double[] values;

    public PerGroupConstant(final double[] values) {
        this.values = values;
    }

    @Override
    public Set<QualifiedPush> requires() {
        return Collections.emptySet();
    }

    @Override
    public void register(final Map<QualifiedPush, Integer> metricIndexes, final GroupKeySet groupKeySet) {
    }

    @Override
    public double[] getGroupStats(final long[][] stats, final int numGroups) {
        return Arrays.copyOf(values, numGroups + 1);
    }

    @Override
    public double apply(final String term, final long[] stats, final int group) {
        return values[group];
    }

    @Override
    public double apply(final long term, final long[] stats, final int group) {
        return values[group];
    }

    @Override
    public AggregateStatTree toImhotep(final Map<QualifiedPush, AggregateStatTree> atomicStats) {
        return AggregateStatTree.perGroupConstant(values);
    }

    @Override
    public boolean needSorted() {
        return false;
    }

    @Override
    public boolean needGroup() {
        return true;
    }

    @Override
    public boolean needStats() {
        return false;
    }
}
