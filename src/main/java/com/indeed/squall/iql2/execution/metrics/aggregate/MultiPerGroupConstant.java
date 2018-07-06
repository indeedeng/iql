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

import com.indeed.squall.iql2.execution.QualifiedPush;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MultiPerGroupConstant implements AggregateMetric {
    public final List<double[]> values;

    public MultiPerGroupConstant(final List<double[]> values) {
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
        throw new UnsupportedOperationException();
    }

    @Override
    public double apply(final String term, final long[] stats, final int group) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double apply(final long term, final long[] stats, final int group) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean needSorted() {
        return false;
    }

    @Override
    public boolean needGroup() {
        return false;
    }

    @Override
    public boolean needStats() {
        return false;
    }
}
