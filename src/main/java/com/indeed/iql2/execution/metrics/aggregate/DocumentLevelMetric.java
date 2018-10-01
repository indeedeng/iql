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

import com.google.common.collect.Lists;
import com.indeed.imhotep.metrics.aggregate.AggregateStatTree;
import com.indeed.iql2.execution.QualifiedPush;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class DocumentLevelMetric implements AggregateMetric {
    private final String sessionName;
    private final List<String> pushes;
    private int index = -1;

    public DocumentLevelMetric(final String sessionName, final List<String> pushes) {
        this.sessionName = sessionName;
        this.pushes = Lists.newArrayList(pushes);
    }

    @Override
    public Set<QualifiedPush> requires() {
        return Collections.singleton(push());
    }

    @Nonnull
    private QualifiedPush push() {
        return new QualifiedPush(sessionName, pushes);
    }

    @Override
    public void register(final Map<QualifiedPush, Integer> metricIndexes, final GroupKeySet groupKeySet) {
        this.index = metricIndexes.get(push());
    }

    @Override
    public double[] getGroupStats(final long[][] stats, final int numGroups) {
        final double[] result = new double[numGroups + 1];
        for (int i = 0; i < Math.min(result.length, stats[index].length); i++) {
            result[i] = (double) stats[index][i];
        }
        return result;
    }

    @Override
    public double apply(final String term, final long[] stats, final int group) {
        return stats[index];
    }

    @Override
    public double apply(final long term, final long[] stats, final int group) {
        return stats[index];
    }

    @Override
    public AggregateStatTree toImhotep(final Map<QualifiedPush, AggregateStatTree> atomicStats) {
        return Objects.requireNonNull(atomicStats.get(push()));
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
        return true;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return "DocumentLevelMetric{" +
                "sessionName='" + sessionName + '\'' +
                ", pushes=" + pushes +
                ", index=" + index +
                '}';
    }
}
