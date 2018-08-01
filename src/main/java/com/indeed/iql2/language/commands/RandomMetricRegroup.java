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

package com.indeed.iql2.language.commands;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.Validator;
import com.indeed.iql2.language.util.ValidationHelper;

import java.util.List;
import java.util.Map;

public class RandomMetricRegroup implements Command {
    public final ImmutableMap<String, ImmutableList<String>> perDatasetMetric;
    private final int k;
    private final String salt;

    public RandomMetricRegroup(final Map<String, List<String>> perDatasetMetric,
                               final int k,
                               final String salt) {
        final ImmutableMap.Builder<String, ImmutableList<String>> copy = ImmutableMap.builder();
        for (final Map.Entry<String, List<String>> entry : perDatasetMetric.entrySet()) {
            copy.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
        }
        this.perDatasetMetric = copy.build();
        this.k = k;
        this.salt = salt;
    }

    @Override
    public void validate(final ValidationHelper validationHelper, final Validator validator) {
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.iql2.execution.commands.RandomMetricRegroup(
                perDatasetMetric,
                k,
                salt
        );
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        final RandomMetricRegroup that = (RandomMetricRegroup) o;
        return Objects.equal(perDatasetMetric, that.perDatasetMetric) && (k == that.k) && Objects.equal(salt, that.salt);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(perDatasetMetric, k, salt);
    }

    @Override
    public String toString() {
        return "RandomMetricRegroup{" +
                "perDatasetMetric=" + perDatasetMetric +
                ", k=" + k +
                ", salt='" + salt + '\'' +
                '}';
    }
}
