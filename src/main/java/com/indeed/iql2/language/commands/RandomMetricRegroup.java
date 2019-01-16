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
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.DocMetric;
import com.indeed.iql2.language.Validator;
import com.indeed.iql2.language.util.ValidationHelper;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RandomMetricRegroup implements Command {
    private final Map<String, DocMetric> perDatasetMetric;
    private final int k;
    private final String salt;

    public RandomMetricRegroup(final Map<String, DocMetric> perDatasetMetric,
                               final int k,
                               final String salt) {
        this.perDatasetMetric = perDatasetMetric;
        this.k = k;
        this.salt = salt;
    }

    @Override
    public void validate(final ValidationHelper validationHelper, final Validator validator) {
        if (k <= 1) {
            validator.error("Bucket count in RANDOM() must be greater than 1, buckets = " + k);
        }
        for (final Map.Entry<String, DocMetric> entry : perDatasetMetric.entrySet()) {
            entry.getValue().validate(entry.getKey(), validationHelper, validator);
        }
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        final Map<String, List<String>> perDatasetCommands = perDatasetMetric.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> ImmutableList.copyOf(entry.getValue().getPushes(entry.getKey()))
                ));
        return new com.indeed.iql2.execution.commands.RandomMetricRegroup(
                perDatasetCommands,
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
