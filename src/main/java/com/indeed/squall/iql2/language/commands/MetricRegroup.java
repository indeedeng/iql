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

package com.indeed.squall.iql2.language.commands;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;

import java.util.List;
import java.util.Map;

public class MetricRegroup implements Command {
    public final ImmutableMap<String, ImmutableList<String>> perDatasetMetric;
    public final long min;
    public final long max;
    public final long interval;
    public final boolean excludeGutters;
    public final boolean withDefault;
    public final boolean fromPredicate;

    public MetricRegroup(Map<String, List<String>> perDatasetMetric, long min, long max, long interval, boolean excludeGutters, boolean withDefault, boolean fromPredicate) {
        final ImmutableMap.Builder<String, ImmutableList<String>> copy = ImmutableMap.builder();
        for (final Map.Entry<String, List<String>> entry : perDatasetMetric.entrySet()) {
            copy.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
        }
        this.perDatasetMetric = copy.build();
        this.min = min;
        this.max = max;
        this.interval = interval;
        this.excludeGutters = excludeGutters;
        this.withDefault = withDefault;
        this.fromPredicate = fromPredicate;
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        if (interval <= 0) {
            validator.error("Bucket size must be positive. size = " + interval);
        }
        if (min >= max) {
            validator.error("Inverval minimum must be lower than interval maximum. Min = " + min + ", Max = " + max);
        }

        // TODO: Validate more List<String>s.... somehow.
    }

    @Override
    public com.indeed.squall.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.squall.iql2.execution.commands.MetricRegroup(
                perDatasetMetric,
                min,
                max,
                interval,
                excludeGutters,
                withDefault,
                fromPredicate
        );
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MetricRegroup)) {
            return false;
        }
        final MetricRegroup that = (MetricRegroup) o;
        return min == that.min &&
                max == that.max &&
                interval == that.interval &&
                excludeGutters == that.excludeGutters &&
                withDefault == that.withDefault &&
                fromPredicate == that.fromPredicate &&
                Objects.equal(perDatasetMetric, that.perDatasetMetric);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(perDatasetMetric, min, max, interval, excludeGutters, withDefault, fromPredicate);
    }

    @Override
    public String toString() {
        return "MetricRegroup{" +
                "perDatasetMetric=" + perDatasetMetric +
                ", min=" + min +
                ", max=" + max +
                ", interval=" + interval +
                ", excludeGutters=" + excludeGutters +
                ", withDefault=" + withDefault +
                ", fromPredicate=" + fromPredicate +
                '}';
    }
}
