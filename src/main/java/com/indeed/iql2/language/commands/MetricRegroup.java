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

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MetricRegroup implements Command {
    public final Map<String, DocMetric> perDatasetMetric;
    public final long min;
    public final long max;
    public final long interval;
    public final boolean excludeGutters;
    public final boolean withDefault;
    public final boolean fromPredicate;

    public MetricRegroup(Map<String, DocMetric> perDatasetMetric, long min, long max, long interval, boolean excludeGutters, boolean withDefault, boolean fromPredicate) {
        this.perDatasetMetric = perDatasetMetric;
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
        if ((max-min)%interval != 0) {
            final long bucketRange = max - min;
            validator.error(MessageFormat.format("Bucket range should be a multiple of the interval. To correct, decrease the upper bound to {0} or increase to {1}", max - bucketRange % interval, max + interval - bucketRange % interval));
        }

        for (final Map.Entry<String, DocMetric> docMetricEntry : perDatasetMetric.entrySet()) {
            docMetricEntry.getValue().validate(docMetricEntry.getKey(), validationHelper, validator);
        }
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        final Map<String, List<String>> perDatasetCommands = perDatasetMetric.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> ImmutableList.copyOf(entry.getValue().getPushes(entry.getKey()))
                ));
        return new com.indeed.iql2.execution.commands.MetricRegroup(
                perDatasetCommands,
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
