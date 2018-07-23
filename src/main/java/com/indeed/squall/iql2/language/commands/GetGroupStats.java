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
import com.google.common.base.Optional;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.squall.iql2.language.AggregateMetric;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.util.ValidationHelper;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class GetGroupStats implements Command {
    public final List<AggregateMetric> metrics;
    public final List<Optional<String>> formatStrings;
    public final boolean returnGroupKeys;

    public GetGroupStats(List<AggregateMetric> metrics, List<Optional<String>> formatStrings, boolean returnGroupKeys) {
        this.metrics = metrics;
        this.formatStrings = formatStrings;
        this.returnGroupKeys = returnGroupKeys;
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        for (final AggregateMetric metric : metrics) {
            metric.validate(validationHelper.datasets(), validationHelper, validator);
        }
        for (final Optional<String> formatString : formatStrings) {
            if (formatString.isPresent() && !isCorrectFormatString(formatString.get())) {
                validator.error("Incorrect format string: <" + formatString.get() + ">");
            }
        }
    }

    @Override
    public com.indeed.squall.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.squall.iql2.execution.commands.GetGroupStats(
                metrics.stream().map(x -> x.toExecutionMetric(namedMetricLookup, groupKeySet)).collect(Collectors.toList()),
                formatStrings,
                returnGroupKeys
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetGroupStats that = (GetGroupStats) o;
        return returnGroupKeys == that.returnGroupKeys &&
                Objects.equals(metrics, that.metrics) &&
                Objects.equals(formatStrings, that.formatStrings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metrics, formatStrings, returnGroupKeys);
    }

    @Override
    public String toString() {
        return "GetGroupStats{" +
                "metrics=" + metrics +
                ", formatStrings=" + formatStrings +
                ", returnGroupKeys=" + returnGroupKeys +
                '}';
    }

    private static boolean isCorrectFormatString(final String str) {
        // Don't know how to check format string.
        // Format string will be used to output doubles, so try to output any double and check for exceptions.
        // Not sure that we can catch all format errors with this approach, but believe that almost all will be caught.
        try {
            final String ignored = String.format(str, 0.0d);
            return true;
        } catch (final Throwable t) {
            return false;
        }
    }
}
