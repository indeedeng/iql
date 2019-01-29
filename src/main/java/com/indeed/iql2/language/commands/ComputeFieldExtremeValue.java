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
import com.google.common.base.Optional;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;

import java.util.List;
import java.util.Objects;

public class ComputeFieldExtremeValue implements Command {
    private final FieldSet field;
    private final AggregateMetric metric;
    private final Optional<AggregateFilter> filter;

    public ComputeFieldExtremeValue(
        final FieldSet field,
        final AggregateMetric metric,
        final Optional<AggregateFilter> filter
    ) {
        this.field = field;
        this.metric = metric;
        this.filter = filter;
    }

    @Override
    public void validate(final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
        ValidationUtil.validateField(field, validationHelper, errorCollector, this);
        metric.validate(validationHelper.datasets(),  validationHelper, errorCollector);
        if (filter.isPresent()) {
            filter.get().validate(validationHelper.datasets(), validationHelper, errorCollector);
        }
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(
        final Function<String, PerGroupConstant> namedMetricLookup,
        final GroupKeySet groupKeySet,
        final List<String> options
    ) {
        return new com.indeed.iql2.execution.commands.ComputeFieldExtremeValue(
            field,
            metric.toExecutionMetric(namedMetricLookup, groupKeySet),
            filter.transform(f -> f.toExecutionFilter(namedMetricLookup, groupKeySet))
        );
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ComputeFieldExtremeValue that = (ComputeFieldExtremeValue) o;
        return Objects.equals(field, that.field) &&
            Objects.equals(metric, that.metric) &&
            Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, metric, filter);
    }

    @Override
    public String toString() {
        return "ComputeFieldExtremeValue{" +
                "field='" + field + '\'' +
                ", metric='" + metric + '\'' +
                ", filter='" + filter + '\'' +
                '}';
    }
}