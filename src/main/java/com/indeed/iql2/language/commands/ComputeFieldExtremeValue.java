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

import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.FieldExtremeType;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

@EqualsAndHashCode
@ToString
public class ComputeFieldExtremeValue implements Command {
    private final FieldSet field;
    private final AggregateMetric metric;
    private final Optional<AggregateFilter> filter;
    private final FieldExtremeType fieldExtremeType;

    public ComputeFieldExtremeValue(
        final FieldSet field,
        final AggregateMetric metric,
        final Optional<AggregateFilter> filter,
        final FieldExtremeType fieldExtremeType
    ) {
        this.field = field;
        this.metric = metric;
        this.filter = filter;
        this.fieldExtremeType = fieldExtremeType;
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
            filter.map(f -> f.toExecutionFilter(namedMetricLookup, groupKeySet)),
            fieldExtremeType
        );
    }
}