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
public class SumAcross implements Command {
    public final FieldSet field;
    public final AggregateMetric metric;
    public final Optional<AggregateFilter> filter;

    public SumAcross(final FieldSet field, final AggregateMetric metric, final Optional<AggregateFilter> filter) {
        this.field = field;
        this.metric = metric;
        this.filter = filter;
    }

    @Override
    public void validate(ValidationHelper validationHelper, ErrorCollector errorCollector) {
        ValidationUtil.validateField(field, validationHelper, errorCollector, this);
        metric.validate(field.datasets(), validationHelper, errorCollector);

        if (filter.isPresent()) {
            filter.get().validate(field.datasets(), validationHelper, errorCollector);
        }
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.iql2.execution.commands.SumAcross(
                field,
                metric.toExecutionMetric(namedMetricLookup, groupKeySet),
                filter.map(x -> x.toExecutionFilter(namedMetricLookup, groupKeySet))
        );
    }
}
