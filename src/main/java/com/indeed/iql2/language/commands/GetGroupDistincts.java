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
import com.google.common.collect.Iterables;
import com.indeed.iql2.execution.commands.GetGroupDistinctsWindowed;
import com.indeed.iql2.execution.commands.GetSimpleGroupDistincts;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import lombok.Data;

import java.util.List;
import java.util.Objects;

@Data
public class GetGroupDistincts implements Command {
    public final FieldSet field;
    public final Optional<AggregateFilter> filter;
    public final int windowSize;

    @Override
    public void validate(ValidationHelper validationHelper, ErrorCollector errorCollector) {
        ValidationUtil.validateField(field, validationHelper, errorCollector, this);
        if (filter.isPresent()) {
            filter.get().validate(field.datasets(), validationHelper, errorCollector);
        }
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(
            final Function<String, PerGroupConstant> namedMetricLookup,
            final GroupKeySet groupKeySet,
            final List<String> options) {
        if ((windowSize == 1)
                && !filter.isPresent()
                && (field.datasets().size() == 1)) {
            final String dataset = Iterables.getOnlyElement(field.datasets());
            return new GetSimpleGroupDistincts(dataset, field.datasetFieldName(dataset));
        } else {
            final Optional<com.indeed.iql2.execution.AggregateFilter> executionFilter = filter.transform(x -> x.toExecutionFilter(namedMetricLookup, groupKeySet));
            if (windowSize > 1) {
                return new GetGroupDistinctsWindowed(field, executionFilter, windowSize);
            } else {
                return new com.indeed.iql2.execution.commands.GetGroupDistincts(field, executionFilter);
            }
        }
    }
}
