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
import com.indeed.iql2.execution.commands.GetSimpleGroupDistincts;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.Validator;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;

import java.util.List;
import java.util.Objects;

public class GetGroupDistincts implements Command {
    public final FieldSet field;
    public final Optional<AggregateFilter> filter;
    public final int windowSize;

    public GetGroupDistincts(FieldSet field, Optional<AggregateFilter> filter, int windowSize) {
        this.field = field;
        this.filter = filter;
        this.windowSize = windowSize;
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        ValidationUtil.validateField(field, validationHelper, validator, this);
        if (filter.isPresent()) {
            filter.get().validate(field.datasets(), validationHelper, validator);
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
            return new com.indeed.iql2.execution.commands.GetGroupDistincts(
                    field,
                    filter.transform(x -> x.toExecutionFilter(namedMetricLookup, groupKeySet)),
                    windowSize
            );
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final GetGroupDistincts that = (GetGroupDistincts) o;
        return windowSize == that.windowSize &&
                Objects.equals(field, that.field) &&
                Objects.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, filter, windowSize);
    }

    @Override
    public String toString() {
        return "GetGroupDistincts{" +
                "field=" + field +
                ", filter=" + filter +
                ", windowSize=" + windowSize +
                '}';
    }
}
