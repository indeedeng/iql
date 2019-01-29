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
import com.google.common.base.Preconditions;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SimpleIterate implements Command {
    public final FieldSet field;
    public final FieldIterateOpts opts;
    public final List<AggregateMetric> selecting;
    private final List<Optional<String>> formatStrings;

    public SimpleIterate(FieldSet field, FieldIterateOpts opts, List<AggregateMetric> selecting, List<Optional<String>> formatStrings) {
        this.field = field;
        this.opts = opts;
        this.selecting = selecting;
        this.formatStrings = formatStrings;
    }

    @Override
    public void validate(ValidationHelper validationHelper, ErrorCollector errorCollector) {
        Preconditions.checkState(validationHelper.datasets().equals(field.datasets()));
        ValidationUtil.validateField(field, validationHelper, errorCollector, this);
        if (opts.topK.isPresent()) {
            final TopK topK = opts.topK.get();
            if (topK.metric.isPresent()) {
                topK.metric.get().validate(validationHelper.datasets(), validationHelper, errorCollector);
            }
        }

        if (opts.filter.isPresent()) {
            opts.filter.get().validate(validationHelper.datasets(), validationHelper, errorCollector);
        }

        for (final AggregateMetric metric : selecting) {
            metric.validate(validationHelper.datasets(), validationHelper, errorCollector);
        }

        for (final Optional<String> formatString : formatStrings) {
            if (formatString.isPresent()) {
                ValidationUtil.validateDoubleFormatString(formatString.get(), errorCollector);
            }
        }
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.iql2.execution.commands.SimpleIterate(
                field,
                opts.toExecution(namedMetricLookup, groupKeySet),
                selecting.stream().map(x -> x.toExecutionMetric(namedMetricLookup, groupKeySet)).collect(Collectors.toList()),
                formatStrings
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SimpleIterate that = (SimpleIterate) o;
        return Objects.equals(field, that.field) &&
                Objects.equals(opts, that.opts) &&
                Objects.equals(selecting, that.selecting) &&
                Objects.equals(formatStrings, that.formatStrings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, opts, selecting, formatStrings);
    }

    @Override
    public String toString() {
        return "SimpleIterate{" +
                "field='" + field + '\'' +
                ", opts=" + opts +
                ", selecting=" + selecting +
                ", formatStrings=" + formatStrings +
                '}';
    }
}
