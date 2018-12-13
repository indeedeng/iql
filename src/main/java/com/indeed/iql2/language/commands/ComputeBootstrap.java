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
import com.google.common.base.Optional;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.AggregateFilter;
import com.indeed.iql2.language.AggregateMetric;
import com.indeed.iql2.language.Validator;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;

import java.util.List;

public class ComputeBootstrap implements Command {
    private final FieldSet field;
    private final Optional<AggregateFilter> filter;
    private final String seed;
    private final AggregateMetric metric;
    private final int numBootstraps;
    private final List<String> varargs;

    public ComputeBootstrap(FieldSet field, Optional<AggregateFilter> filter, String seed, AggregateMetric metric, int numBootstraps, List<String> varargs) {
        this.field = field;
        this.filter = filter;
        this.seed = seed;
        this.metric = metric;
        this.numBootstraps = numBootstraps;
        this.varargs = varargs;
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        ValidationUtil.validateField(field, validationHelper, validator, this);
        metric.validate(field.datasets(), validationHelper, validator);
        if (filter.isPresent()) {
            filter.get().validate(field.datasets(), validationHelper, validator);
        }
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.iql2.execution.commands.ComputeBootstrap(
                field,
                filter.transform(x -> x.toExecutionFilter(namedMetricLookup, groupKeySet)),
                seed,
                metric.toExecutionMetric(namedMetricLookup, groupKeySet),
                numBootstraps,
                varargs
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ComputeBootstrap that = (ComputeBootstrap) o;
        return numBootstraps == that.numBootstraps &&
                Objects.equal(field, that.field) &&
                Objects.equal(filter, that.filter) &&
                Objects.equal(seed, that.seed) &&
                Objects.equal(metric, that.metric) &&
                Objects.equal(varargs, that.varargs);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(field, filter, seed, metric, numBootstraps, varargs);
    }

    @Override
    public String toString() {
        return "ComputeBootstrap{" +
                "field='" + field + '\'' +
                ", filter=" + filter +
                ", seed='" + seed + '\'' +
                ", metric=" + metric +
                ", numBootstraps=" + numBootstraps +
                ", varargs=" + varargs +
                '}';
    }
}
