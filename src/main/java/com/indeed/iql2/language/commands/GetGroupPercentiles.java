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
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class GetGroupPercentiles implements Command {
    public final FieldSet field;
    public final double[] percentiles;

    public GetGroupPercentiles(FieldSet field, double[] percentiles) {
        this.field = field;
        this.percentiles = percentiles;
    }

    @Override
    public void validate(ValidationHelper validationHelper, ErrorCollector errorCollector) {
        ValidationUtil.validateIntField(field, validationHelper, errorCollector, this);
        for (final double percentile : percentiles) {
            if ((percentile < 0) || (percentile > 100.0)) {
                errorCollector.error("Percentile must be in [0, 100] range, user value is " + percentile);
            }
        }
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.iql2.execution.commands.GetGroupPercentiles(field, percentiles);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final GetGroupPercentiles that = (GetGroupPercentiles) o;
        return Objects.equals(field, that.field) &&
                Arrays.equals(percentiles, that.percentiles);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(field);
        result = 31 * result + Arrays.hashCode(percentiles);
        return result;
    }

    @Override
    public String toString() {
        return "GetGroupPercentiles{" +
                "field='" + field + '\'' +
                ", percentiles=" + Arrays.toString(percentiles) +
                '}';
    }
}
