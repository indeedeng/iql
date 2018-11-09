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
import com.google.common.base.Preconditions;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.Validator;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;

import java.util.List;
import java.util.Objects;

public class ExplodePerDocPercentile implements Command {
    public final FieldSet field;
    public final int numBuckets;

    public ExplodePerDocPercentile(FieldSet field, int numBuckets) {
        this.field = field;
        this.numBuckets = numBuckets;
    }

    @Override
    public void validate(final ValidationHelper validationHelper, final Validator validator) {
        Preconditions.checkState(validationHelper.datasets().equals(field.datasets()));
        ValidationUtil.validateIntField(field, validationHelper, validator, this);
        if (numBuckets <= 0) {
            validator.error("Bucket count in QUANTILES must be positive, current value = " + numBuckets);
        }
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.iql2.execution.commands.ExplodePerDocPercentile(
                field,
                numBuckets
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExplodePerDocPercentile that = (ExplodePerDocPercentile) o;
        return Objects.equals(numBuckets, that.numBuckets) &&
                Objects.equals(field, that.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, numBuckets);
    }

    @Override
    public String toString() {
        return "ExplodePerDocPercentile{" +
                "field='" + field + '\'' +
                ", numBuckets=" + numBuckets +
                '}';
    }
}
