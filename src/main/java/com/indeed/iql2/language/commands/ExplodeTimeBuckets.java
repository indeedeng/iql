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
import com.indeed.iql2.language.Validator;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;

import java.util.List;
import java.util.Objects;

public class ExplodeTimeBuckets implements Command {
    private final int numBuckets;
    private final Optional<FieldSet> timeField;
    private final Optional<String> timeFormat;

    public ExplodeTimeBuckets(int numBuckets, Optional<FieldSet> timeField, Optional<String> timeFormat) {
        this.numBuckets = numBuckets;
        this.timeField = timeField;
        this.timeFormat = timeFormat;
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        if (timeField.isPresent()) {
            Preconditions.checkState(validationHelper.datasets().equals(timeField.get().datasets()));
            ValidationUtil.validateIntField(timeField.get(), validationHelper, validator, this);
        }
        if (timeFormat.isPresent()) {
            ValidationUtil.validateDateTimeFormat(timeFormat.get(), validator);
        }
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.iql2.execution.commands.ExplodeTimeBuckets(
                numBuckets,
                timeField,
                timeFormat
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExplodeTimeBuckets that = (ExplodeTimeBuckets) o;
        return Objects.equals(numBuckets, that.numBuckets) &&
                Objects.equals(timeField, that.timeField) &&
                Objects.equals(timeFormat, that.timeFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numBuckets, timeField, timeFormat);
    }

    @Override
    public String toString() {
        return "ExplodeTimeBuckets{" +
                "numBuckets=" + numBuckets +
                ", timeField=" + timeField +
                ", timeFormat=" + timeFormat +
                '}';
    }
}
