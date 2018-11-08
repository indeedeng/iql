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
import com.indeed.iql2.language.Positioned;
import com.indeed.iql2.language.Validator;
import com.indeed.iql2.language.util.ValidationHelper;

import java.util.List;

public class ExplodeMonthOfYear implements Command {
    private final Optional<String> timeField;
    private final Optional<String> timeFormat;

    public ExplodeMonthOfYear(final Optional<String> timeField, final Optional<String> timeFormat) {
        this.timeField = timeField;
        this.timeFormat = timeFormat;
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {

    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.iql2.execution.commands.ExplodeMonthOfYear(timeField, timeFormat);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ExplodeMonthOfYear that = (ExplodeMonthOfYear) o;
        return Objects.equal(timeField, that.timeField) &&
                Objects.equal(timeFormat, that.timeFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(timeField, timeFormat);
    }

    @Override
    public String toString() {
        return "ExplodeMonthOfYear{" +
                "timeField=" + timeField +
                ", timeFormat=" + timeFormat +
                '}';
    }
}
