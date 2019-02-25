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

import com.google.common.base.Preconditions;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@EqualsAndHashCode
@ToString
public class TimePeriodRegroup implements Command {
    private final long periodMillis;
    private final Optional<FieldSet> timeField;
    private final Optional<String> timeFormat;
    private final boolean isRelative;

    public TimePeriodRegroup(final long periodMillis, final Optional<FieldSet> timeField, final Optional<String> timeFormat, final boolean isRelative) {
        this.periodMillis = periodMillis;
        this.timeField = timeField;
        this.timeFormat = timeFormat;
        this.isRelative = isRelative;
    }

    @Override
    public void validate(ValidationHelper validationHelper, ErrorCollector errorCollector) {
        if (timeField.isPresent()) {
            Preconditions.checkState(validationHelper.datasets().equals(timeField.get().datasets()));
            ValidationUtil.validateIntField(timeField.get(), validationHelper, errorCollector, this);
        }
        if (timeFormat.isPresent()) {
            ValidationUtil.validateDateTimeFormat(timeFormat.get(), errorCollector);
        }
        ValidationUtil.validateGroupByTimeRange(validationHelper, TimeUnit.MILLISECONDS.toSeconds(periodMillis), errorCollector);
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.iql2.execution.commands.TimePeriodRegroup(
                periodMillis,
                timeField,
                timeFormat,
                isRelative
        );
    }
}
