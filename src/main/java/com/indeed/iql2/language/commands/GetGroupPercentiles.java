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
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;
import java.util.function.Function;

@EqualsAndHashCode
@ToString
public class GetGroupPercentiles implements Command {
    public final FieldSet field;
    public final double percentile;

    public GetGroupPercentiles(final FieldSet field, final double percentile) {
        this.field = field;
        this.percentile = percentile;
    }

    @Override
    public void validate(final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
        ValidationUtil.validateIntField(field, validationHelper, errorCollector, this);
        if ((percentile < 0) || (percentile > 100.0)) {
            errorCollector.error("Percentile must be in [0, 100] range, user value is " + percentile);
        }
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(
            final Function<String, PerGroupConstant> namedMetricLookup,
            final GroupKeySet groupKeySet,
            final List<String> options) {
        return new com.indeed.iql2.execution.commands.GetGroupPercentiles(field, percentile);
    }
}
