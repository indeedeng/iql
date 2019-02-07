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
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ValidationHelper;
import com.indeed.iql2.language.util.ValidationUtil;
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

@EqualsAndHashCode
@ToString
public class ExplodePerDocPercentile implements Command {
    public final FieldSet field;
    public final int numBuckets;

    public ExplodePerDocPercentile(final FieldSet field, final int numBuckets) {
        this.field = field;
        this.numBuckets = numBuckets;
    }

    @Override
    public void validate(final ValidationHelper validationHelper, final ErrorCollector errorCollector) {
        Preconditions.checkState(validationHelper.datasets().equals(field.datasets()));
        ValidationUtil.validateIntField(field, validationHelper, errorCollector, this);
        if (numBuckets <= 0) {
            errorCollector.error("Bucket count in QUANTILES must be positive, current value = " + numBuckets);
        }
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.iql2.execution.commands.ExplodePerDocPercentile(
                field,
                numBuckets
        );
    }
}
