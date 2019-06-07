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
import com.indeed.iql2.server.web.servlets.query.ErrorCollector;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;
import java.util.function.Function;

@EqualsAndHashCode
@ToString
public class ExplodeRandom implements Command {
    private final FieldSet field;
    private final int k;
    private final String salt;

    public ExplodeRandom(final FieldSet field, final int k, final String salt) {
        this.field = field;
        this.k = k;
        this.salt = salt;
    }

    @Override
    public void validate(ValidationHelper validationHelper, ErrorCollector errorCollector) {
        Preconditions.checkState(validationHelper.datasets().equals(field.datasets()));
        for (final String dataset : validationHelper.datasets()) {
            validationHelper.containsField(dataset, field.datasetFieldName(dataset));
        }
        if (k <= 1) {
            errorCollector.error("Bucket count in RANDOM() must be greater than 1, buckets = " + k);
        }
    }

    @Override
    public com.indeed.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.iql2.execution.commands.ExplodeRandom(
                field,
                k,
                salt
        );
    }
}
