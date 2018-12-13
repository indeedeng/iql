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

package com.indeed.iql2.language.actions;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.indeed.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.iql2.language.Validator;
import com.indeed.iql2.language.query.fieldresolution.FieldSet;
import com.indeed.iql2.language.util.ValidationHelper;

import java.util.Set;

public class IntOrAction implements Action {
    public final FieldSet field;
    public final ImmutableSet<Long> terms;

    public final int targetGroup;
    public final int positiveGroup;
    public final int negativeGroup;

    public IntOrAction(FieldSet field, Set<Long> terms, int targetGroup, int positiveGroup, int negativeGroup) {
        this.field = field;
        this.terms = ImmutableSet.copyOf(terms);
        this.targetGroup = targetGroup;
        this.positiveGroup = positiveGroup;
        this.negativeGroup = negativeGroup;
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        for (final String dataset : field.datasets()) {
            validationHelper.validateIntField(dataset, field.datasetFieldName(dataset), validator, this);
        }
    }

    @Override
    public com.indeed.iql2.execution.actions.Action toExecutionAction(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
        return new com.indeed.iql2.execution.actions.IntOrAction(
                field,
                terms,
                targetGroup,
                positiveGroup,
                negativeGroup
        );
    }

    @Override
    public String toString() {
        return "IntOrAction{" +
                "field=" + field +
                ", terms=" + terms +
                ", targetGroup=" + targetGroup +
                ", positiveGroup=" + positiveGroup +
                ", negativeGroup=" + negativeGroup +
                '}';
    }
}
