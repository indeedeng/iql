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
import com.indeed.iql2.language.util.ValidationHelper;

import java.util.Set;

public class UnconditionalAction implements Action {
    public final ImmutableSet<String> scope;
    public final int targetGroup;
    public final int newGroup;

    public UnconditionalAction(Set<String> scope, int targetGroup, int newGroup) {
        this.scope = ImmutableSet.copyOf(scope);
        this.targetGroup = targetGroup;
        this.newGroup = newGroup;
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {

    }

    @Override
    public com.indeed.iql2.execution.actions.Action toExecutionAction(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet) {
        return new com.indeed.iql2.execution.actions.UnconditionalAction(
                scope, targetGroup, newGroup
        );
    }

    @Override
    public String toString() {
        return "UnconditionalAction{" +
                "scope=" + scope +
                ", targetGroup=" + targetGroup +
                ", newGroup=" + newGroup +
                '}';
    }
}
