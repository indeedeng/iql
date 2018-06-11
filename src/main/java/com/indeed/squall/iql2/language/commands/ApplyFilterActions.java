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

package com.indeed.squall.iql2.language.commands;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.indeed.squall.iql2.execution.groupkeys.sets.GroupKeySet;
import com.indeed.squall.iql2.execution.metrics.aggregate.PerGroupConstant;
import com.indeed.squall.iql2.language.Validator;
import com.indeed.squall.iql2.language.actions.Action;
import com.indeed.squall.iql2.language.util.ValidationHelper;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ApplyFilterActions implements Command {
    public final ImmutableList<Action> actions;

    public ApplyFilterActions(List<Action> actions) {
        this.actions = ImmutableList.copyOf(actions);
    }

    @Override
    public void validate(ValidationHelper validationHelper, Validator validator) {
        for (final Action action : actions) {
            action.validate(validationHelper, validator);
        }
    }

    @Override
    public com.indeed.squall.iql2.execution.commands.Command toExecutionCommand(Function<String, PerGroupConstant> namedMetricLookup, GroupKeySet groupKeySet, List<String> options) {
        return new com.indeed.squall.iql2.execution.commands.ApplyFilterActions(
                actions
                .stream()
                .map(x -> x.toExecutionAction(namedMetricLookup, groupKeySet))
                .collect(Collectors.toList())
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ApplyFilterActions that = (ApplyFilterActions) o;
        return Objects.equals(actions, that.actions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actions);
    }

    @Override
    public String toString() {
        return "ApplyFilterActions{" +
                "actions=" + actions +
                '}';
    }
}
